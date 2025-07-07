""" Enhanced Cloud Run Service with Shopify Integration """
import os
import logging
import tempfile
import subprocess
import json
import re
from typing import Optional, Dict, List
from dataclasses import dataclass
from flask import Flask, request, jsonify
from google.cloud import storage, secretmanager, videointelligence
import requests

# --- 設定とロギング ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

# 環境変数
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT', 'douga-auto-system')
BUCKET_ORIGINALS = f"{PROJECT_ID}-originals"
BUCKET_PROCESSED = f"{PROJECT_ID}-processed"

# クライアント初期化
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()
video_client = videointelligence.VideoIntelligenceServiceClient()

def get_shopify_access_token():
    """Secret ManagerからShopify APIトークンを取得し、確実に文字列として返す"""
    try:
        secret_name = f"projects/{PROJECT_ID}/secrets/shopify-admin-api-token/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        
        # バイトデータを取得
        raw_data = response.payload.data
        logging.info(f"Raw data type: {type(raw_data)}, length: {len(raw_data)}")
        
        # UTF-8でデコード
        token = raw_data.decode("UTF-8")
        logging.info(f"After decode - type: {type(token)}, length: {len(token)}")
        
        # BOMを除去（UTF-8 BOM: \ufeff）
        if token.startswith('\ufeff'):
            logging.info("Removing UTF-8 BOM")
            token = token[1:]
        
        # 前後の空白、改行、制御文字を除去
        original_length = len(token)
        token = token.strip().replace('\n', '').replace('\r', '').replace('\t', '')
        
        # 制御文字を除去
        token = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', token)
        
        if len(token) != original_length:
            logging.info(f"Token cleaned: length changed from {original_length} to {len(token)}")
        
        # 最終的な型確認
        if not isinstance(token, str):
            raise TypeError(f"Token is not a string: {type(token)}")
        
        # トークンの形式確認（Shopify Admin APIトークンは通常64文字）
        if len(token) < 32:
            logging.warning(f"Token seems too short: {len(token)} characters")
        
        logging.info(f"Successfully retrieved and cleaned token of length {len(token)}")
        logging.info(f"Token type: {type(token)}")
        logging.info(f"Token preview: {token[:8]}...{token[-8:] if len(token) > 16 else token}")
        
        return token
    except Exception as e:
        logging.error(f"Failed to get Shopify access token: {e}", exc_info=True)
        raise

# --- データクラスとShopify APIクライアント ---
@dataclass
class VideoProduct:
    title: str; description: str; price: float; preview_video_url: str; main_video_url: str; ai_tags: List[str]; original_filename: str

class ShopifyAPIClient:
    def __init__(self, shop_domain: str, access_token: str):
        self.shop_domain = shop_domain.replace('https://','').replace('http://','')
        if not self.shop_domain.endswith('.myshopify.com'): self.shop_domain += '.myshopify.com'
        
        # アクセストークンの型を確認
        if not isinstance(access_token, str):
            raise TypeError(f"Access token must be a string, got {type(access_token)}")
        
        self.access_token = access_token
        self.base_url = f"https://{self.shop_domain}/admin/api/2024-04"
        self.headers = {'X-Shopify-Access-Token': self.access_token, 'Content-Type': 'application/json'}
        
        # ヘッダーの型を確認（デバッグ用）
        for key, value in self.headers.items():
            if not isinstance(value, str):
                raise TypeError(f"Header {key} must be a string, got {type(value)}: {value}")
        
        logging.info(f"ShopifyAPIClient initialized for domain: {self.shop_domain}")
        logging.info(f"Access token length: {len(self.access_token)}")
    
    def create_product(self, product_data: dict) -> Optional[Dict]:
        logging.info(f"Attempting to create product: {product_data.get('product', {}).get('title', 'N/A')}")
        
        # デバッグ用：ヘッダーの型と値を確認
        for key, value in self.headers.items():
            logging.info(f"Header {key}: type={type(value)}, length={len(value) if isinstance(value, str) else 'N/A'}")
            if isinstance(value, bytes):
                logging.error(f"CRITICAL: Found bytes in header {key}: {value}")
                raise TypeError(f"Header {key} contains bytes instead of string")
        
        try:
            logging.info(f"Making POST request to: {self.base_url}/products.json")
            response = requests.post(f"{self.base_url}/products.json", headers=self.headers, json=product_data, timeout=30)
            
            logging.info(f"Response status code: {response.status_code}")
            logging.info(f"Response headers: {dict(response.headers)}")
            
            if response.status_code == 201:
                product = response.json()['product']
                logging.info(f"SUCCESS: Product creation successful: {product['title']} (ID: {product['id']})")
                return product
            else:
                logging.error(f"FAILED: Shopify API returned status {response.status_code}")
                logging.error(f"Response body: {response.text}")
                return None
        except Exception as e:
            logging.error(f"EXCEPTION during Shopify API call: {e}", exc_info=True)
            return None

def download_video_from_gcs(bucket_name: str, blob_name: str) -> str:
    """GCSから動画をダウンロードし、一時ファイルのパスを返す"""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
    blob.download_to_filename(temp_file.name)
    logging.info(f"Downloaded {blob_name} to {temp_file.name}")
    return temp_file.name

def add_watermark(input_path: str, output_path: str) -> bool:
    """ffmpegで透かしを追加"""
    try:
        cmd = ['ffmpeg', '-i', input_path, '-i', 'watermark.png', '-filter_complex', 
               '[1:v]scale=100:100[wm];[0:v][wm]overlay=W-w-10:H-h-10', '-c:a', 'copy', output_path, '-y']
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logging.info(f"Watermark added successfully: {output_path}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"FFmpeg error: {e.stderr}")
        return False

def analyze_video_with_ai(gcs_uri: str) -> List[str]:
    """Video Intelligence APIで動画を分析"""
    try:
        features = [videointelligence.Feature.LABEL_DETECTION]
        operation = video_client.annotate_video(request={"features": features, "input_uri": gcs_uri})
        result = operation.result(timeout=300)
        
        tags = []
        for annotation in result.annotation_results[0].segment_label_annotations:
            if annotation.entity.description not in tags:
                tags.append(annotation.entity.description)
        
        logging.info(f"AI analysis completed. Tags: {tags[:10]}")
        return tags[:10]
    except Exception as e:
        logging.error(f"AI analysis failed: {e}")
        return ["動画", "コンテンツ"]

def upload_to_gcs(local_path: str, bucket_name: str, blob_name: str) -> str:
    """ファイルをGCSにアップロードし、公開URLを返す"""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    blob.make_public()
    logging.info(f"Uploaded {blob_name} to {bucket_name}")
    return blob.public_url

@app.route('/process-video', methods=['POST'])
def process_video():
    """メイン処理エンドポイント"""
    try:
        data = request.get_json()
        bucket_name = data['bucket']
        file_name = data['name']
        
        logging.info(f"Processing video: {file_name} from bucket: {bucket_name}")
        
        # 1. 動画をダウンロード
        input_path = download_video_from_gcs(bucket_name, file_name)
        
        # 2. 透かしを追加
        output_path = tempfile.NamedTemporaryFile(delete=False, suffix='_watermarked.mp4').name
        if not add_watermark(input_path, output_path):
            return jsonify({"error": "Watermark addition failed"}), 500
        
        # 3. 処理済み動画をアップロード
        processed_blob_name = f"processed_{file_name}"
        processed_url = upload_to_gcs(output_path, BUCKET_PROCESSED, processed_blob_name)
        
        # 4. AI分析
        gcs_uri = f"gs://{bucket_name}/{file_name}"
        ai_tags = analyze_video_with_ai(gcs_uri)
        
        # 5. Shopifyに商品を作成
        try:
            shopify_access_token = get_shopify_access_token()
            logging.info("Successfully retrieved Shopify access token")
            
            client = ShopifyAPIClient("tategatafree.myshopify.com", shopify_access_token)
            
            product_payload = {
                "product": {
                    "title": f"縦型動画: {file_name.replace('.mp4', '')}",
                    "body_html": f"<p>AI分析タグ: {', '.join(ai_tags)}</p><p>オリジナルファイル: {file_name}</p>",
                    "vendor": "縦型動画フリー",
                    "product_type": "デジタルコンテンツ",
                    "tags": ", ".join(ai_tags),
                    "variants": [{
                        "price": "1000.00",
                        "inventory_management": None,
                        "inventory_quantity": 999
                    }],
                    "images": [],
                    "metafields": [
                        {"namespace": "video", "key": "processed_url", "value": processed_url, "type": "url"},
                        {"namespace": "video", "key": "original_filename", "value": file_name, "type": "single_line_text_field"}
                    ]
                }
            }
            
            result = client.create_product(product_payload)
            if result:
                logging.info(f"SUCCESS: Shopify product created with ID: {result['id']}")
                return jsonify({
                    "status": "success",
                    "processed_url": processed_url,
                    "ai_tags": ai_tags,
                    "shopify_product_id": result['id']
                })
            else:
                logging.error("FAILED: Shopify product creation failed")
                return jsonify({
                    "status": "partial_success",
                    "processed_url": processed_url,
                    "ai_tags": ai_tags,
                    "error": "Shopify product creation failed"
                }), 500
                
        except Exception as e:
            logging.error(f"Shopify integration failed: {e}", exc_info=True)
            return jsonify({
                "status": "partial_success",
                "processed_url": processed_url,
                "ai_tags": ai_tags,
                "error": f"Shopify integration failed: {str(e)}"
            }), 500
        
        # クリーンアップ
        os.unlink(input_path)
        os.unlink(output_path)
        
    except Exception as e:
        logging.error(f"Processing failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """ヘルスチェックエンドポイント"""
    return jsonify({"status": "healthy", "service": "process-new-video-v2"})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
