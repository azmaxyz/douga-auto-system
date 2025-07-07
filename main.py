"""
Enhanced Cloud Run Service with Shopify Integration (Final Version)
動画処理とShopify商品自動登録を統合したCloud Runサービス
"""

import os
import tempfile
import ffmpeg
import logging
import json
from flask import Flask, request
from google.cloud import storage, firestore, videointelligence, secretmanager
from dataclasses import dataclass
from typing import Dict, List, Optional
import requests

# --- 初期設定 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'douga-auto-system')
ORIGINALS_BUCKET = os.getenv('ORIGINALS_BUCKET', 'douga-auto-system-originals')
PROCESSED_BUCKET = os.getenv('PROCESSED_BUCKET', 'douga-auto-system-processed')
WATERMARK_FILE = 'watermark.png'
SHOPIFY_SHOP_DOMAIN = os.getenv('SHOPIFY_SHOP_DOMAIN', 'tategatafree.myshopify.com')
DEFAULT_PRODUCT_PRICE = float(os.getenv('DEFAULT_PRODUCT_PRICE', '500.0'))

# Google Cloudクライアント
storage_client = storage.Client()
firestore_client = firestore.Client()
video_intelligence_client = videointelligence.VideoIntelligenceServiceClient()
secret_client = secretmanager.SecretManagerServiceClient()

app = Flask(__name__)

def get_shopify_access_token():
    secret_name = f"projects/{PROJECT_ID}/secrets/shopify-admin-api-token/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})
    return response.payload.data.decode("UTF-8")

@dataclass
class VideoProduct:
    title: str; description: str; price: float; preview_video_url: str; main_video_url: str; ai_tags: List[str]; original_filename: str
class ShopifyAPIClient:
    def __init__(self, shop_domain: str, access_token: str):
        self.shop_domain = shop_domain.replace('https://','').replace('http://','')
        if not self.shop_domain.endswith('.myshopify.com'): self.shop_domain += '.myshopify.com'
        self.access_token = access_token
        self.base_url = f"https://{self.shop_domain}/admin/api/2024-04"
        self.headers = {'X-Shopify-Access-Token': self.access_token, 'Content-Type': 'application/json'}
    def create_product(self, video_product: VideoProduct) -> Optional[Dict]:
        product_data = {"product": {"title": video_product.title, "body_html": video_product.description, "vendor": "縦型動画フリー", "product_type": "デジタル動画", "tags": ", ".join(video_product.ai_tags), "variants": [{"price": str(video_product.price), "inventory_management": None, "inventory_policy": "continue", "requires_shipping": False}], "metafields": [{"namespace": "custom", "key": "preview_video_url", "value": video_product.preview_video_url, "type": "url"}, {"namespace": "custom", "key": "main_video_url", "value": video_product.main_video_url, "type": "url"}, {"namespace": "custom", "key": "ai_tags", "value": json.dumps(video_product.ai_tags), "type": "list.single_line_text_field"}, {"namespace": "custom", "key": "original_filename", "value": video_product.original_filename, "type": "single_line_text_field"}]}}
        try:
            response = requests.post(f"{self.base_url}/products.json", headers=self.headers, json=product_data, timeout=30)
            if response.status_code == 201:
                product = response.json()['product']; logging.info(f"商品作成成功: {product['title']} (ID: {product['id']})"); return product
            else:
                logging.error(f"商品作成失敗: {response.status_code} - {response.text}"); return None
        except Exception as e:
            logging.error(f"商品作成中に予期しないエラー: {e}", exc_info=True); return None
def create_video_product_from_data(filename: str, processed_url: str, original_url: str, tags: List[str]) -> VideoProduct:
    title = f"縦型動画 - {os.path.splitext(filename)[0].replace('_', ' ').title()}"
    description = f"この動画には以下の要素が含まれています：{', '.join(tags[:5])}。高品質な縦型動画コンテンツをお楽しみください。" if tags else "高品質な縦型動画コンテンツです。"
    return VideoProduct(title=title, description=description, price=DEFAULT_PRODUCT_PRICE, preview_video_url=processed_url, main_video_url=original_url, ai_tags=tags, original_filename=filename)

@app.route('/', methods=['POST'])
def index():
    try:
        event_data = request.get_json(); bucket_name = event_data['bucket']; file_name = event_data['name']
        logging.info(f"Received event for file: {file_name} in bucket: {bucket_name}")
        process_video_file(bucket_name, file_name)
        return "OK", 200
    except Exception as e:
        logging.error(f"Unhandled error in main handler: {e}", exc_info=True); return "Internal Server Error", 500

def process_video_file(bucket_name, file_name):
    with tempfile.TemporaryDirectory() as temp_dir:
        original_video_path = os.path.join(temp_dir, file_name)
        watermark_path = WATERMARK_FILE
        processed_video_path = os.path.join(temp_dir, f"processed_{file_name}")
        try:
            logging.info(f"Step 1: Downloading original video...")
            download_blob(bucket_name, file_name, original_video_path)
            logging.info(f"Step 2: Adding watermark from local file...")
            add_watermark(original_video_path, watermark_path, processed_video_path)
            logging.info(f"Step 3: Uploading processed video...")
            processed_blob = upload_blob(PROCESSED_BUCKET, os.path.basename(processed_video_path), processed_video_path)
            processed_url = f"https://storage.googleapis.com/{PROCESSED_BUCKET}/{processed_blob.name}"
            logging.info(f"Step 4: Analyzing video...")
            tags = analyze_video_tags(f"gs://{bucket_name}/{file_name}")
            logging.info(f"Step 5: Creating Shopify product...")
            original_url = f"https://storage.googleapis.com/{bucket_name}/{file_name}"
            client = ShopifyAPIClient(SHOPIFY_SHOP_DOMAIN, get_shopify_access_token())
            video_product = create_video_product_from_data(file_name, processed_url, original_url, tags)
            result = client.create_product(video_product)
            if result:
                save_to_firestore(file_name, processed_url, original_url, tags, "SUCCESS")
                logging.info(f"SUCCESS: All processing completed for {file_name}")
            else:
                raise Exception("Shopify product creation failed")
        except Exception as e:
            logging.error(f"Failed to process {file_name}: {e}", exc_info=True)
            save_error_to_firestore(file_name, str(e))
            raise

def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client.bucket(bucket_name).blob(source_blob_name).download_to_filename(destination_file_name)
def upload_blob(bucket_name, destination_blob_name, source_file_name):
    blob = storage_client.bucket(bucket_name).blob(destination_blob_name); blob.upload_from_filename(source_file_name); return blob
def add_watermark(input_path, watermark_path, output_path):
    try:
        ffmpeg.input(input_path).overlay(ffmpeg.input(watermark_path), x='W-w-10', y='H-h-10').output(output_path, vcodec='libx264', preset='fast', crf=23, acodec='copy').run(capture_stdout=True, capture_stderr=True, overwrite_output=True)
    except ffmpeg.Error as e:
        logging.error(f"FFmpeg error: {e.stderr.decode('utf8')}"); raise
def analyze_video_tags(gcs_uri):
    result = video_intelligence_client.annotate_video(request={"features": [videointelligence.Feature.LABEL_DETECTION], "input_uri": gcs_uri}).result(timeout=900)
    return sorted(list(set([item.entity.description for item in result.annotation_results[0].segment_label_annotations])))
def save_to_firestore(file_name, processed_url, original_url, tags, shopify_status):
    doc_ref = firestore_client.collection('videos').document(file_name); doc_data = {'original_file': file_name, 'processed_url': processed_url, 'original_url': original_url, 'tags': tags, 'status': 'PROCESSED_SUCCESS', 'processed_at': firestore.SERVER_TIMESTAMP, 'shopify_status': shopify_status}; doc_ref.set(doc_data)
def save_error_to_firestore(filename: str, error_message: str):
    firestore_client.collection('videos').document(filename).set({'original_file': filename, 'status': 'PROCESSING_FAILED', 'error_message': error_message, 'processed_at': firestore.SERVER_TIMESTAMP}, merge=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))
