"""
Enhanced Cloud Run Service with Shopify Integration (Final Diagnostic Version)
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
import traceback

# --- ロギング設定の強化 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 初期設定 ---
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
    try:
        secret_name = f"projects/{PROJECT_ID}/secrets/shopify-admin-api-token/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        token = response.payload.data.decode("UTF-8").strip()
        logging.info(f"Successfully retrieved token of length {len(token)}")
        return token
    except Exception as e:
        logging.error(f"Failed to get Shopify access token: {e}", exc_info=True)
        raise

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
    
    def create_product(self, product_data: dict) -> Optional[Dict]:
        logging.info("--- Preparing to send request to Shopify ---")
        logging.info(f"Request URL: {self.base_url}/products.json")
        masked_headers = self.headers.copy()
        if 'X-Shopify-Access-Token' in masked_headers:
            masked_headers['X-Shopify-Access-Token'] = '***REDACTED***'
        logging.info(f"Request Headers: {json.dumps(masked_headers, indent=2)}")
        logging.info(f"Request Body: {json.dumps(product_data, indent=2, ensure_ascii=False)}")
        
        try:
            response = requests.post(f"{self.base_url}/products.json", headers=self.headers, json=product_data, timeout=30)
            logging.info("--- Received response from Shopify ---")
            logging.info(f"Response Status Code: {response.status_code}")
            logging.info(f"Response Headers: {json.dumps(dict(response.headers), indent=2)}")
            logging.info(f"Response Body: {response.text}")
            
            if response.status_code == 201:
                product = response.json()['product']
                logging.info(f"SUCCESS: Product creation successful: {product['title']} (ID: {product['id']})")
                return product
            else:
                logging.error(f"FAILED: Shopify API returned non-201 status.")
                return None
        except Exception as e:
            logging.error(f"EXCEPTION during Shopify API call: {e}", exc_info=True)
            return None

# ★★★★★ ここからが新しい診断用コード ★★★★★
@app.route('/test-shopify')
def shopify_test_endpoint():
    logging.info("--- Shopify Test Endpoint Triggered ---")
    try:
        token = get_shopify_access_token()
        if not token:
            return "Failed to get Shopify token from Secret Manager", 500

        client = ShopifyAPIClient(SHOPIFY_SHOP_DOMAIN, token)
        
        test_product_payload = {
            "product": {
                "title": f"API Test Product - {os.urandom(4).hex()}",
                "body_html": "<strong>This is a test product created via API call.</strong>",
                "vendor": "API Test",
                "product_type": "Digital Goods",
                "status": "draft"
            }
        }
        
        result = client.create_product(test_product_payload)
        
        if result:
            return f"<h1>Success!</h1><p>Product created with ID: {result.get('id')}</p><pre>{json.dumps(result, indent=2, ensure_ascii=False)}</pre>", 200
        else:
            return "<h1>Failed.</h1><p>Check the Cloud Run logs for detailed request/response information.</p>", 500
            
    except Exception as e:
        logging.error(f"Critical error in /test-shopify endpoint: {e}", exc_info=True)
        return f"<h1>Internal Server Error</h1><p>{traceback.format_exc()}</p>", 500
# ★★★★★ ここまでが新しい診断用コード ★★★★★

# 元の動画処理用のエンドポイント
@app.route('/', methods=['POST'])
def index():
    # 本番用の動画処理は、テストが成功するまで一旦何もしないようにします
    logging.info("Main endpoint received a request, but is currently disabled for testing.")
    return "OK, but processing is disabled pending diagnostics.", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))
