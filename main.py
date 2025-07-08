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
SHOPIFY_SHOP_DOMAIN = os.getenv('SHOPIFY_SHOP_DOMAIN', 'tategatafree.myshopify.com')

# Google Cloudクライアント
secret_client = secretmanager.SecretManagerServiceClient()
app = Flask(__name__)

def get_shopify_access_token():
    """Secret ManagerからShopify APIトークンを取得し、必ず文字列として返す"""
    try:
        secret_name = f"projects/{PROJECT_ID}/secrets/shopify-admin-api-token/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_name})
        token = response.payload.data.decode("UTF-8").strip()
        logging.info(f"Successfully retrieved token of length {len(token)}")
        return token
    except Exception as e:
        logging.error(f"Failed to get Shopify access token: {e}", exc_info=True)
        raise

class ShopifyAPIClient:
    """Shopify Admin API クライアント"""
    def __init__(self, shop_domain: str, access_token: str):
        self.shop_domain = shop_domain.replace('https://','').replace('http://','').split('/')[0]
        self.access_token = access_token
        self.base_url = f"https://{self.shop_domain}/admin/api/2024-04"
        self.headers = {'X-Shopify-Access-Token': self.access_token, 'Content-Type': 'application/json'}

    def create_product(self, product_data: dict) -> Optional[Dict]:
        """Shopifyに商品を作成し、詳細なログを出力する"""
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
                product = response.json().get('product')
                logging.info(f"SUCCESS: Product creation successful: {product.get('title')} (ID: {product.get('id')})")
                return product
            else:
                logging.error(f"FAILED: Shopify API returned non-201 status.")
                return None
        except Exception as e:
            logging.error(f"EXCEPTION during Shopify API call: {e}", exc_info=True)
            return None

@app.route('/test-shopify', methods=['GET'])
def shopify_test_endpoint():
    """ShopifyへのAPI呼び出しだけをテストする診断用エンドポイント"""
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

@app.route('/')
def index():
    """メインの動画処理エンドポイント（現在は診断のため無効化）"""
    logging.info("Main endpoint received a request, but is currently disabled for testing.")
    return "OK, but video processing is disabled pending diagnostics.", 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
