"""
Enhanced Cloud Run Service with Shopify Integration (Final Confirmed Version)
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'douga-auto-system')
ORIGINALS_BUCKET = os.getenv('ORIGINALS_BUCKET', 'douga-auto-system-originals')
PROCESSED_BUCKET = os.getenv('PROCESSED_BUCKET', 'douga-auto-system-processed')
WATERMARK_FILE = 'watermark.png'
SHOPIFY_SHOP_DOMAIN = os.getenv('SHOPIFY_SHOP_DOMAIN', 'tategatafree.myshopify.com')
DEFAULT_PRODUCT_PRICE = float(os.getenv('DEFAULT_PRODUCT_PRICE', '500.0'))

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
        return token
    except Exception as e:
        logging.error(f"Failed to get Shopify access token: {e}", exc_info=True)
        raise

class ShopifyAPIClient:
    def __init__(self, shop_domain: str, access_token: str):
        self.shop_domain = shop_domain.replace('https://','').replace('http://','').split('/')[0]
        self.access_token = access_token
        self.base_url = f"https://{self.shop_domain}/admin/api/2024-04"
        self.headers = {'X-Shopify-Access-Token': self.access_token, 'Content-Type': 'application/json'}
    
    def create_product(self, product_data: dict) -> Optional[Dict]:
        logging.info("--- Preparing to send request to Shopify ---")
        logging.info(f"Request URL: {self.base_url}/products.json")
        logging.info(f"Request Body: {json.dumps(product_data, indent=2, ensure_ascii=False)}")
        try:
            response = requests.post(f"{self.base_url}/products.json", headers=self.headers, json=product_data, timeout=30)
            logging.info("--- Received response from Shopify ---")
            logging.info(f"Response Status Code: {response.status_code}")
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

def process_video_file(bucket_name, file_name):
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            generated_title = f"API Test Product - {file_name}"
            tags = ["test", "api", "video"]
            
            logging.info(f"Step 5: Creating Shopify product with hardcoded title: {generated_title}")
            shopify_access_token = get_shopify_access_token()
            client = ShopifyAPIClient(SHOPIFY_SHOP_DOMAIN, shopify_access_token)
            
            product_payload = {
                "product": {
                    "title": generated_title,
                    "body_html": "<strong>This is a test product created via API call.</strong>",
                    "vendor": "API Test",
                    "product_type": "Digital Goods",
                    "status": "draft",
                    "tags": ", ".join(tags),
                    "variants": [{"price": str(DEFAULT_PRODUCT_PRICE)}],
                }
            }
            result = client.create_product(product_payload)

            if result:
                logging.info(f"END: All processing completed successfully for {file_name}")
            else:
                raise Exception("Shopify product creation failed. See previous logs for details.")
        except Exception as e:
            logging.error(f"CRITICAL ERROR in process_video_file for {file_name}: {e}", exc_info=True)

# ★★★★★★★★★★★★★★★★★★★★★★
# 変更点：受付URLを / から /process に変更
# ★★★★★★★★★★★★★★★★★★★★★★
@app.route('/process', methods=['POST'])
def index():
    try:
        event_data = request.get_json()
        if not event_data or 'bucket' not in event_data or 'name' not in event_data:
            return "Bad Request: Invalid event payload", 400
        process_video_file(event_data['bucket'], event_data['name'])
        return "OK", 200
    except Exception as e:
        return f"Internal Server Error: {e}", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))
