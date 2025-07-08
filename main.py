"""
Full Shopify Integration Video Processor (Final Version for GCS Trigger)
"""
import os
import logging
import json
from flask import Flask, request
from google.cloud import storage, secretmanager
import requests
from typing import Optional, Dict

# --- Basic Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- GCP and App Configuration from Environment Variables ---
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
ORIGINALS_BUCKET_NAME = os.getenv('ORIGINALS_BUCKET')
SHOPIFY_SHOP_DOMAIN = os.getenv('SHOPIFY_SHOP_DOMAIN')
DEFAULT_PRODUCT_PRICE = float(os.getenv('DEFAULT_PRODUCT_PRICE', '500.0'))

# --- Initialize Google Cloud Clients ---
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()

app = Flask(__name__)

# --- Helper Functions ---
def get_secret(secret_name: str) -> str:
    """Fetches a secret from Google Cloud Secret Manager with enhanced cleaning."""
    try:
        full_secret_name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
        response = secret_client.access_secret_version(request={"name": full_secret_name})
        
        # バイトデータを取得
        raw_data = response.payload.data
        logging.info(f"Secret '{secret_name}' - Raw data type: {type(raw_data)}, length: {len(raw_data)}")
        
        # UTF-8でデコード
        token = raw_data.decode("UTF-8")
        logging.info(f"Secret '{secret_name}' - After decode type: {type(token)}, length: {len(token)}")
        
        # BOMを除去（UTF-8 BOM: \ufeff）
        if token.startswith('\ufeff'):
            logging.info(f"Secret '{secret_name}' - Removing UTF-8 BOM")
            token = token[1:]
        
        # 前後の空白、改行、制御文字を除去
        original_length = len(token)
        token = token.strip().replace('\n', '').replace('\r', '').replace('\t', '')
        
        if len(token) != original_length:
            logging.info(f"Secret '{secret_name}' - Cleaned {original_length - len(token)} characters")
        
        # 最終確認
        logging.info(f"Secret '{secret_name}' - Final type: {type(token)}, length: {len(token)}")
        
        return token
        
    except Exception as e:
        logging.error(f"Failed to access secret: {secret_name}. Error: {e}", exc_info=True)
        raise

# --- Shopify API Client Class ---
class ShopifyAPIClient:
    """A client to interact with the Shopify Admin API."""
    def __init__(self, shop_domain: str, access_token: str):
        if not shop_domain or not access_token:
            raise ValueError("Shopify domain and access token must be provided.")
        self.shop_domain = shop_domain
        self.base_url = f"https://{self.shop_domain}/admin/api/2024-07"
        self.headers = {
            'X-Shopify-Access-Token': access_token,
            'Content-Type': 'application/json'
        }

    def create_product(self, product_data: dict) -> Optional[Dict]:
        """Creates a product on Shopify and returns the response."""
        url = f"{self.base_url}/products.json"
        logging.info(f"Creating product with title: {product_data.get('product', {}).get('title')}")
        try:
            response = requests.post(url, headers=self.headers, json=product_data, timeout=30)
            response.raise_for_status()
            product = response.json().get('product')
            logging.info(f"SUCCESS: Product created with ID: {product.get('id')}")
            return product
        except requests.exceptions.RequestException as e:
            logging.error(f"FAILED: Shopify product creation failed. Status: {e.response.status_code if e.response else 'N/A'}. Body: {e.response.text if e.response else 'No response'}", exc_info=True)
            return None

    def attach_video_media(self, product_id: int, original_video_url: str) -> Optional[Dict]:
        """Attaches a video to a product using a URL."""
        url = f"{self.base_url}/products/{product_id}/media.json"
        logging.info(f"Attaching video to product ID: {product_id}")
        media_payload = { "media": { "original_source": original_video_url, "media_type": "VIDEO" } }
        try:
            response = requests.post(url, headers=self.headers, json=media_payload, timeout=60)
            response.raise_for_status()
            media = response.json().get('media')
            logging.info(f"SUCCESS: Video media attached. Media ID: {media.get('id')}")
            return media
        except requests.exceptions.RequestException as e:
            logging.error(f"FAILED: Shopify media attachment failed. Status: {e.response.status_code if e.response else 'N/A'}. Body: {e.response.text if e.response else 'No response'}", exc_info=True)
            return None

# --- Main Video Processing Logic ---
def process_video_and_create_product(bucket_name: str, file_name: str):
    """Main workflow: processes video and creates a Shopify product with video media."""
    logging.info(f"Starting processing for gs://{bucket_name}/{file_name}")

    try:
        shopify_access_token = get_secret('shopify-admin-api-token')
        shopify_client = ShopifyAPIClient(SHOPIFY_SHOP_DOMAIN, shopify_access_token)
        
        product_result = shopify_client.create_product({ "product": { "title": f"Video - {file_name}", "status": "draft" } })
        if not product_result or not product_result.get('id'):
            raise Exception("Failed to create product listing or get product_id.")
        
        product_id = product_result.get('id')
        
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        # ShopifyのMedia APIは公開URLを必要とするため、一時的な署名付きURLを生成します
        signed_url = blob.generate_signed_url(version="v4", expiration=3600) # URLは1時間有効
        logging.info(f"Generated signed URL for Shopify")
        
        media_result = shopify_client.attach_video_media(product_id, signed_url)
        if not media_result:
            raise Exception("Product listing created, but failed to attach video media.")

        logging.info(f"Workflow completed successfully for {file_name}")

    except Exception as e:
        logging.error(f"CRITICAL ERROR in workflow for {file_name}: {e}", exc_info=True)
        raise

# --- Flask Web Server Entrypoint ---
@app.route('/', methods=['POST'])
def index():
    """Receives event from GCS trigger and starts the processing workflow."""
    event_data = request.get_json()
    if not event_data or 'bucket' not in event_data or 'name' not in event_data:
        return "Bad Request: Invalid event payload", 400
    
    try:
        process_video_and_create_product(event_data['bucket'], event_data['name'])
        return "OK", 200
    except Exception as e:
        return "Internal Server Error", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))

