"""
Full Shopify Integration Video Processor with Idempotency Check and Media Attachment (Final Production Version)
"""
import os
import logging
import json
from flask import Flask, request
from google.cloud import storage, secretmanager
import requests
from typing import Optional, Dict, List

# --- Basic Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- GCP and App Configuration from Environment Variables ---
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
ORIGINALS_BUCKET_NAME = os.getenv('ORIGINALS_BUCKET')
# PROCESSED_BUCKET_NAME = os.getenv('PROCESSED_BUCKET') # 透かし処理済み動画の保存先（将来利用）
SHOPIFY_SHOP_DOMAIN = os.getenv('SHOPIFY_SHOP_DOMAIN')
DEFAULT_PRODUCT_PRICE = float(os.getenv('DEFAULT_PRODUCT_PRICE', '500.0'))

# --- Initialize Google Cloud Clients ---
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()

app = Flask(__name__)

# --- Helper Functions ---
def get_secret(secret_name: str) -> str:
    """Fetches a secret from Google Cloud Secret Manager."""
    try:
        full_secret_name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
        response = secret_client.access_secret_version(request={"name": full_secret_name})
        return response.payload.data.decode("UTF-8").strip()
    except Exception as e:
        logging.error(f"Failed to access secret: {secret_name}. Error: {e}", exc_info=True)
        raise

# --- Shopify API Client Class ---
class ShopifyAPIClient:
    """A client to interact with the Shopify Admin API."""
    def __init__(self, shop_domain: str, access_token: str):
        self.shop_domain = shop_domain
        self.base_url = f"https://{self.shop_domain}/admin/api/2024-07"
        self.headers = {
            'X-Shopify-Access-Token': access_token,
            'Content-Type': 'application/json'
        }

    def find_product_by_title(self, title: str) -> Optional[List[Dict]]:
        """Finds products by an exact title match."""
        url = f"{self.base_url}/products.json?title={requests.utils.quote(title)}&fields=id,title"
        logging.info(f"Searching for product with title: {title}")
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            products = response.json().get('products', [])
            if products:
                logging.info(f"Found {len(products)} existing product(s) with the same title.")
                return products
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"FAILED: Shopify product search failed.", exc_info=True)
            return None

    def create_product(self, product_data: dict) -> Optional[Dict]:
        """Creates a product on Shopify."""
        url = f"{self.base_url}/products.json"
        logging.info(f"Creating product with title: {product_data.get('product', {}).get('title')}")
        try:
            response = requests.post(url, headers=self.headers, json=product_data, timeout=30)
            response.raise_for_status()
            product = response.json().get('product')
            logging.info(f"SUCCESS: Product created with ID: {product.get('id')}")
            return product
        except requests.exceptions.RequestException as e:
            logging.error(f"FAILED: Shopify product creation failed.", exc_info=True)
            return None

    def attach_video_media(self, product_id: int, original_video_url: str) -> Optional[Dict]:
        """Attaches a video to a product using a URL."""
        url = f"{self.base_url}/products/{product_id}/media.json"
        logging.info(f"Attaching video to product ID: {product_id}")
        media_payload = { "media": { "original_source": original_video_url, "media_type": "VIDEO" } }
        try:
            response = requests.post(url, headers=self.headers, json=media_payload, timeout=90) # タイムアウトを延長
            response.raise_for_status()
            media = response.json().get('media')
            logging.info(f"SUCCESS: Video media attachment process started. Media ID: {media.get('id')}")
            return media
        except requests.exceptions.RequestException as e:
            logging.error(f"FAILED: Shopify media attachment failed.", exc_info=True)
            return None

# --- Main Logic ---
def main_workflow(bucket_name: str, file_name: str):
    """Main workflow with idempotency check."""
    logging.info(f"Starting workflow for gs://{bucket_name}/{file_name}")

    try:
        shopify_access_token = get_secret('shopify-admin-api-token')
        shopify_client = ShopifyAPIClient(SHOPIFY_SHOP_DOMAIN, shopify_access_token)
        
        # --- (未実装) ここで動画を解析し、ユニークなタイトルを生成します ---
        generated_title = f"Video - {os.path.splitext(file_name)[0]}"

        # === 重複防止チェック ===
        if shopify_client.find_product_by_title(generated_title):
            logging.warning(f"Product '{generated_title}' already exists. Skipping workflow.")
            return

        # === 商品ページの作成 ===
        # --- (未実装) ここで解析結果のタグなどもペイロードに含めます ---
        product_payload = { "product": { "title": generated_title, "status": "draft" } }
        product_result = shopify_client.create_product(product_payload)
        if not product_result or not product_result.get('id'):
            raise Exception("Failed to create product or get product_id.")
        
        product_id = product_result.get('id')
        
        # === 動画メディアの紐付け ===
        # --- (未実装) ここでは元の動画をそのまま使いますが、将来的には透かし入り動画のURLを使います ---
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        signed_url = blob.generate_signed_url(version="v4", expiration=3600) # URLは1時間有効
        logging.info(f"Generated signed URL for Shopify.")
        
        media_result = shopify_client.attach_video_media(product_id, signed_url)
        if not media_result:
            raise Exception("Product listing created, but failed to attach video media.")

        logging.info(f"Workflow for {file_name} completed successfully.")

    except Exception as e:
        logging.error(f"CRITICAL ERROR in workflow for {file_name}: {e}", exc_info=True)
        raise

# --- Flask Web Server Entrypoint ---
@app.route('/', methods=['POST'])
def index():
    """Receives event from GCS trigger and starts the workflow."""
    event_data = request.get_json()
    if not event_data or 'bucket' not in event_data or 'name' not in event_data:
        return "Bad Request: Invalid event payload", 400
    
    try:
        main_workflow(event_data['bucket'], event_data['name'])
        return "OK", 200
    except Exception:
        # エラーの詳細はmain_workflow内でログ記録済みのため、ここでは汎用エラーを返す
        return "Internal Server Error", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))
