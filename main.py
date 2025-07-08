"""
Full Shopify Integration Video Processor
"""
import os
import tempfile
import ffmpeg
import logging
import json
from flask import Flask, request
from google.cloud import storage, firestore, videointelligence, secretmanager
import requests
from typing import Optional, Dict

# --- Basic Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- GCP and App Configuration from Environment Variables ---
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
ORIGINALS_BUCKET_NAME = os.getenv('ORIGINALS_BUCKET')
PROCESSED_BUCKET_NAME = os.getenv('PROCESSED_BUCKET')
SHOPIFY_SHOP_DOMAIN = os.getenv('SHOPIFY_SHOP_DOMAIN')
DEFAULT_PRODUCT_PRICE = float(os.getenv('DEFAULT_PRODUCT_PRICE', '500.0'))
WATERMARK_FILE = 'watermark.png' # Included in the container

# --- Initialize Google Cloud Clients ---
storage_client = storage.Client()
firestore_client = firestore.Client()
video_intelligence_client = videointelligence.VideoIntelligenceServiceClient()
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
        if not shop_domain or not access_token:
            raise ValueError("Shopify domain and access token must be provided.")
        self.shop_domain = shop_domain
        self.base_url = f"https://{self.shop_domain}/admin/api/2024-04"
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
            response.raise_for_status()  # Raises an exception for bad status codes (4xx or 5xx)
            
            product = response.json().get('product')
            logging.info(f"SUCCESS: Product created with ID: {product.get('id')}")
            return product
        except requests.exceptions.RequestException as e:
            logging.error(f"FAILED: Shopify API request failed. Status: {e.response.status_code if e.response else 'N/A'}. Body: {e.response.text if e.response else 'No response'}", exc_info=True)
            return None

# --- Main Video Processing Logic ---
def process_video_and_create_product(bucket_name: str, file_name: str):
    """
    Main workflow: downloads, processes video, and creates a Shopify product.
    NOTE: Video processing (ffmpeg, AI analysis) is stubbed out for this version.
    """
    logging.info(f"Starting processing for gs://{bucket_name}/{file_name}")

    try:
        # --- Step 1: Initialize API Clients and Get Credentials ---
        shopify_access_token = get_secret('shopify-admin-api-token')
        shopify_client = ShopifyAPIClient(SHOPIFY_SHOP_DOMAIN, shopify_access_token)

        # --- Step 2: (Stubbed) Video Processing ---
        # In a real scenario, you would download, apply watermark, and re-upload.
        # For now, we will just use placeholders.
        logging.info("Skipping actual video processing (ffmpeg, AI analysis) for this version.")
        
        # --- Step 3: Prepare Product Data ---
        # This data would eventually be enriched by AI analysis.
        generated_title = f"Vertical Video - {os.path.splitext(file_name)[0]}"
        tags = ["vertical video", "stock footage", "ai generated"]
        
        product_payload = {
            "product": {
                "title": generated_title,
                "body_html": f"High-quality vertical video: <strong>{file_name}</strong>. Ready for your social media projects.",
                "vendor": "Auto-System Inc.",
                "product_type": "Digital Video",
                "status": "draft", # Create as a draft to be reviewed later
                "tags": ", ".join(tags),
                "variants": [{
                    "price": str(DEFAULT_PRODUCT_PRICE),
                    "sku": f"VID-{os.path.splitext(file_name)[0]}"
                }],
            }
        }

        # --- Step 4: Create Product in Shopify ---
        product_result = shopify_client.create_product(product_payload)
        
        if not product_result:
            raise Exception("Failed to create product in Shopify. See previous logs for API response.")

        logging.info(f"Workflow completed successfully for {file_name}")

    except Exception as e:
        logging.error(f"CRITICAL ERROR in workflow for {file_name}: {e}", exc_info=True)
        # Re-raise to ensure Cloud Run knows the execution failed
        raise

# --- Flask Web Server Entrypoint ---
@app.route('/process', methods=['POST'])
def index():
    """Receives event from GCS trigger and starts the processing workflow."""
    event_data = request.get_json()
    if not event_data or 'bucket' not in event_data or 'name' not in event_data:
        logging.error("Malformed request received.")
        return "Bad Request: Invalid event payload", 400
    
    try:
        process_video_and_create_product(event_data['bucket'], event_data['name'])
        return "OK", 200
    except Exception as e:
        # The raised exceptions from the workflow will be caught here
        logging.error(f"Endpoint error: {e}", exc_info=True)
        return f"Internal Server Error", 500

if __name__ == '__main__':
    # PORT is automatically set by Cloud Run.
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))
