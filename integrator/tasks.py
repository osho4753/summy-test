"""
Celery tasks for ERP to E-shop synchronization.

This module implements Delta Sync logic:
- Only products with changed data are sent to the API
- Uses SHA-256 hash to detect changes
- Handles rate limiting with automatic retries
"""
import hashlib
import json
import time

import requests
from celery import shared_task

from integrator.models import ProductSyncState
from integrator.services import parse_and_transform_erp_data


# E-shop API configuration
ESHOP_API_BASE_URL = "https://api.fake-eshop.cz/v1/products"
ESHOP_API_HEADERS = {"X-Api-Key": "symma-secret-token"}

# Rate limiting: max 4 requests per second (API limit is 5 req/s)
REQUEST_DELAY = 0.25


@shared_task(bind=True, max_retries=5)
def sync_erp_to_eshop(self):
    """
    Synchronize ERP product data to e-shop API using Delta Sync.
    
    - Reads and transforms data from erp_data.json
    - Computes hash for each product to detect changes
    - Sends only new or modified products to the API
    - POST for new products, PATCH for updates
    - Handles rate limiting (429) with automatic retry
    
    Returns:
        dict: Summary of sync operation with counts of created, updated, skipped products
    """
    # Get transformed product data from ERP
    products = parse_and_transform_erp_data()
    
    # Counters for sync summary
    stats = {
        'created': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0,
    }
    
    for product in products:
        sku = product['sku']
        
        # Calculate SHA-256 hash of product data for change detection
        product_hash = _calculate_hash(product)
        
        # Check if product was previously synced
        try:
            sync_state = ProductSyncState.objects.get(sku=sku)
            
            # Product exists in our sync state
            if sync_state.data_hash == product_hash:
                # Hash matches - no changes, skip this product
                stats['skipped'] += 1
                continue
            
            # Hash differs - product was modified, use PATCH
            method = 'PATCH'
            url = f"{ESHOP_API_BASE_URL}/{sku}/"
            
        except ProductSyncState.DoesNotExist:
            # New product - use POST
            sync_state = None
            method = 'POST'
            url = f"{ESHOP_API_BASE_URL}/"
        
        # Send request to e-shop API
        try:
            response = _send_to_eshop(method, url, product)
            
            # Handle rate limiting
            if response.status_code == 429:
                # Rate limit exceeded - retry the entire task after 10 seconds
                raise self.retry(
                    countdown=10,
                    exc=Exception(f"Rate limit exceeded while processing {sku}")
                )
            
            # Check for successful response
            if response.status_code in (200, 201):
                # Save or update sync state with new hash
                if sync_state:
                    sync_state.data_hash = product_hash
                    sync_state.save()
                    stats['updated'] += 1
                else:
                    ProductSyncState.objects.create(
                        sku=sku,
                        data_hash=product_hash
                    )
                    stats['created'] += 1
            else:
                # Log error but continue with other products
                stats['errors'] += 1
                
        except requests.RequestException as e:
            # Network error - log and continue
            stats['errors'] += 1
        
        # Rate limiting: wait before next request
        time.sleep(REQUEST_DELAY)
    
    return stats


def _calculate_hash(product: dict) -> str:
    """
    Calculate SHA-256 hash of product data.
    
    Uses sorted keys to ensure consistent hash regardless of dict ordering.
    
    Args:
        product: Product dictionary to hash
        
    Returns:
        str: Hexadecimal SHA-256 hash (64 characters)
    """
    product_json = json.dumps(product, sort_keys=True).encode('utf-8')
    return hashlib.sha256(product_json).hexdigest()


def _send_to_eshop(method: str, url: str, data: dict) -> requests.Response:
    """
    Send product data to e-shop API.
    
    Args:
        method: HTTP method ('POST' or 'PATCH')
        url: API endpoint URL
        data: Product data to send
        
    Returns:
        requests.Response: API response object
    """
    if method == 'POST':
        return requests.post(url, json=data, headers=ESHOP_API_HEADERS)
    else:
        return requests.patch(url, json=data, headers=ESHOP_API_HEADERS)
