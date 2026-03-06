"""
Celery tasks for ERP to E-shop synchronization.

This module implements Delta Sync logic:
- Only products with changed data are sent to the API
- Uses SHA-256 hash to detect changes
- Uses chunking to process products in batches
- Uses Celery's rate_limit for API throttling (non-blocking)
"""
import hashlib
import json

import requests
from celery import shared_task, group

from integrator.models import ProductSyncState
from integrator.services import parse_and_transform_erp_data


# E-shop API configuration
ESHOP_API_BASE_URL = "https://api.fake-eshop.cz/v1/products"
ESHOP_API_HEADERS = {"X-Api-Key": "symma-secret-token"}

# Chunk size for batch processing
CHUNK_SIZE = 100


def _chunk_list(lst: list, chunk_size: int):
    """Split a list into chunks of specified size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


@shared_task
def sync_erp_to_eshop():
    """
    Orchestrator task: reads ERP data and dispatches chunk sub-tasks.
    
    This task does not block the worker. It reads the ERP file, determines which
    products need syncing, groups them into chunks, and dispatches sync_product_chunk
    tasks to the Celery queue. If a chunk fails (e.g., 429), only that chunk retries.
    
    Returns:
        dict: Summary with counts of dispatched, skipped products and chunks
    """
    products = parse_and_transform_erp_data()
    
    products_to_sync = []
    skipped = 0
    
    for product in products:
        sku = product['sku']
        product_hash = _calculate_hash(product)
        
        # Check if product was previously synced with same hash
        try:
            sync_state = ProductSyncState.objects.get(sku=sku)
            if sync_state.data_hash == product_hash:
                skipped += 1
                continue
            is_new = False
        except ProductSyncState.DoesNotExist:
            is_new = True
        
        products_to_sync.append({
            'product': product,
            'hash': product_hash,
            'is_new': is_new,
        })
    
    # Split into chunks and dispatch
    chunks = list(_chunk_list(products_to_sync, CHUNK_SIZE))
    tasks = [sync_product_chunk.s(chunk) for chunk in chunks]
    
    if tasks:
        group(tasks).apply_async()
    
    return {
        'dispatched': len(products_to_sync),
        'skipped': skipped,
        'total': len(products),
        'chunks': len(chunks),
    }


@shared_task(bind=True, max_retries=5)
def sync_product_chunk(self, chunk: list):
    """
    Sync a chunk of products to the e-shop API.
    
    If rate limited (429), only this chunk retries - not the entire sync.
    
    Args:
        chunk: List of dicts with 'product', 'hash', 'is_new' keys
        
    Returns:
        dict: Summary of chunk sync results
    """
    results = {'created': 0, 'updated': 0, 'errors': 0}
    
    for item in chunk:
        product = item['product']
        product_hash = item['hash']
        is_new = item['is_new']
        sku = product['sku']
        
        if is_new:
            method = 'POST'
            url = f"{ESHOP_API_BASE_URL}/"
        else:
            method = 'PATCH'
            url = f"{ESHOP_API_BASE_URL}/{sku}/"
        
        try:
            response = _send_to_eshop(method, url, product)
            
            # Handle rate limiting - retry only this chunk
            if response.status_code == 429:
                raise self.retry(
                    countdown=10,
                    exc=Exception(f"Rate limit exceeded at {sku}, chunk will retry")
                )
            
            if response.status_code in (200, 201):
                ProductSyncState.objects.update_or_create(
                    sku=sku,
                    defaults={'data_hash': product_hash}
                )
                if is_new:
                    results['created'] += 1
                else:
                    results['updated'] += 1
            else:
                results['errors'] += 1
                
        except requests.RequestException:
            results['errors'] += 1
    
    return results


# Keep single product task for backward compatibility and fine-grained control
@shared_task(bind=True, max_retries=5, rate_limit='4/s')
def sync_product_to_eshop(self, product: dict, product_hash: str, is_new: bool):
    """
    Sync a single product to the e-shop API.
    
    Rate limiting is handled by Celery's rate_limit='4/s' (4 requests per second),
    which respects the API limit of 5 req/s with safety margin.
    
    Args:
        product: Product data dictionary
        product_hash: Pre-calculated SHA-256 hash of product data
        is_new: True if product is new (POST), False if update (PATCH)
        
    Returns:
        dict: Result of the sync operation
    """
    sku = product['sku']
    
    if is_new:
        method = 'POST'
        url = f"{ESHOP_API_BASE_URL}/"
    else:
        method = 'PATCH'
        url = f"{ESHOP_API_BASE_URL}/{sku}/"
    
    try:
        response = _send_to_eshop(method, url, product)
        
        # Handle rate limiting with automatic retry
        if response.status_code == 429:
            raise self.retry(
                countdown=10,
                exc=Exception(f"Rate limit exceeded for {sku}")
            )
        
        if response.status_code in (200, 201):
            # Update or create sync state
            ProductSyncState.objects.update_or_create(
                sku=sku,
                defaults={'data_hash': product_hash}
            )
            return {
                'sku': sku,
                'status': 'created' if is_new else 'updated',
            }
        else:
            return {
                'sku': sku,
                'status': 'error',
                'code': response.status_code,
            }
            
    except requests.RequestException as e:
        return {
            'sku': sku,
            'status': 'error',
            'message': str(e),
        }


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
