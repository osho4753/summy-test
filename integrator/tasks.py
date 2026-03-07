import hashlib
import json
import logging

import requests
from celery import shared_task, group
from django.conf import settings

from integrator.models import ProductSyncState
from integrator.services import parse_and_transform_erp_data

logger = logging.getLogger(__name__)

ESHOP_API_BASE_URL = settings.ESHOP_API_BASE_URL

_http_session = requests.Session()
_http_session.headers.update({"X-Api-Key": settings.ESHOP_API_KEY})


@shared_task
def sync_erp_to_eshop():
    products = parse_and_transform_erp_data()
    
    products_with_hashes = [
        (product, _calculate_hash(product))
        for product in products
    ]
    all_skus = [product['sku'] for product in products]
    existing_states = dict(
        ProductSyncState.objects.filter(sku__in=all_skus).values_list('sku', 'data_hash')
    )
    
    tasks = []
    skipped = 0
    
    for product, product_hash in products_with_hashes:
        sku = product['sku']
        if sku in existing_states:
            if existing_states[sku] == product_hash:
                skipped += 1
                continue
            is_new = False
        else:
            is_new = True
        
        tasks.append(sync_product_to_eshop.s(product, product_hash, is_new))
    
    if tasks:
        group(tasks).apply_async()
    
    return {
        'dispatched': len(tasks),
        'skipped': skipped,
        'total': len(products),
    }


@shared_task(bind=True, max_retries=5, rate_limit='4/s')
def sync_product_to_eshop(self, product: dict, product_hash: str, is_new: bool):
    sku = product['sku']
    
    if is_new:
        method = 'POST'
        url = f"{ESHOP_API_BASE_URL}/"
    else:
        method = 'PATCH'
        url = f"{ESHOP_API_BASE_URL}/{sku}/"
    
    try:
        response = _send_to_eshop(method, url, product)
        if response.status_code == 429:
            logger.warning(
                "Rate limit exceeded for SKU %s, scheduling retry in 10s",
                sku
            )
            raise self.retry(
                countdown=10,
                exc=Exception(f"Rate limit exceeded for {sku}")
            )
        
        if response.status_code in (200, 201):
            ProductSyncState.objects.update_or_create(
                sku=sku,
                defaults={'data_hash': product_hash}
            )
            return {
                'sku': sku,
                'status': 'created' if is_new else 'updated',
            }
        else:
            logger.error(
                "Failed to sync SKU %s: API returned status %d, response: %s",
                sku, response.status_code, response.text[:500]
            )
            return {
                'sku': sku,
                'status': 'error',
                'code': response.status_code,
            }
            
    except requests.RequestException as exc:
        logger.error(
            "Failed to sync SKU %s: %s",
            sku, exc,
            exc_info=True  
        )
        return {
            'sku': sku,
            'status': 'error',
            'message': str(exc),
        }


def _decimal_default(obj):
    """JSON serializer for Decimal objects."""
    from decimal import Decimal
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f'Object of type {type(obj).__name__} is not JSON serializable')


def _calculate_hash(product: dict) -> str:
    product_json = json.dumps(product, sort_keys=True, default=_decimal_default).encode('utf-8')
    return hashlib.sha256(product_json).hexdigest()


def _send_to_eshop(method: str, url: str, data: dict) -> requests.Response:
    # Serialize with Decimal support, then send as raw JSON
    json_data = json.dumps(data, default=_decimal_default)
    if method == 'POST':
        return _http_session.post(url, data=json_data, headers={'Content-Type': 'application/json'})
    else:
        return _http_session.patch(url, data=json_data, headers={'Content-Type': 'application/json'})
