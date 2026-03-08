import hashlib
import json
import logging
import random
import requests
from celery import shared_task
from celery.signals import worker_process_init
from django.conf import settings
from .models import ProductSyncState
from .services import stream_erp_data

logger = logging.getLogger(__name__)

_http_session = None

@worker_process_init.connect
def init_worker_session(**kwargs):
    global _http_session
    _http_session = requests.Session()
    _http_session.headers.update({"X-Api-Key": settings.ESHOP_API_KEY})

def _calculate_hash(payload: dict) -> str:
    json_str = json.dumps(payload, sort_keys=True).encode('utf-8')
    return hashlib.sha256(json_str).hexdigest()

@shared_task
def sync_erp_to_eshop():
    chunk_size = 500
    current_chunk = {}
    dispatched_count = 0

    for product in stream_erp_data():
        payload = product.to_eshop_payload()
        current_chunk[product.sku] = (payload, _calculate_hash(payload))

        if len(current_chunk) >= chunk_size:
            dispatched_count += _dispatch_delta_tasks(current_chunk)
            current_chunk = {}

    if current_chunk:
        dispatched_count += _dispatch_delta_tasks(current_chunk)

    return {"dispatched_tasks": dispatched_count}

def _dispatch_delta_tasks(chunk: dict) -> int:
    skus = list(chunk.keys())
    existing_states = dict(
        ProductSyncState.objects.filter(sku__in=skus).values_list('sku', 'data_hash')
    )
    
    dispatched = 0
    for sku, (payload, prod_hash) in chunk.items():
        if sku in existing_states and existing_states[sku] == prod_hash:
            continue 
            
        is_new = sku not in existing_states
        
        sync_single_product.delay(payload, prod_hash, is_new)
        dispatched += 1
        
    return dispatched

@shared_task(bind=True, rate_limit='5/s', max_retries=10)
def sync_single_product(self, payload: dict, prod_hash: str, is_new: bool):
    sku = payload['sku']
    method = 'POST' if is_new else 'PATCH'
    url = f"{settings.ESHOP_API_BASE_URL}/" if is_new else f"{settings.ESHOP_API_BASE_URL}/{sku}/"
    
    try:
        response = _http_session.request(method, url, json=payload)
        
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 0))
            if retry_after == 0:
                retry_after = (2 ** self.request.retries) + random.uniform(0, 1)
            raise self.retry(countdown=retry_after, exc=Exception("Rate Limit 429"))
            
        if response.status_code in (200, 201):
              ProductSyncState.objects.update_or_create(
                sku=sku,
                defaults={'data_hash': prod_hash}
            )
        else:
            logger.error(f"Sync failed for {sku}: {response.status_code} - {response.text}")
            
    except requests.RequestException as exc:
        raise self.retry(countdown=(2 ** self.request.retries) + random.uniform(0, 1), exc=exc)