import hashlib
import json
import logging
import random
from celery import shared_task
from celery.signals import worker_process_init
from requests.exceptions import RequestException

from .models import ProductSyncState
from .services import stream_erp_data
from .api_client import EShopAPIClient, RateLimitExceeded
from .rate_limiter import wait_for_rate_limit

logger = logging.getLogger(__name__)

_api_client = None

@worker_process_init.connect
def init_worker_session(**kwargs):
    global _api_client
    _api_client = EShopAPIClient()

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
        prod_hash = _calculate_hash(payload)
        
        current_chunk[product.sku] = (payload, prod_hash)

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

@shared_task(bind=True, max_retries=10)
def sync_single_product(self, payload: dict, prod_hash: str, is_new: bool):
    sku = payload['sku']
    
    wait_for_rate_limit(limit_per_second=5)
    
    try:
        _api_client.sync_product(sku, payload, is_new, prod_hash)
        
        ProductSyncState.objects.update_or_create(
            sku=sku,
            defaults={'data_hash': prod_hash}
        )
        logger.info(f"Successfully synced SKU: {sku}")
        
    except RateLimitExceeded as exc:
        logger.warning(f"Rate limit hit for {sku}. Retrying in {exc.retry_after}s.")
        raise self.retry(countdown=exc.retry_after, exc=exc)
        
    except RequestException as exc:
        backoff = (2 ** self.request.retries) + random.uniform(0, 1)
        logger.error(f"API Error for {sku}. Retrying in {backoff:.2f}s. Error: {str(exc)}")
        raise self.retry(countdown=backoff, exc=exc)