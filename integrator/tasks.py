import hashlib
import json
import logging
import random
from celery import shared_task
from celery.signals import worker_process_init
from requests.exceptions import RequestException
from celery import chord
from celery import Task
from .models import ProductSyncState
from .services import stream_erp_data
from .api_client import EShopAPIClient, RateLimitExceeded

logger = logging.getLogger(__name__)

class EShopSyncTask(Task):
    _api_client = None

    @property
    def api_client(self):
        if self._api_client is None:
            self._api_client = EShopAPIClient()
        return self._api_client

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
    
    tasks_to_run = []
    for sku, (payload, prod_hash) in chunk.items():
        if sku in existing_states and existing_states[sku] == prod_hash:
            continue 
            
        is_new = sku not in existing_states
        tasks_to_run.append(sync_single_product.s(payload, prod_hash, is_new))
        
    if tasks_to_run:
        chord(tasks_to_run)(bulk_save_sync_states.s())
        
    return len(tasks_to_run)

@shared_task(bind=True, base=EShopSyncTask, max_retries=10, rate_limit='5/s')
def sync_single_product(self, payload: dict, prod_hash: str, is_new: bool):
    sku = payload['sku']

    try:
        self.api_client.sync_product(sku, payload, is_new, prod_hash)
        return {'sku': sku, 'hash': prod_hash}
        
    except RateLimitExceeded as exc:
        logger.warning(f"Rate limit hit for {sku}. Retrying in {exc.retry_after}s.")
        raise self.retry(countdown=exc.retry_after, exc=exc)

@shared_task
def bulk_save_sync_states(results):
    from .models import ProductSyncState
    
    successful_results = [r for r in results if isinstance(r, dict) and 'sku' in r]
    if not successful_results:
        return
        
    states_to_update = []
    for res in successful_results:
        state = ProductSyncState(sku=res['sku'], data_hash=res['hash'])
        states_to_update.append(state)
        
    ProductSyncState.objects.bulk_create(
        states_to_update,
        update_conflicts=True,
        unique_fields=['sku'],
        update_fields=['data_hash', 'last_synced']
    )