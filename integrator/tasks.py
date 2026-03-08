import hashlib
import json
import logging
import random
import requests

from celery import shared_task
from celery.signals import worker_process_init
from django.conf import settings
from integrator.models import ProductSyncState
from integrator.services import stream_erp_data_in_batches

logger = logging.getLogger(__name__)

_http_session = None

@worker_process_init.connect
def init_worker_session(**kwargs):
    global _http_session
    _http_session = requests.Session()
    _http_session.headers.update({"X-Api-Key": settings.ESHOP_API_KEY})


def _calculate_hash(product: dict) -> str:
    product_json = json.dumps(product, sort_keys=True).encode('utf-8')
    return hashlib.sha256(product_json).hexdigest()


@shared_task
def sync_erp_to_eshop():
    dispatched_batches = 0
    for batch in stream_erp_data_in_batches():
        process_sync_batch.delay(batch)
        dispatched_batches += 1
        
    return {"dispatched_batches": dispatched_batches}


@shared_task(bind=True, max_retries=10)
def process_sync_batch(self, batch: list[dict]):
    products_with_hashes = {p['sku']: (p, _calculate_hash(p)) for p in batch}
    all_skus = list(products_with_hashes.keys())
    
    existing_states = dict(
        ProductSyncState.objects.filter(sku__in=all_skus).values_list('sku', 'data_hash')
    )
    
    successful_syncs = []
    
    for sku, (product, product_hash) in products_with_hashes.items():
        is_new = sku not in existing_states
        if not is_new and existing_states[sku] == product_hash:
            continue 
            
        method = 'POST' if is_new else 'PATCH'
        url = f"{settings.ESHOP_API_BASE_URL}/" if is_new else f"{settings.ESHOP_API_BASE_URL}/{sku}/"
        
        try:
            response = _http_session.request(method, url, json=product)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 0))
                if retry_after == 0:
                    retry_after = (2 ** self.request.retries) + random.uniform(0, 1)
                
                logger.warning(f"Rate limit for {sku}, retrying batch in {retry_after}s")
                raise self.retry(countdown=retry_after, exc=Exception("Rate Limit 429"))

            if response.status_code in (200, 201):
                successful_syncs.append(ProductSyncState(sku=sku, data_hash=product_hash))
            else:
                logger.error(f"Error syncing {sku}: {response.status_code}")
                
        except requests.RequestException as exc:
            raise self.retry(countdown=(2 ** self.request.retries) + random.uniform(0, 1), exc=exc)

    if successful_syncs:
        ProductSyncState.objects.bulk_create(
            successful_syncs,
            update_conflicts=True,
            unique_fields=['sku'],
            update_fields=['data_hash', 'last_synced']
        )