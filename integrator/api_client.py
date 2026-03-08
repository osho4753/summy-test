import requests
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

class RateLimitExceeded(Exception):
    def __init__(self, retry_after: int):
        self.retry_after = retry_after

class EShopAPIClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"X-Api-Key": settings.ESHOP_API_KEY})
        self.base_url = settings.ESHOP_API_BASE_URL

    def sync_product(self, sku: str, payload: dict, is_new: bool, payload_hash: str):
        method = 'POST' if is_new else 'PATCH'
        url = f"{self.base_url}/" if is_new else f"{self.base_url}/{sku}/"
        
        headers = {"Idempotency-Key": payload_hash}

        response = self.session.request(method, url, json=payload, headers=headers)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 10))
            raise RateLimitExceeded(retry_after)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            logger.error(f"Ошибка API для {sku}: {response.text}")
            raise e  