import time
from redis import Redis
from django.conf import settings

redis_client = Redis.from_url(settings.CELERY_BROKER_URL)

def wait_for_rate_limit(limit_per_second: int = 5):
    current_second = int(time.time())
    redis_key = f"eshop_api_limit:{current_second}"
    
    current_requests = redis_client.incr(redis_key)
    
    if current_requests == 1:
        redis_client.expire(redis_key, 5)
        
    if current_requests > limit_per_second:
        sleep_time = 1.0 - (time.time() % 1)
        time.sleep(sleep_time)
        return wait_for_rate_limit(limit_per_second)