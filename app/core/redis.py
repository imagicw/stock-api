import redis
from app.core.config import settings

pool = redis.ConnectionPool.from_url(settings.REDIS_URL, decode_responses=True)
redis_client = redis.Redis(connection_pool=pool)

def get_redis_client():
    return redis_client
