import redis
from app.core.config import settings

kwargs = {"decode_responses": True}
if settings.REDIS_PASSWORD:
    kwargs["password"] = settings.REDIS_PASSWORD

pool = redis.ConnectionPool.from_url(settings.REDIS_URL, **kwargs)
redis_client = redis.Redis(connection_pool=pool)

def get_redis_client():
    return redis_client
