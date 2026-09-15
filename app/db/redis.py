import redis.asyncio as redis

from app.core.config import settings


def create_redis_client():
    return redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )


redis_client = create_redis_client()