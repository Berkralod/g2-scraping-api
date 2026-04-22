import hashlib
import json
import os

from upstash_redis import Redis

redis = Redis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN")
)


def make_cache_key(endpoint: str, params: dict) -> str:
    param_str = json.dumps(params, sort_keys=True)
    param_hash = hashlib.md5(param_str.encode()).hexdigest()[:8]
    return f"g2:{endpoint}:{param_hash}"


def get_cached(key: str):
    try:
        data = redis.get(key)
        return json.loads(data) if data else None
    except Exception:
        return None


def set_cached(key: str, data, ttl: int) -> None:
    try:
        redis.setex(key, ttl, json.dumps(data))
    except Exception:
        pass


def flush_all() -> int:
    keys = redis.keys("*")
    if not keys:
        return 0
    redis.delete(*keys)
    return len(keys)


def list_keys() -> list:
    return redis.keys("*") or []
