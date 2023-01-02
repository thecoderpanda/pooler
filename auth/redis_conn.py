import contextlib

import redis
import redis.exceptions as redis_exc
from redis import asyncio as aioredis

from .conf import auth_settings


def construct_redis_url():
    if auth_settings.redis.password:
        return f'redis://{auth_settings.redis.password}@{auth_settings.redis.host}:{auth_settings.redis.port}'\
               f'/{auth_settings.redis.db}'
    else:
        return f'redis://{auth_settings.redis.host}:{auth_settings.redis.port}/{auth_settings.redis.db}'


async def get_aioredis_pool(pool_size=200):
    return await aioredis.from_url(
        url=construct_redis_url(),
        retry_on_error=[redis.exceptions.ReadOnlyError, ],
        max_connections=pool_size
    )


@contextlib.contextmanager
def create_redis_conn(connection_pool: redis.BlockingConnectionPool) -> redis.Redis:
    """
    Contextmanager that will create and teardown a session.
    """
    try:
        redis_conn = redis.Redis(connection_pool=connection_pool)
        yield redis_conn
    except redis_exc.RedisError:
        raise
    except KeyboardInterrupt:
        pass


class RedisPoolCache:
    def __init__(self, pool_size=500):
        self._aioredis_pool = None
        self._pool_size = pool_size

    async def populate(self):
        if not self._aioredis_pool:
            self._aioredis_pool: aioredis.Redis = await get_aioredis_pool(self._pool_size)
