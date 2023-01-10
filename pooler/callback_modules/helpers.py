import asyncio
import multiprocessing
import resource
import signal
from functools import partial
from typing import Dict
from typing import Union
from urllib.parse import urljoin
from uuid import uuid4

import aio_pika
import requests
from aio_pika import IncomingMessage
from aio_pika.pool import Pool
from eth_utils import keccak
from httpx import AsyncClient
from loguru import logger
from redis import asyncio as aioredis
from setproctitle import setproctitle
from tenacity import AsyncRetrying
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential

from pooler.callback_modules.data_models import PayloadCommitAPIRequest
from pooler.settings.config import settings
from pooler.utils.default_logger import logger
from pooler.utils.redis.redis_conn import RedisPoolCache

# setup logger
helper_logger = logger.bind(module='PowerLoom|Callback|Helpers')


async def get_rabbitmq_connection():
    return await aio_pika.connect_robust(
        host=settings.rabbitmq.host,
        port=settings.rabbitmq.port,
        virtual_host='/',
        login=settings.rabbitmq.user,
        password=settings.rabbitmq.password,
    )


async def get_rabbitmq_channel(connection_pool) -> aio_pika.Channel:
    async with connection_pool.acquire() as connection:
        return await connection.channel()


class AuditProtocolCommandsHelper:
    @classmethod
    async def set_diff_rule_for_pair_reserves(
            cls, pair_contract_address, session: AsyncClient,
            redis_conn: aioredis.Redis, stream='pair_total_reserves',
    ):
        project_id = f'uniswap_pairContract_{stream}_{pair_contract_address}_{settings.namespace}'
        if not await redis_conn.sismember(f'uniswap:diffRuleSetFor:{settings.namespace}', project_id):
            """ Setup diffRules for this market"""
            # retry below call given at settings.audit_protocol_engine.RETRY
            async for attempt in AsyncRetrying(reraise=True, stop=stop_after_attempt(settings.audit_protocol_engine.retry)):
                with attempt:
                    response_obj = await session.post(
                        url=urljoin(
                            settings.audit_protocol_engine.url,
                            f'/{project_id}/diffRules',
                        ),
                        json={
                            'rules': [
                                {
                                    'ruleType': 'ignore',
                                    'field': 'chainHeightRange',
                                    'fieldType': 'map',
                                    'ignoreMemberFields': ['begin', 'end'],
                                },
                                {
                                    'ruleType': 'compare',
                                    'field': 'token0Reserves',
                                    'fieldType': 'map',
                                    'operation': 'listSlice',
                                    'memberFields': -1,
                                },
                                {
                                    'ruleType': 'compare',
                                    'field': 'token1Reserves',
                                    'fieldType': 'map',
                                    'operation': 'listSlice',
                                    'memberFields': -1,
                                },

                                {
                                    'ruleType': 'ignore',
                                    'field': 'broadcast_id',
                                    'fieldType': 'str',
                                },

                                {
                                    'ruleType': 'ignore',
                                    'field': 'timestamp',
                                    'fieldType': 'float',
                                },
                            ],
                        },
                    )
                    response_status_code = response_obj.status_code
                    response = response_obj.json() or {}
                    helper_logger.debug(
                        'Response code on setting diff rule on audit protocol: {}', response_status_code,
                    )
                    if response_status_code in range(200, 300):
                        await redis_conn.sadd(f'uniswap:diffRuleSetFor:{settings.namespace}', project_id)
                        return {'message': f'success status code: {response_status_code}', 'response': response}
                    elif response_status_code == 500 or response_status_code == 502:
                        return {
                            'message': f'failed with status code: {response_status_code}', 'response': response,
                        }  # ignore 500 and 502 errors
                    else:
                        raise Exception(
                            'Failed audit protocol engine call with status code: {} and response: {}'.format(
                                response_status_code, response,
                            ),
                        )

    @classmethod
    async def set_diff_rule_for_trade_volume(
            cls, pair_contract_address, session: AsyncClient,
            redis_conn: aioredis.Redis = None, stream='trade_volume',

    ):
        project_id = f'uniswap_pairContract_{stream}_{pair_contract_address}_{settings.namespace}'
        if not await redis_conn.sismember(f'uniswap:diffRuleSetFor:{settings.namespace}', project_id):
            """ Setup diffRules for this market"""
            # retry below call given at settings.audit_protocol_engine.RETRY
            async for attempt in AsyncRetrying(reraise=True, stop=stop_after_attempt(settings.audit_protocol_engine.retry)):
                with attempt:
                    response_obj = await session.post(
                        url=urljoin(
                            settings.audit_protocol_engine.url,
                            f'/{project_id}/diffRules',
                        ),
                        json={
                            'rules': [
                                {
                                    'ruleType': 'ignore',
                                    'field': 'chainHeightRange',
                                    'fieldType': 'map',
                                    'ignoreMemberFields': ['begin', 'end'],
                                },
                                {
                                    'ruleType': 'ignore',
                                    'field': 'broadcast_id',
                                    'fieldType': 'str',
                                },
                                {
                                    'ruleType': 'ignore',
                                    'field': 'timestamp',
                                    'fieldType': 'float',
                                },
                                {
                                    'ruleType': 'ignore',
                                    'field': 'events',
                                    'fieldType': 'float',
                                },
                                {
                                    'ruleType': 'ignore',
                                    'field': 'recent_logs',
                                    'fieldType': 'float',
                                },
                            ],
                        },
                    )
                    response_status_code = response_obj.status_code
                    response = response_obj.json() or {}
                    helper_logger.debug(
                        'Response code on setting diff rule on audit protocol: {}', response_status_code,
                    )
                    if response_status_code in range(200, 300):
                        await redis_conn.sadd(f'uniswap:diffRuleSetFor:{settings.namespace}', project_id)
                        return {'message': f'success status code: {response_status_code}', 'response': response}
                    elif response_status_code == 500 or response_status_code == 502:
                        return {
                            'message': f'failed with status code: {response_status_code}', 'response': response,
                        }  # ignore 500 and 502 errors
                    else:
                        raise Exception(
                            'Failed audit protocol engine call with status code: {} and response: {}'.format(
                                response_status_code, response,
                            ),
                        )

    @classmethod
    def set_commit_callback_url(cls, pair_contract_address, stream, redis_conn: aioredis.Redis):
        project_id = f'uniswap_pairContract_{stream}_{pair_contract_address}_{settings.namespace}'
        if not redis_conn.sismember(f'uniswap:{settings.namespace}:callbackURLSetFor', project_id):
            r = requests.post(
                url=urljoin(
                    settings.audit_protocol_engine.url,
                    f'/{project_id}/confirmations/callback',
                ),
                json={
                    'callbackURL': urljoin(
                        settings.webhook_listener.root,
                        settings.webhook_listener.commit_confirmation_callback_path,
                    ),
                },
            )
            if r.status_code in range(200, 300):
                redis_conn.sadd(f'uniswap:{settings.namespace}:callbackURLSetFor', project_id)

    @classmethod
    async def commit_payload(cls, report_payload: PayloadCommitAPIRequest, session: AsyncClient):
        async for attempt in AsyncRetrying(
                reraise=False,
                stop=stop_after_attempt(settings.audit_protocol_engine.retry),
                wait=wait_random_exponential(multiplier=2, max=10),
        ):
            with attempt:
                response_obj = await session.post(
                    url=urljoin(settings.audit_protocol_engine.url, 'commit_payload'),
                    json=report_payload.dict(),
                )
                response_status_code = response_obj.status_code
                response = response_obj.json() or {}
                if response_status_code in range(200, 300):
                    return response
                elif attempt.retry_state.attempt_number == settings.audit_protocol_engine.retry:
                    if attempt.retry_state.outcome and attempt.retry_state.outcome.exception():
                        raise attempt.retry_state.outcome.exception()
                    else:
                        raise Exception(
                            f'Failed audit protocol engine call with status code: {response_status_code} and response: {response}'
                        )
                else:
                    raise Exception(
                        f'Failed audit protocol engine call with status code: {response_status_code} and response: {response}'
                    )


class CallbackAsyncWorker(multiprocessing.Process):
    def __init__(self, name, rmq_q, rmq_routing, **kwargs):
        self._core_rmq_consumer: asyncio.Task
        self._q = rmq_q
        self._rmq_routing = rmq_routing
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]
        self._aioredis_pool = None
        self._redis_conn: Union[None, aioredis.Redis] = None
        self._running_callback_tasks: Dict[str, asyncio.Task] = dict()
        super(CallbackAsyncWorker, self).__init__(name=name, **kwargs)
        self._logger = logger.bind(module=self.name)

        self._shutdown_signal_received_count = 0

    async def _shutdown_handler(self, sig, loop: asyncio.AbstractEventLoop):
        self._shutdown_signal_received_count += 1
        if self._shutdown_signal_received_count > 1:
            self._logger.info(
                f'Received exit signal {sig.name}. Not processing as shutdown sequence was already initiated...',
            )
        else:
            self._logger.info(
                f'Received exit signal {sig.name}. Processing shutdown sequence...',
            )
            # check the done or cancelled status of self._running_callback_tasks.values()
            for u_uid, t in self._running_callback_tasks.items():
                self._logger.debug(
                    'Shutdown handler: Checking result and status of aio_pika consumer callback task {}', t.get_name(),
                )
                try:
                    task_result = t.result()
                except asyncio.CancelledError:
                    self._logger.info(
                        'Shutdown handler: aio_pika consumer callback task {} was cancelled', t.get_name(),
                    )
                except asyncio.InvalidStateError:
                    self._logger.info(
                        'Shutdown handler: aio_pika consumer callback task {} result not available yet. '
                        'Still running.',
                        t.get_name(),
                    )
                except Exception as e:
                    self._logger.info(
                        'Shutdown handler: aio_pika consumer callback task {} raised Exception. '
                        '{}',
                        t.get_name(), e,
                    )
                else:
                    self._logger.info(
                        'Shutdown handler: aio_pika consumer callback task returned with result {}',
                        t.get_name(),
                        task_result,
                    )
            # await asyncio.gather(*self._running_callback_tasks.values(), return_exceptions=True)

            tasks = [
                t for t in asyncio.all_tasks(loop) if t is not
                asyncio.current_task(loop)
            ]

            [task.cancel() for task in tasks]

            self._logger.info(f'Cancelling {len(tasks)} outstanding tasks')
            await asyncio.gather(*tasks, return_exceptions=True)
            loop.stop()
            self._logger.info('Shutdown complete.')

    async def _rabbitmq_consumer(self, loop):
        self._rmq_connection_pool = Pool(get_rabbitmq_connection, max_size=5, loop=loop)
        self._rmq_channel_pool = Pool(
            partial(get_rabbitmq_channel, self._rmq_connection_pool), max_size=20,
            loop=loop,
        )
        async with self._rmq_channel_pool.acquire() as channel:
            await channel.set_qos(20)
            q_obj = await channel.get_queue(
                name=self._q,
                ensure=False,
            )
            self._logger.debug(
                f'Consuming queue {self._q} with routing key {self._rmq_routing}...',
            )
            await q_obj.consume(self._on_rabbitmq_message)

    async def _on_rabbitmq_message(self, message: IncomingMessage):
        await message.ack()

    async def init_redis_pool(self):
        if not self._aioredis_pool:
            self._aioredis_pool = RedisPoolCache()
            await self._aioredis_pool.populate()
            self._redis_conn = self._aioredis_pool._aioredis_pool

    def run(self) -> None:
        setproctitle(self._unique_id)
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )
        # logging.config.dictConfig(config_logger_with_namespace(self.name))
        self._logger = logger.bind(module=self.name)

        ev_loop = asyncio.get_event_loop()
        signals = (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT)
        for s in signals:
            ev_loop.add_signal_handler(
                s, lambda x=s: ev_loop.create_task(self._shutdown_handler(x, ev_loop)),
            )
        self._logger.debug(f'Starting asynchronous epoch callback worker {self._unique_id}...')
        self._core_rmq_consumer = asyncio.ensure_future(self._rabbitmq_consumer(ev_loop))
        try:
            ev_loop.run_forever()
        finally:
            ev_loop.close()