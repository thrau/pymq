import json
import logging
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Callable

import redis

from pymq import RpcRequest
from pymq.core import Empty, Queue
from pymq.json import DeepDictDecoder, DeepDictEncoder
from pymq.provider.base import AbstractEventBus, DefaultSkeletonMethod, invoke_function

logger = logging.getLogger(__name__)


class RedisConfig:
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

        self.rds = args[0] if len(args) > 0 and isinstance(args[0], redis.Redis) else None

        self.args = args
        self.kwargs = kwargs

    def get_redis(self):
        return self.rds or redis.Redis(*self.args, **self.kwargs, decode_responses=True)

    def __call__(self):
        return RedisEventBus(rds=self.get_redis())


class RedisQueue(Queue):
    """
    Queue implementation over Redis. Uses very naive serialization using pickle and Base64.
    """

    def __init__(self, rds: redis.Redis, name: str, key: str = None) -> None:
        super().__init__()
        self._rds = rds
        self._name = name
        self._key = key or name

    @property
    def name(self):
        return self._name

    def get(self, block=True, timeout=None):
        if block:
            response = self._rds.brpop(self._key, timeout)
            if response is None:
                raise Empty
            response = response[1]
        else:
            response = self._rds.rpop(self._key)

        if response is None:
            raise Empty

        return self._deserialize(response)

    def put(self, item, block=False, timeout=None):
        if block:
            raise NotImplementedError()

        self._rds.lpush(self._key, self._serialize(item))

    def qsize(self):
        return self._rds.llen(self._key)

    def _serialize(self, item):
        return json.dumps(item, cls=DeepDictEncoder)

    def _deserialize(self, item):
        return json.loads(item, cls=DeepDictDecoder)


class RedisSkeletonMethod(DefaultSkeletonMethod):
    """
    This special skeleton makes sure the result channels expire after a given time as to not create garbage.
    """

    # noinspection PyUnresolvedReferences
    def _queue_response(self, request, response):
        super()._queue_response(request, response)
        self._bus.rds.expire(
            self._bus.channel_prefix + request.response_channel, self._bus.rpc_channel_expire
        )


class RedisEventBus(AbstractEventBus):
    rpc_channel_expire = 300  # 5 minute default

    def __init__(self, namespace="global", dispatcher=None, rds: redis.Redis = None) -> None:
        super().__init__()
        self.namespace = namespace
        self.dispatcher: ThreadPoolExecutor = dispatcher
        self.rds: redis.Redis = rds

        self._pubsub: redis.client.PubSub = None
        self._lock = threading.Condition()
        self._closed = False

        self.channel_prefix = "__eventbus:" + self.namespace + ":"

    def _listen(self):
        while True:
            if not self._pubsub:
                logger.error("invalid state, pubsub object is not set")
                return

            logger.debug("waiting for subscriptions to appear")
            with self._lock:
                self._lock.wait_for(lambda: self._pubsub.subscribed or self._closed)

            if self._closed:
                logger.debug("eventbus closed, listening stops")
                return

            logger.debug("subscriptions available starting to listen on pubsub object")

            yield from self._pubsub.listen()
            logger.debug("pubsub listen returned, waiting on next iteration")

    def run(self):
        with self._lock:
            if self.dispatcher is None:
                self.dispatcher = ThreadPoolExecutor(1)

            if self.rds is None:
                self.rds = redis.Redis(decode_responses=True)

            self._pubsub = self.rds.pubsub()

            self._init_subscriptions()
            self._lock.notify()

        try:
            logger.debug("starting to listen on pubsub...")
            for message in self._listen():
                logger.debug("got message %s", message)

                if not (message["type"] == "message" or message["type"] == "pmessage"):
                    continue

                if message["pattern"] is None:
                    key = (message["channel"], False)
                else:
                    key = (message["pattern"], True)

                key = key[0][len(self.channel_prefix) :], key[1]

                if key not in self._subscribers:
                    logger.warning("inconsistent state: no listeners for %s", key)
                    continue

                for fn in self._subscribers[key]:
                    logger.debug("dispatching %s to %s", message, fn)

                    self.dispatcher.submit(RedisEventBus._call_listener, fn, message["data"])

        except Exception as listen_error:
            logger.error(listen_error)

        finally:
            logger.debug("acquiring close lock")
            with self._lock:
                logger.debug("closing pubsub")
                self._pubsub.close()

        logger.debug("exitting eventbus listen loop")

    def subscribe(self, callback, channel=None, pattern=False):
        with self._lock:
            super().subscribe(callback, channel, pattern)
            self._lock.notify()

    def unsubscribe(self, callback, channel=None, pattern=False):
        with self._lock:
            super().unsubscribe(callback, channel, pattern)
            self._lock.notify()

    def close(self):
        with self._lock:
            if self._closed or not self._pubsub:
                return

            self._closed = True

            logger.debug("unsubscribing from all channels")
            self._pubsub.punsubscribe()
            self._pubsub.unsubscribe()

            self._lock.notify()

        logger.debug("shutting down dispatcher")
        self.dispatcher.shutdown()
        logger.debug("shutdown complete")

    def queue(self, name: str) -> Queue:
        return RedisQueue(self.rds, name, self.channel_prefix + name)

    def _publish(self, event, channel: str):
        data = json.dumps(event, cls=DeepDictEncoder)

        redis_channel = self.channel_prefix + channel

        logger.debug('publishing into "%s" data %s', redis_channel, data)
        return self.rds.publish(redis_channel, data)

    def _subscribe(self, _: Callable, channel: str, pattern: bool):
        if self._pubsub is None or self._closed:
            return

        redis_channel = self.channel_prefix + channel

        if pattern:
            if redis_channel not in self._pubsub.patterns:
                self._pubsub.psubscribe(redis_channel)
        else:
            if redis_channel not in self._pubsub.channels:
                self._pubsub.subscribe(redis_channel)

    def _unsubscribe(self, _: Callable, channel: str, pattern: bool):
        if self._pubsub is None:
            return

        if (channel, pattern) in self._subscribers:
            return

        logger.debug('no callbacks left in "%s, (pattern? %s)", unsubscribing', channel, pattern)

        redis_channel = self.channel_prefix + channel

        if pattern:
            if redis_channel in self._pubsub.patterns:
                self._pubsub.punsubscribe(redis_channel)
        else:
            if redis_channel in self._pubsub.channels:
                self._pubsub.unsubscribe(redis_channel)

    def _init_subscriptions(self):
        logger.debug("initializing subscriptions %s", self._subscribers)
        channels = [
            self.channel_prefix + channel
            for channel, pattern in self._subscribers.keys()
            if not pattern
        ]
        patterns = [
            self.channel_prefix + channel
            for channel, pattern in self._subscribers.keys()
            if pattern
        ]

        if channels:
            logger.debug("initializing channel subscriptions %s", channels)
            self._pubsub.subscribe(*channels)
        if patterns:
            logger.debug("initializing pattern subscriptions %s", patterns)
            self._pubsub.psubscribe(*patterns)

    @staticmethod
    def _call_listener(fn, data):
        invoke_function(fn, data)

    def _create_skeleton_method(self, channel, fn) -> Callable[[RpcRequest], None]:
        return RedisSkeletonMethod(self, channel, fn)
