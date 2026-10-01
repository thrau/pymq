"""
Event bus provider backed by Redis Pub/Sub and Redis lists.

See :class:`RedisConfig` for configuration and :class:`RedisEventBus` for the event bus
implementation.
"""

import json
import logging
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, Callable, Iterator

import redis

from pymq.core import Empty, Queue, RpcRequest, RpcResponse
from pymq.json import DeepDictDecoder, DeepDictEncoder
from pymq.provider.base import AbstractEventBus, DefaultSkeletonMethod, invoke_function

logger = logging.getLogger(__name__)


class RedisConfig:
    """
    Configuration class for Redis-based EventBus and Queue providers.
    It can be initialized with the same arguments as ``redis.Redis`` or with an existing ``redis.Redis`` instance.
    Example::

        import pymq
        from pymq.provider.redis import RedisConfig

        pymq.init(RedisConfig(host="localhost", port=6379))
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Create a Redis configuration.

        If the first positional argument is a :class:`redis.Redis` instance it is used directly,
        otherwise the arguments are stored and forwarded to :class:`redis.Redis` on first use.
        """
        super().__init__()

        self.rds = args[0] if len(args) > 0 and isinstance(args[0], redis.Redis) else None

        self.args = args
        self.kwargs = kwargs

    def get_redis(self) -> redis.Redis:
        """
        Get the connected Redis client, creating one from the stored arguments if necessary.

        :return: the Redis client
        """
        return self.rds or redis.Redis(*self.args, **self.kwargs, decode_responses=True)

    def __call__(self) -> "RedisEventBus":
        """
        Create a Redis event bus from this configuration.

        :return: a new Redis event bus
        """
        return RedisEventBus(rds=self.get_redis())


class RedisQueue(Queue):
    """
    Queue implementation over Redis using the LIST data structure. Uses json for serialization.

    Implementation details:
      - Uses Redis LIST type for FIFO queue operations
      - `lpush` adds items to the head (left) of the list
      - `rpop`/`brpop` removes items from the tail (right) of the list
      - This achieves FIFO behavior: first item pushed is first item popped
      - `brpop` provides blocking behavior with optional timeout
      - `llen` returns queue size

    Redis keys are constructed using the pattern: `__eventbus:<namespace>:<name>`.

    Example:
        For a queue named "my_queue" in the default "global" namespace, the Redis key will be
        `__eventbus:global:my_queue`.
    """

    def __init__(self, rds: redis.Redis, name: str, key: str | None = None) -> None:
        """
        Create a Redis-backed queue.

        :param rds: the Redis client
        :param name: the logical name of the queue
        :param key: the Redis key to use, defaults to ``name``
        """
        super().__init__()
        self._rds = rds
        self._name = name
        self._key = key or name

    @property
    def name(self) -> str:
        """
        :return: the name of the queue
        """
        return self._name

    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        """
        Pop an item from the queue.

        :param block: if True, block until an item is available
        :param timeout: timeout in seconds when blocking
        :return: the item
        :raises Empty: if the queue is empty
        """
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

    def put(self, item: Any, block: bool = False, timeout: float | None = None) -> None:
        """
        Push an item onto the queue.

        :param item: the item to push
        :param block: not supported by this provider
        :param timeout: not supported by this provider
        """
        if block:
            raise NotImplementedError()

        self._rds.lpush(self._key, self._serialize(item))

    def qsize(self) -> int:
        """
        :return: the number of items in the queue
        """
        return self._rds.llen(self._key)

    def _serialize(self, item: Any) -> str:
        """
        Serialize an item to JSON using :class:`DeepDictEncoder`.
        """
        return json.dumps(item, cls=DeepDictEncoder)

    def _deserialize(self, item: str) -> Any:
        """
        Deserialize a JSON item using :class:`DeepDictDecoder`.
        """
        return json.loads(item, cls=DeepDictDecoder)


class RedisSkeletonMethod(DefaultSkeletonMethod):
    """
    A specialized RPC skeleton for Redis that ensures response channels have a TTL (Time To Live).
    This prevents temporary RPC response queues from cluttering Redis memory.
    """

    # noinspection PyUnresolvedReferences
    def _queue_response(self, request: RpcRequest, response: RpcResponse) -> None:
        """
        Send the response and set a TTL on the response queue so it is eventually cleaned up.

        :param request: the original request
        :param response: the response to send
        """
        super()._queue_response(request, response)
        self._bus.rds.expire(
            self._bus.channel_prefix + request.response_channel, self._bus.rpc_channel_expire
        )


class RedisEventBus(AbstractEventBus):
    """
    EventBus implementation using Redis Pub/Sub for event distribution and RPC.

    The EventBus uses a configurable namespace to isolate channels. All Redis keys and
    Pub/Sub channels are prefixed with `__eventbus:<namespace>:`.

    Key construction examples:
        - Pub/Sub channel for "my_event": `__eventbus:global:my_event`
        - RPC response channel: `__eventbus:global:rpc-res-<uuid>`

    Default `rpc_channel_expire` is 300 seconds (5 minutes).
    """

    rpc_channel_expire = 300  # 5 minute default

    def __init__(
        self,
        namespace: str = "global",
        dispatcher: ThreadPoolExecutor | None = None,
        rds: redis.Redis | None = None,
    ) -> None:
        """
        Create a Redis event bus.

        :param namespace: the namespace used to prefix all Redis keys and channels
        :param dispatcher: optional thread pool used to dispatch events
        :param rds: optional existing Redis client to use
        """
        super().__init__()
        self.namespace = namespace
        self.dispatcher: ThreadPoolExecutor | None = dispatcher
        self.rds: redis.Redis | None = rds

        self._pubsub: redis.client.PubSub | None = None
        self._lock = threading.Condition()
        self._closed = False

        self.channel_prefix = "__eventbus:" + self.namespace + ":"

    def _listen(self) -> Iterator[Any]:
        """
        Yield messages from the underlying pub/sub connection, waiting until at least one
        subscription is active before listening.
        """
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

    def run(self) -> None:
        """
        Runs the core logic for managing Redis pub/sub communication and dispatching
        messages to registered subscribers. This method handles initializing Redis
        connections, subscribing to channels, listening for messages, and processing
        incoming messages.

        It uses a ThreadPoolExecutor to manage the submission of tasks for subscriber
        callbacks, ensuring non-blocking behavior. The method safeguards critical sections
        with a threading lock to ensure thread safety while modifying shared resources.

        Error handling is implemented to log exceptions during message listening, and
        resources are cleaned up properly during shutdown.
        """
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

    def subscribe(
        self, callback: Callable, channel: str | None = None, pattern: bool = False
    ) -> None:
        """
        Subscribe a callback to a channel and wake up the listen loop.
        """
        with self._lock:
            super().subscribe(callback, channel, pattern)
            self._lock.notify()

    def unsubscribe(
        self, callback: Callable, channel: str | None = None, pattern: bool = False
    ) -> None:
        """
        Unsubscribe a callback from a channel and wake up the listen loop.
        """
        with self._lock:
            super().unsubscribe(callback, channel, pattern)
            self._lock.notify()

    def close(self) -> None:
        """
        Close the event bus, unsubscribing from all channels and shutting down the dispatcher.
        """
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
        """
        Get a Redis-backed queue by name.

        :param name: the name of the queue
        :return: the queue instance
        """
        return RedisQueue(self.rds, name, self.channel_prefix + name)

    def _publish(self, event: Any, channel: str) -> int:
        """
        Serialize and publish an event to a Redis pub/sub channel.

        :param event: the event to publish
        :param channel: the channel to publish on
        :return: the number of subscribers that received the event
        """
        data = json.dumps(event, cls=DeepDictEncoder)

        redis_channel = self.channel_prefix + channel

        logger.debug('publishing into "%s" data %s', redis_channel, data)
        return self.rds.publish(redis_channel, data)

    def _subscribe(self, _: Callable, channel: str, pattern: bool) -> None:
        """
        Subscribe the underlying pub/sub connection to the given channel.
        """
        if self._pubsub is None or self._closed:
            return

        redis_channel = self.channel_prefix + channel

        if pattern:
            if redis_channel not in self._pubsub.patterns:
                self._pubsub.psubscribe(redis_channel)
        else:
            if redis_channel not in self._pubsub.channels:
                self._pubsub.subscribe(redis_channel)

    def _unsubscribe(self, _: Callable, channel: str, pattern: bool) -> None:
        """
        Unsubscribe the underlying pub/sub connection from the given channel once no callbacks remain.
        """
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

    def _init_subscriptions(self) -> None:
        """
        Subscribe the pub/sub connection to all currently registered channels and patterns.
        """
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
    def _call_listener(fn: Callable, data: str) -> None:
        """
        Dispatch a serialized event to a listener function.
        """
        invoke_function(fn, data)

    def _create_skeleton_method(self, channel: str, fn: Callable) -> Callable[[RpcRequest], None]:
        """
        :return: a Redis-specific skeleton method that expires its response queues
        """
        return RedisSkeletonMethod(self, channel, fn)
