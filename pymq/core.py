import abc
import logging
import threading
from queue import Empty, Full
from typing import Any, Callable, Generic, List, NamedTuple, Optional, Tuple, TypeVar, Union

logger = logging.getLogger(__name__)

Empty = Empty
Full = Full

QItem = TypeVar("QItem")


class Queue(abc.ABC, Generic[QItem]):
    """
    Base class for a queue.
    """

    @property
    def name(self) -> str:
        """
        :return: the name of the queue
        """
        raise NotImplementedError

    def get(self, block: bool = True, timeout: float = None) -> QItem:
        """
        Get an item from the queue.

        :param block: if True, block until an item is available
        :param timeout: timeout in seconds
        :return: the item
        :raises Empty: if the queue is empty and timeout is reached
        """
        raise NotImplementedError

    def put(self, item: QItem, block: bool = True, timeout: float = None):
        """
        Put an item into the queue.

        :param item: the item to put
        :param block: if True, block until space is available
        :param timeout: timeout in seconds
        :raises Full: if the queue is full and timeout is reached
        """
        raise NotImplementedError

    def qsize(self) -> int:
        """
        Returns the number of elements in the queue.

        :return: the approximate size of the queue
        """
        raise NotImplementedError

    def empty(self) -> bool:
        """
        Determine if the queue is empty.

        :return: A boolean value where `True` indicates the queue is empty and
            `False` indicates it is not.
        """
        return self.qsize() == 0

    def put_nowait(self, item: QItem):
        """
        Like a put but returns immediately. Depending on the provider and the queue parameters, this may
        raise an exception if the item cannot be put into the queue momentarily.

        :param item:  the item to put
        :raises Full: if the queue is full
        """
        return self.put(item, block=False)

    def get_nowait(self) -> QItem:
        """
        Like a get but returns immediately. Depending on the provider and the queue parameters, this may
        raise an exception if the item cannot be retrieved from the queue momentarily

        :return: the item retrieved from the queue
        :raises Empty: if the queue is empty
        """
        return self.get(block=False)

    def close(self):
        pass

    def free(self):
        """
        Frees the underlying resource needed for the Queue. This is relevant for some provides (like the POSIX ICP),
        where the queue needs to be unlinked.
        """
        pass


class Topic(abc.ABC):
    """
    Base class for a topic.
    """

    @property
    def name(self) -> str:
        """
        :return: the name of the topic
        """
        raise NotImplementedError

    @property
    def is_pattern(self) -> bool:
        """
        :return: True if the topic name is a pattern
        """
        raise NotImplementedError

    def publish(self, event) -> int:
        """
        Publish an event to the topic.

        :param event: the event to publish
        :return: the number of subscribers that received the event
        """
        raise NotImplementedError

    def subscribe(self, callback):
        """
        Subscribe a callback to the topic.

        :param callback: the callback to subscribe
        """
        raise NotImplementedError


class RpcRequest(NamedTuple):
    """
    Represents a request for a remote procedure call.
    """

    fn: str
    response_channel: str
    args: tuple = None
    kwargs: dict = None


class RpcResponse(NamedTuple):
    """
    Represents the response of a remote procedure call.
    """

    fn: str
    result: Any
    result_type: str = None
    error: bool = False


class StubMethod:
    """
    A callable stub for a remote method.
    """

    def __call__(self, *args, **kwargs) -> Any:
        """
        Invoke the remote method.

        :param args: positional arguments
        :param kwargs: keyword arguments
        :return: the result of the remote method call
        """
        raise NotImplementedError

    def rpc(self, *args, **kwargs) -> Union[RpcResponse, List[RpcResponse]]:
        """
        Invoke the remote method and return the full RPC response.

        :param args: positional arguments
        :param kwargs: keyword arguments
        :return: the RPC response or a list of RPC responses (if multi=True)
        """
        raise NotImplementedError


class EventBus(abc.ABC):
    """
    Base class for an event bus.
    """

    def run(self):
        """
        Start the event bus loop. This method blocks until the event bus is closed.
        """
        raise NotImplementedError

    def close(self):
        """
        Close the event bus and shut down the event bus loop.
        """
        raise NotImplementedError

    def publish(self, event: Any, channel: str = None) -> Optional[int]:
        """
        Publish an event to a channel.

        :param event: the event to publish
        :param channel: the channel to publish to (defaults to the event class name)
        :return: the number of subscribers that received the event
        """
        raise NotImplementedError

    def subscribe(self, callback: Callable, channel: str | None = None, pattern=False):
        """
        Subscribe a callback to a channel.

        :param callback: the callback to subscribe
        :param channel: the channel to subscribe to (defaults to the first argument type hint)
        :param pattern: if True, the channel name is treated as a pattern
        """
        raise NotImplementedError

    def unsubscribe(self, callback: Callable, channel=None, pattern=False):
        """
        Unsubscribe a callback from a channel.

        :param callback: the callback to unsubscribe
        :param channel: the channel to unsubscribe from
        :param pattern: if True, the channel name is treated as a pattern
        """
        raise NotImplementedError

    def queue(self, name: str) -> Queue:
        """
        Get a queue by name.

        :param name: the name of the queue
        :return: the queue instance
        """
        raise NotImplementedError

    def topic(self, name: str, pattern: bool = False) -> Topic:
        """
        Get a topic by name.

        :param name: the name of the topic
        :param pattern: if True, the name is treated as a pattern
        :return: the topic instance
        """
        raise NotImplementedError

    def stub(
        self, fn: Callable | str, timeout: float | None = None, multi: bool = False
    ) -> StubMethod:
        """
        Create a stub for a remote method.

        :param fn: the remote method (name or callable)
        :param timeout: timeout in seconds
        :param multi: if True, the stub will call all available providers
        :return: the stub method
        """
        raise NotImplementedError

    def expose(self, fn: Callable, channel: str = None):
        """
        Expose a local method for remote invocation.

        :param fn: the method to expose
        :param channel: the channel name to expose the method on
        """
        raise NotImplementedError

    def unexpose(self, fn: Callable):
        """
        Unexpose a previously exposed method.

        :param fn: the method to unexpose
        """
        raise NotImplementedError


_EB = TypeVar("_EB", bound=EventBus)
"""EventBus type variable."""

_uninitialized_subscribers: List[Tuple[Callable, str, bool]] = list()
"""Callbacks that were subscribed before the bus was initialized."""
_uninitialized_remote_fns: List[Tuple[Callable, str]] = list()
"""Methods that were exposed before the bus was initialized."""

_bus: Optional[EventBus] = None
"""The global event bus instance."""
_runner: Optional[threading.Thread] = None
"""The global event bus runner thread."""
_lock = threading.RLock()
"""Lock for thread-safe operations on the global event bus."""


class _WrapperTopic(Topic):
    """
    Wrapper for a topic on the global event bus.
    """

    _name: str
    _is_pattern: bool

    def __init__(self, name, is_pattern=False) -> None:
        super().__init__()
        self._name = name
        self._is_pattern = is_pattern

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_pattern(self) -> bool:
        return self._is_pattern

    def publish(self, event) -> int:
        if self.is_pattern:
            raise ValueError("Cannot publish to pattern topic")
        else:
            return publish(event, self.name)

    def subscribe(self, callback):
        return subscribe(callback, self.name, self.is_pattern)


def subscribe(callback, channel=None, pattern=False):
    """
    Subscribe a callback to a channel on the global event bus.

    :param callback: the callback to subscribe
    :param channel: the channel to subscribe to (defaults to the first argument type hint)
    :param pattern: if True, the channel name is treated as a pattern
    """
    with _lock:
        if _bus:
            _bus.subscribe(callback, channel, pattern)
        else:
            _uninitialized_subscribers.append((callback, channel, pattern))


def unsubscribe(callback, channel=None, pattern=False):
    """
    Unsubscribe a callback from a channel on the global event bus.

    :param callback: the callback to unsubscribe
    :param channel: the channel to unsubscribe from
    :param pattern: if True, the channel name is treated as a pattern
    """
    with _lock:
        if _bus:
            _bus.unsubscribe(callback, channel, pattern)
        else:
            _uninitialized_subscribers.remove((callback, channel, pattern))


def subscriber(*args, **kwargs):
    """
    Decorator for subscribing a function to a channel on the global event bus.

    :param args: positional arguments for the subscription
    :param kwargs: keyword arguments for the subscription
    :return: the decorated function
    """
    if args and callable(args[0]):
        subscribe(args[0], *args[1:], **kwargs)
        return args[0]

    def _decorator(fn):
        subscribe(fn, *args, **kwargs)
        return fn

    return _decorator


def init(factory: Callable[[], _EB], start_bus: bool = True) -> _EB:
    """
    Initialize the global event bus.

    :param factory: a callable that returns an EventBus instance (e.g., a RedisConfig instance)
    :param start_bus: if True, start the event bus loop in a background thread
    :return: the initialized event bus instance
    """
    with _lock:
        global _bus
        _bus = factory()
        if start_bus:
            start()

        for callback, channel, pattern in _uninitialized_subscribers:
            _bus.subscribe(callback, channel, pattern)

        _uninitialized_subscribers.clear()

        for callback, channel in _uninitialized_remote_fns:
            _bus.expose(callback, channel)

        _uninitialized_remote_fns.clear()

        return _bus


def publish(event: Any, channel: str | None = None):
    """
    Publish an event to a channel on the global event bus.

    :param event: the event to publish
    :param channel: the channel to publish to (defaults to the event class name)
    :return: the number of subscribers that received the event
    """
    if _bus is None:
        logger.error("Event bus was not initialized, cannot publish message. Please run pymq.init")
        return

    return _bus.publish(event, channel)


def queue(name: str) -> Queue:
    """
    Get a queue by name from the global event bus.

    :param name: the name of the queue
    :return: the queue instance
    """
    if _bus is None:
        logger.error("Event bus was not initialized, cannot get queue. Please run pymq.init")
        raise ValueError("Bus not set yet")

    return _bus.queue(name)


def topic(name: str, pattern: bool = False) -> Topic:
    """
    Get a topic by name from the global event bus.

    :param name: the name of the topic
    :param pattern: if True, the name is treated as a pattern
    :return: the topic instance
    """
    if _bus is None:
        return _WrapperTopic(name, pattern)

    return _bus.topic(name, pattern)


def stub(fn: Callable, timeout=None, multi=False) -> StubMethod:
    """
    Create a stub for a remote method on the global event bus.

    :param fn: the remote method (name or callable)
    :param timeout: timeout in seconds
    :param multi: if True, the stub will call all available providers
    :return: the stub method
    """
    if _bus is None:
        logger.error("Event bus was not initialized, cannot get stub. Please run pymq.init")
        raise ValueError("Bus not set yet")

    return _bus.stub(fn, timeout, multi)


def expose(fn: Callable, channel: str | None = None):
    """
    Expose a local method for remote invocation on the global event bus.

    :param fn: the method to expose
    :param channel: the channel name to expose the method on
    """
    with _lock:
        if _bus:
            _bus.expose(fn, channel)
        else:
            _uninitialized_remote_fns.append((fn, channel))


def unexpose(fn: Callable):
    """
    Unexpose a previously exposed method on the global event bus.

    :param fn: the method to unexpose
    """
    if _bus is None:
        # FIXME: will not remote uninitialized skeletons
        logger.error("Event bus was not initialized, cannot unexpose method. Please run pymq.init")
        raise ValueError("Bus not set yet")

    return _bus.unexpose(fn)


def start():
    """
    Start the global event bus loop in a background thread.

    :raises ValueError: if the bus is not initialized (call ``pymq.init`` first)
    """
    with _lock:
        if _bus is None:
            raise ValueError("Bus not set yet")

        logger.debug("starting global event bus")
        global _runner
        if _runner is None:
            _runner = threading.Thread(target=_bus.run, name="eventbus-runner")
            _runner.daemon = True
            _runner.start()


def shutdown():
    """
    Shutdown the global event bus and its background thread.
    """
    global _runner, _bus
    with _lock:
        if _runner is None:
            return
        logger.debug("stopping global event bus")
        if _bus is not None:
            _bus.close()
        _runner.join()
        logger.debug("global event bus stopped")
        _runner = None
        _bus = None
        _uninitialized_subscribers.clear()
        _uninitialized_remote_fns.clear()


def remote(*args, **kwargs):
    """
    Decorator for exposing a function for remote invocation on the global event bus.

    Example::

        @pymq.remote
        def my_remote_function():
            pass

        @pymq.remote("product_remote")
        def product(a: int, b: int) -> int: # pymq relies on type hints for marshalling
            return a * b

    :param args: positional arguments for the exposition
    :param kwargs: keyword arguments for the exposition
    :return: the decorated function
    """
    if callable(args[0]):
        expose(args[0], *args[1:], **kwargs)
        return args[0]

    def _decorator(fn):
        expose(fn, *args, **kwargs)
        return fn

    return _decorator
