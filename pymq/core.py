import abc
import logging
import threading
from queue import Empty
from typing import Callable, Union, List, Any, Optional, Tuple, NamedTuple

logger = logging.getLogger(__name__)

Empty = Empty


class Queue(abc.ABC):

    @property
    def name(self):
        raise NotImplementedError

    def get(self, block=True, timeout=None):
        raise NotImplementedError

    def put(self, item, block=True, timeout=None):
        raise NotImplementedError

    def qsize(self):
        raise NotImplementedError

    def empty(self):
        return self.qsize() == 0

    def put_nowait(self, item):
        return self.put(item, block=False)

    def get_nowait(self):
        return self.get(block=False)


class Topic(abc.ABC):

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def is_pattern(self) -> bool:
        raise NotImplementedError

    def publish(self, event) -> int:
        raise NotImplementedError

    def subscribe(self, callback):
        raise NotImplementedError


class RpcRequest(NamedTuple):
    fn: str
    response_channel: str
    args: tuple = None
    kwargs: dict = None


class RpcResponse(NamedTuple):
    fn: str
    result: Any
    result_type: str = None
    error: bool = False


class StubMethod:

    def __call__(self, *args, **kwargs):
        raise NotImplementedError

    def rpc(self, *args, **kwargs) -> Union[RpcResponse, List[RpcResponse]]:
        raise NotImplementedError


class EventBus(abc.ABC):

    def run(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def publish(self, event, channel=None):
        raise NotImplementedError

    def subscribe(self, callback: Callable, channel=None, pattern=False):
        raise NotImplementedError

    def unsubscribe(self, callback: Callable, channel=None, pattern=False):
        raise NotImplementedError

    def queue(self, name: str) -> Queue:
        raise NotImplementedError

    def topic(self, name: str, pattern: bool = False):
        raise NotImplementedError

    def stub(self, fn, timeout=None, multi=False) -> StubMethod:
        raise NotImplementedError

    def expose(self, fn, channel=None):
        raise NotImplementedError


_uninitialized_subscribers: List[Tuple[Callable, str, bool]] = list()
_uninitialized_remote_fns: List[Tuple[Callable, str]] = list()

_bus: Optional[EventBus] = None
_runner: Optional[threading.Thread] = None
_lock = threading.RLock()


class _WrapperTopic(Topic):
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
            raise ValueError('Cannot publish to pattern topic')
        else:
            return publish(event, self.name)

    def subscribe(self, callback):
        return subscribe(callback, self.name, self.is_pattern)


def subscribe(callback, channel=None, pattern=False):
    with _lock:
        if _bus:
            _bus.subscribe(callback, channel, pattern)
        else:
            _uninitialized_subscribers.append((callback, channel, pattern))


def unsubscribe(callback, channel=None, pattern=False):
    with _lock:
        if _bus:
            _bus.unsubscribe(callback, channel, pattern)
        else:
            _uninitialized_subscribers.remove((callback, channel, pattern))


def subscriber(*args, **kwargs):
    if callable(args[0]):
        subscribe(args[0], *args[1:], **kwargs)
        return args[0]

    def _decorator(fn):
        subscribe(fn, *args, **kwargs)
        return fn

    return _decorator


def init(factory, start_bus=True):
    with _lock:
        global _bus
        _bus = factory()
        if start_bus:
            start()

        for (callback, channel, pattern) in _uninitialized_subscribers:
            _bus.subscribe(callback, channel, pattern)

        _uninitialized_subscribers.clear()

        for (callback, channel) in _uninitialized_remote_fns:
            _bus.expose(callback, channel)

        _uninitialized_remote_fns.clear()

        return _bus


def publish(event, channel=None):
    if _bus is None:
        logger.error('Event bus was not initialized, cannot publish message. Please run pymq.init')
        return

    return _bus.publish(event, channel)


def queue(name) -> Queue:
    if _bus is None:
        logger.error('Event bus was not initialized, cannot get queue. Please run pymq.init')
        raise ValueError('Bus not set yet')

    return _bus.queue(name)


def topic(name, pattern=False):
    if _bus is None:
        return _WrapperTopic(name, pattern)

    return _bus.topic(name, pattern)


def stub(fn, timeout=None, multi=False) -> StubMethod:
    if _bus is None:
        logger.error('Event bus was not initialized, cannot get stub. Please run pymq.init')
        raise ValueError('Bus not set yet')

    return _bus.stub(fn, timeout, multi)


def expose(fn, channel=None):
    with _lock:
        if _bus:
            _bus.expose(fn, channel)
        else:
            _uninitialized_remote_fns.append((fn, channel))


def start():
    with _lock:
        if _bus is None:
            raise ValueError('Bus not set yet')

        logger.debug('starting global event bus')
        global _runner
        if _runner is None:
            _runner = threading.Thread(target=_bus.run, name='eventbus-runner')
            _runner.daemon = True
            _runner.start()


def shutdown():
    global _runner, _bus
    with _lock:
        if _runner is None:
            return
        logger.debug('stopping global event bus')
        _bus.close()
        _runner.join()
        logger.debug('global event bus stopped')
        _runner = None
        _bus = None
        _uninitialized_subscribers.clear()
        _uninitialized_remote_fns.clear()


def remote(*args, **kwargs):
    if callable(args[0]):
        expose(args[0], *args[1:], **kwargs)
        return args[0]

    def _decorator(fn):
        expose(fn, *args, **kwargs)
        return fn

    return _decorator
