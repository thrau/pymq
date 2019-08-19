import abc
import inspect
import logging
import threading
import uuid
from queue import Empty
from typing import Dict, Callable, Union, List, Any, Optional, Tuple

from pymq.typing import deep_from_dict, load_class

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


class EventBus(abc.ABC):

    def run(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def publish(self, event, channel=None):
        raise NotImplementedError

    def subscribe(self, callback: Callable, channel, pattern=False):
        raise NotImplementedError

    def unsubscribe(self, callback: Callable, channel, pattern=False):
        raise NotImplementedError

    def queue(self, name: str) -> Queue:
        raise NotImplementedError

    def topic(self, name: str, pattern: bool = False):
        raise NotImplementedError


_uninitialized_subscribers: List[Tuple[Callable, str, bool]] = list()
_remote_fns: Dict[str, Callable] = dict()

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
        _remote_fns.clear()


def rpc_channel(fn) -> str:
    return fn.__module__ + '.' + fn.__qualname__


class RpcRequest:
    fn: str
    args: List[Any]
    callback_queue: str

    def __init__(self, fn: str, callback_queue: str, args: List[Any] = None) -> None:
        super().__init__()
        self.fn = fn
        self.args = args
        self.callback_queue = callback_queue

    def __str__(self) -> str:
        return 'RpcRequest(%s)' % self.__dict__

    def __repr__(self):
        return self.__str__()


class RpcResponse:
    fn: str
    result: Any
    result_type: str
    error: bool

    def __init__(self, fn, result: Any, result_type: str = None, error=False) -> None:
        super().__init__()
        self.fn = fn
        self.result = result
        self.result_type = result_type or str(result.__class__)[8:-2]
        self.error = error

    def __str__(self) -> str:
        return 'RpcResponse(%s)' % self.__dict__

    def __repr__(self):
        return self.__str__()


def _skeleton_invoke(request: RpcRequest):
    logger.debug('searching remote functions for %s in %s', request.fn, _remote_fns)

    if request.fn not in _remote_fns:
        raise ValueError('No such function %s' % request.fn)

    fn = _remote_fns[request.fn]

    spec = inspect.getfullargspec(fn)

    if not spec.args:
        if request.args:
            raise TypeError('%s takes 0 positional arguments but %d were given' % (request.fn, len(request.args)))
        return fn()

    args = list()

    if spec.args[0] == 'self':
        spec.args.remove('self')

    for i in range(min(len(request.args), len(spec.args))):
        name = spec.args[i]
        value = request.args[i]

        if name in spec.annotations:
            arg_type = spec.annotations[name]
            value = deep_from_dict(value, arg_type)

        args.append(value)

    return fn(*args)


def _rpc_wrapper(event: RpcRequest):
    logger.debug('calling rpc wrapper with even %s', event)

    try:
        result = RpcResponse(event.fn, _skeleton_invoke(event))
    except Exception as e:
        logger.exception('Exception while invoking %s', event)
        result = RpcResponse(event.fn, str(e), error=True)

    queue(event.callback_queue).put(result)


def rpc(fn: Union[str, Callable], *args, timeout=None) -> List[RpcResponse]:
    callback_queue = '__rpc_' + str(uuid.uuid4())

    if not isinstance(fn, str):
        fn = rpc_channel(fn)

    # FIXME: the fundamental issue with this approach is that a pattern subscription '*' will break this. because such a
    #  subscription is probably just listening, and a real remote object, the expectation that there will be n results
    #  may not be correct
    n = publish(RpcRequest(fn, callback_queue, list(args)), channel=fn)

    results = list()

    for i in range(n):
        try:
            response: RpcResponse = queue(callback_queue).get(timeout=timeout)

            if response.result is not None:
                response.result = deep_from_dict(response.result, load_class(response.result_type))

            results.append(response)
        except Empty:
            results.append(RpcResponse(fn, TimeoutError('Gave up waiting after %s' % timeout), error=True))

    return results


def expose(fn, channel=None):
    with _lock:
        if channel is None:
            channel = rpc_channel(fn)

        if channel in _remote_fns:
            raise ValueError('Function on channel %s already exposed' % channel)

        logger.debug('exposing to channel "%s" the callback %s', channel, fn)

        callback = _rpc_wrapper

        _remote_fns[channel] = fn

        subscribe(callback, channel, False)


def remote(*args, **kwargs):
    if callable(args[0]):
        expose(args[0], *args[1:], **kwargs)
        return args[0]

    def _decorator(fn):
        expose(fn, *args, **kwargs)
        return fn

    return _decorator
