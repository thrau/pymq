import abc
import inspect
import logging
import threading
import uuid
from collections import defaultdict
from queue import Empty
from typing import Dict, Callable, Union, List, Any, Optional

from pymq.typing import fullname, deep_from_dict, load_class

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


class EventBus(abc.ABC):

    def publish(self, event, channel):
        raise NotImplementedError

    def add_listener(self, callback, channel, pattern):
        raise NotImplementedError

    def remove_listener(self, callback, channel, pattern):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def queue(self, name: str) -> Queue:
        raise NotImplementedError

    @property
    def listeners(self):
        return _listeners


_listeners: dict = defaultdict(list)
_remote_fns: Dict[str, Callable] = dict()

_bus: Optional[EventBus] = None
_runner: Optional[threading.Thread] = None
_lock = threading.RLock()


def inspect_listener(fn) -> str:
    spec = inspect.getfullargspec(fn)

    if hasattr(fn, '__self__'):
        # method is bound to an object
        if len(spec.args) != 2:
            raise ValueError('Listener functions need exactly one arguments')

        if spec.args[1] not in spec.annotations:
            raise ValueError('Please annotate the event class with an appropriate type')

        event_type = spec.annotations[spec.args[1]]
        return fullname(event_type)

    else:
        if len(spec.args) != 1:
            raise ValueError('Listener functions need exactly one arguments')

        if spec.args[0] not in spec.annotations:
            raise ValueError('Please annotate the event class with an appropriate type')

        event_type = spec.annotations[spec.args[0]]
        return fullname(event_type)


def add_listener(callback, channel=None, pattern=False):
    with _lock:
        if channel is None:
            channel = inspect_listener(callback)
            pattern = False

        logger.debug('adding to channel "%s" a callback %s', channel, callback)

        _listeners[(channel, pattern)].append(callback)

        if _bus is not None:
            _bus.add_listener(callback, channel, pattern)


def remove_listener(callback, channel=None, pattern=False):
    with _lock:
        if channel is None:
            channel = inspect_listener(callback)
            pattern = False

        callbacks = _listeners.get((channel, pattern))
        if callback:
            callbacks.remove(callback)

        if _bus is not None:
            _bus.remove_listener(callback, channel, pattern)


def listener(*args, **kwargs):
    if callable(args[0]):
        add_listener(args[0], *args[1:], **kwargs)
        return args[0]

    def _decorator(fn):
        add_listener(fn, *args, **kwargs)
        return fn

    return _decorator


def init(factory, start_bus=True):
    with _lock:
        global _bus
        _bus = factory()
        if start_bus:
            start()
        return _bus


def publish(event, channel=None):
    if _bus is None:
        logger.error('Event bus was not initialized, cannot publish message. Please run pymq.init')
        return

    if channel is None:
        channel = fullname(event)

    return _bus.publish(event, channel)


def queue(name) -> Queue:
    if _bus is None:
        logger.error('Event bus was not initialized, cannot get queue. Please run pymq.init')
        raise ValueError('Bus not set yet')

    return _bus.queue(name)


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
        _listeners.clear()
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
        pattern = False  # can't have patterns for RPC calls

        if channel is None:
            channel = rpc_channel(fn)

        if channel in _remote_fns:
            raise ValueError('Function on channel %s already exposed' % channel)

        logger.debug('exposing to channel "%s" the callback %s', channel, fn)

        callback = _rpc_wrapper

        _listeners[(channel, pattern)].append(callback)
        _remote_fns[channel] = fn

        logger.debug('bus is %s', _bus)

        if _bus is not None:
            _bus.add_listener(callback, channel, pattern)


def remote(*args, **kwargs):
    if callable(args[0]):
        expose(args[0], *args[1:], **kwargs)
        return args[0]

    def _decorator(fn):
        expose(fn, *args, **kwargs)
        return fn

    return _decorator
