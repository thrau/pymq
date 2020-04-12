import abc
import inspect
import logging
import uuid
from collections import defaultdict
from typing import Dict, Callable, List, Tuple, Union, Any

from pymq import json
from pymq.core import EventBus, Topic, RpcResponse, RpcRequest, StubMethod, Empty
from pymq.exceptions import *
from pymq.json import DeepDictDecoder
from pymq.typing import fullname, deep_from_dict, load_class

logger = logging.getLogger(__name__)


def invoke_function(fn, data: str):
    """
    Invokes the passed function with the given data. Expects the data to be a JSON object that contains the serialized
    parameters for the function.

    :param fn: the function to invoke (a callable)
    :param data: the json object containing the data
    :return:
    """
    # passes the event to the first parameter of the listener
    try:
        spec = inspect.getfullargspec(fn)
        args = spec.args

        if hasattr(fn, '__self__'):
            # fn is bound to an object
            event_arg = args[1]
        else:
            event_arg = args[0]

        # checks whether the parameter has a type hint, and if so attempts to convert the event to the type

        if event_arg in spec.annotations:
            t = spec.annotations[event_arg]
            logger.debug('instantiating new %s with event %s', t, data)
            # this is sort of an implicit shallow (no nested objects) de-serialization. events classes are expected
            # to have a constructor with kwargs that contain all the data.
            event = json.loads(data, cls=DeepDictDecoder.for_type(t))
        else:
            event = json.loads(data, cls=DeepDictDecoder)

        logger.debug('invoking %s with %s', fn, event)
        # event listeners are expected to have exactly one parameter: the event
        fn(event)
    except Exception as e:
        logger.exception(e)


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


def get_remote_name(fn):
    return fn.__module__ + '.' + fn.__qualname__


class WrapperTopic(Topic):
    _bus: EventBus
    _name: str
    _is_pattern: bool

    def __init__(self, bus: EventBus, name, is_pattern) -> None:
        super().__init__()
        self._bus = bus
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
            return self._bus.publish(event, self.name)

    def subscribe(self, callback):
        return self._bus.subscribe(callback, self.name, self.is_pattern)


class DefaultStubMethod(StubMethod):

    def __init__(self, bus: EventBus, channel: str, spec=None, timeout=None, multi=False) -> None:
        super().__init__()
        self._bus = bus
        self._channel = channel
        self._spec = spec

        self.timeout = timeout
        self.multi = multi

    def __call__(self, *args, **kwargs):
        try:
            response = self.rpc(*args, **kwargs)
        except NoSuchRemoteError:
            return [] if self.multi else None

        if self.multi:
            return [self._unmarshal(r, raise_error=False) for r in response]
        else:
            return self._unmarshal(response, raise_error=True)

    def rpc(self, *args, **kwargs) -> Union[RpcResponse, List[RpcResponse]]:
        request = RpcRequest(self._channel, self._next_callback_queue(), args, kwargs)
        return self._invoke(request)

    def _unmarshal(self, response: RpcResponse, raise_error=False):
        if response.error:
            if isinstance(response.result, Exception):
                result = RemoteInvocationError(response.result_type, *response.result.args)
            else:
                result = RemoteInvocationError(response.result_type, *response.result)

            if raise_error:
                raise result
            else:
                return result

        return deep_from_dict(response.result, load_class(response.result_type))

    def _next_callback_queue(self):
        return '__rpc_' + str(uuid.uuid4())

    def _get_response_queue(self, request: RpcRequest):
        return self._bus.queue(request.response_channel)

    def _invoke(self, request: RpcRequest) -> Union[RpcResponse, List[RpcResponse]]:
        # FIXME: the fundamental issue with this approach is that a pattern subscription '*' will break this. because
        #  such a subscription is probably just listening, and a real remote object, the expectation that there will
        #  be n results may not be correct
        fn = request.fn

        logger.debug('publishing to channel "%s" the request %s', fn, request)
        n = self._bus.publish(request, channel=fn)

        if n is None:
            raise RuntimeError('Implementation error in bus: publish returned None instead of subscriber count')
        if n == 0:
            raise NoSuchRemoteError(request.fn)

        queue = self._get_response_queue(request)
        try:
            results = list()

            for i in range(n):
                try:
                    logger.debug('waiting for response on queue %s, timeout %s,', queue.name, self.timeout)
                    # FIXME: calculate overall remaining timeout
                    response: RpcResponse = queue.get(timeout=self.timeout)
                    results.append(response)
                except Empty:
                    response = RpcResponse(fn, ('Gave up waiting after %s' % self.timeout,), 'TimeoutError', True)
                    results.append(response)

                if not self.multi:
                    return results[0]

            return results
        finally:
            self._finalize_response_queue(queue)

    def _finalize_response_queue(self, queue):
        """
        Hook to do something with the queue used as response channel once it's no longer needed.

        :param queue: the response queue that was created for this invocation
        """
        pass


class DefaultSkeletonMethod:
    _bus: EventBus

    _channel: str
    _fn: Callable
    _fn_spec: inspect.FullArgSpec

    def __init__(self, bus: EventBus, channel: str, fn: Callable) -> None:
        super().__init__()
        self._bus = bus
        self._channel = channel
        self._fn = fn
        self._fn_spec = inspect.getfullargspec(fn)

    def __call__(self, request: RpcRequest):
        try:
            result = self._invoke(request)
            response = RpcResponse(request.fn, result, fullname(result))
        except Exception as e:
            logger.exception('Exception while invoking %s', request)
            response = RpcResponse(request.fn, e, fullname(e), error=True)

        self._queue_response(request, response)

    def _queue_response(self, request, response):
        self._bus.queue(request.response_channel).put(response)

    def _invoke(self, request: RpcRequest) -> Any:
        spec = self._fn_spec

        if not spec.args:
            if request.args:
                raise TypeError('%s takes 0 positional arguments but %d were given' % (request.fn, len(request.args)))
        else:
            if spec.args[0] == 'self':
                spec.args.remove('self')

        args = list()

        logger.debug('converting args %s to spec %s', request.args, spec)

        for i in range(min(len(request.args), len(spec.args))):
            name = spec.args[i]
            value = request.args[i]

            if name in spec.annotations:
                arg_type = spec.annotations[name]
                value = deep_from_dict(value, arg_type)

            args.append(value)

        return self._fn(*args)


class AbstractEventBus(EventBus, abc.ABC):
    _subscribers: Dict[Tuple[str, bool], List[Callable]]
    _remote_fns: Dict[str, Callable]

    def __init__(self) -> None:
        super().__init__()
        self._subscribers = defaultdict(list)
        self._remote_fns = dict()

    def topic(self, name: str, pattern: bool = False):
        return WrapperTopic(self, name, pattern)

    def publish(self, event, channel=None):
        if channel is None:
            channel = fullname(event)

        return self._publish(event, channel)

    def subscribe(self, callback, channel=None, pattern=False):
        if channel is None:
            channel = inspect_listener(callback)
            pattern = False

        logger.debug('adding to channel "%s" a callback %s', channel, callback)

        self._subscribers[(channel, pattern)].append(callback)
        self._subscribe(callback, channel, pattern)

    def unsubscribe(self, callback, channel=None, pattern=False):
        if channel is None:
            channel = inspect_listener(callback)
            pattern = False

        callbacks = self._subscribers.get((channel, pattern))

        if callbacks:
            callbacks.remove(callback)
            if len(callbacks) == 0:
                del self._subscribers[(channel, pattern)]

        self._unsubscribe(callback, channel, pattern)

    def stub(self, fn, timeout=None, multi=False) -> StubMethod:
        if callable(fn):
            channel = get_remote_name(fn)
            spec = inspect.getfullargspec(fn)
        elif isinstance(fn, str):
            channel = str(fn)
            spec = None
        else:
            raise TypeError('cannot create stub for fn type %s' % type(fn))

        return self._create_stub_method(channel, spec, timeout, multi)

    def expose(self, fn, channel=None):
        if channel is None:
            channel = get_remote_name(fn)

        if channel in self._remote_fns:
            raise ValueError('Function on channel %s already exposed' % channel)

        logger.debug('exposing at channel "%s" the function %s', channel, fn)

        skeleton = self._create_skeleton_method(channel, fn)

        self._remote_fns[channel] = skeleton
        self.subscribe(skeleton, channel, False)

    def _create_stub_method(self, channel, spec, timeout, multi):
        return DefaultStubMethod(self, channel, spec, timeout, multi)

    def _create_skeleton_method(self, channel, fn) -> Callable[[RpcRequest], None]:
        return DefaultSkeletonMethod(self, channel, fn)

    def _publish(self, event, channel: str) -> int:
        raise NotImplementedError

    def _subscribe(self, callback: Callable, channel: str, pattern: bool):
        raise NotImplementedError

    def _unsubscribe(self, callback: Callable, channel: str, pattern: bool):
        raise NotImplementedError
