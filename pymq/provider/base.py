import abc
import inspect
import logging
import uuid
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from pymq import json
from pymq.core import Empty, EventBus, RpcRequest, RpcResponse, StubMethod, Topic
from pymq.exceptions import *
from pymq.json import DeepDictDecoder
from pymq.typing import deep_from_dict, fullname, load_class

logger = logging.getLogger(__name__)


def invoke_function(fn: Callable, data: str | bytes) -> None:
    """
    Invokes the passed function with the given data. Expects the data to be a JSON object that contains the serialized
    parameters for the function.

    The function uses type hints of the target function to perform deep de-serialization of the input data.

    :param fn: the function to invoke (a callable)
    :param data: the json object containing the data
    """
    # passes the event to the first parameter of the listener
    try:
        spec = inspect.getfullargspec(fn)
        args = spec.args

        if hasattr(fn, "__self__"):
            # fn is bound to an object
            event_arg = args[1]
        else:
            event_arg = args[0]

        # checks whether the parameter has a type hint, and if so attempts to convert the event to the type

        if event_arg in spec.annotations:
            t = spec.annotations[event_arg]
            logger.debug("instantiating new %s with event %s", t, data)
            # this is sort of an implicit shallow (no nested objects) de-serialization. events classes are expected
            # to have a constructor with kwargs that contain all the data.
            event = json.loads(data, cls=DeepDictDecoder.for_type(t))
        else:
            event = json.loads(data, cls=DeepDictDecoder)

        logger.debug("invoking %s with %s", fn, event)
        # event listeners are expected to have exactly one parameter: the event
        fn(event)
    except Exception as e:
        logger.exception(e)


def inspect_listener(fn) -> str:
    """
    Inspects a listener function to determine the event type it is interested in.
    The function must have exactly one argument (excluding 'self' for methods) and that argument must be type-hinted.

    :param fn: the listener function
    :return: the fully qualified name of the event type
    :raises ValueError: if the function signature does not match requirements
    """
    spec = inspect.getfullargspec(fn)

    if hasattr(fn, "__self__"):
        # method is bound to an object
        if len(spec.args) != 2:
            raise ValueError("Listener functions need exactly one arguments")

        if spec.args[1] not in spec.annotations:
            raise ValueError("Please annotate the event class with an appropriate type")

        event_type = spec.annotations[spec.args[1]]
        return fullname(event_type)

    else:
        if len(spec.args) != 1:
            raise ValueError("Listener functions need exactly one arguments")

        if spec.args[0] not in spec.annotations:
            raise ValueError("Please annotate the event class with an appropriate type")

        event_type = spec.annotations[spec.args[0]]
        return fullname(event_type)


def get_remote_name(fn: Callable) -> str:
    """
    Returns a unique remote name for a function, typically its module and qualified name.
    """
    return fn.__module__ + "." + fn.__qualname__


class WrapperTopic(Topic):
    """
    A Topic implementation that wraps an EventBus instance.
    It delegates publish and subscribe operations back to the bus using its name.
    """

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

    def publish(self, event: Any) -> int:
        if self.is_pattern:
            raise ValueError("Cannot publish to pattern topic")
        else:
            return self._bus.publish(event, self.name)

    def subscribe(self, callback: Callable) -> None:
        return self._bus.subscribe(callback, self.name, self.is_pattern)


class DefaultStubMethod(StubMethod):
    """
    Default implementation of an RPC stub (client-side).

    This class generalizes RPC over pub/sub and queues. It performs an RPC call by:
    1. Creating a unique temporary response queue.
    2. Publishing an ``RpcRequest`` to the event bus on a channel named after the remote function.
    3. Waiting for the ``RpcResponse`` on the temporary queue.

    This design allows RPC to work on any event bus that implements basic pub/sub and queue primitives.
    """

    def __init__(self, bus: EventBus, channel: str, spec=None, timeout=None, multi=False) -> None:
        super().__init__()
        self._bus = bus
        self._channel = channel
        self._spec = spec

        self.timeout = timeout
        self.multi = multi

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
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

    def _unmarshal(self, response: RpcResponse, raise_error: bool = False) -> Any:
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

    def _next_callback_queue(self) -> str:
        return "__rpc_" + str(uuid.uuid4())

    def _get_response_queue(self, request: RpcRequest) -> Any:
        return self._bus.queue(request.response_channel)

    def _invoke(self, request: RpcRequest) -> Union[RpcResponse, List[RpcResponse]]:
        # FIXME: the fundamental issue with this approach is that a pattern subscription '*' will break this. because
        #  such a subscription is probably just listening, and a real remote object, the expectation that there will
        #  be n results may not be correct
        fn = request.fn
        logger.debug('publishing to channel "%s" the request %s', fn, request)

        queue = self._get_response_queue(request)

        try:
            n = self._bus.publish(request, channel=fn)

            if n == 0:
                raise NoSuchRemoteError(request.fn)

            if n is None:
                if self.multi:
                    raise RuntimeError(
                        "For multi-invoke to work, publish needs to return the subscriber count, returned None"
                    )
                else:
                    n = 1

            results = list()

            for i in range(n):
                try:
                    logger.debug(
                        "waiting for response on queue %s, timeout %s,", queue.name, self.timeout
                    )
                    # FIXME: calculate overall remaining timeout
                    response: RpcResponse = queue.get(timeout=self.timeout)
                    results.append(response)
                except Empty:
                    response = RpcResponse(
                        fn, ("Gave up waiting after %s" % self.timeout,), "TimeoutError", True
                    )
                    results.append(response)

                if not self.multi:
                    return results[0]

            return results
        finally:
            self._finalize_response_queue(queue)

    def _finalize_response_queue(self, queue: Any) -> None:
        """
        Hook to do something with the queue used as response channel once it's no longer needed.

        :param queue: the response queue that was created for this invocation
        """
        pass

    def __repr__(self) -> str:
        if self._spec is None:
            return "%s()" % self._channel
        else:
            return "%s(%s)" % (self._channel, self._spec)


class DefaultSkeletonMethod:
    """
    Default implementation of an RPC skeleton (server-side).

    This class handles the execution of remote calls. It is typically subscribed to a channel
    representing a remote function. When an ``RpcRequest`` is received:
    1. It unmarshals the arguments according to the target function's signature.
    2. It invokes the local function.
    3. It wraps the result (or exception) in an ``RpcResponse``.
    4. It sends the response back to the requester via the queue specified in the request's ``response_channel``.
    """

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

    def __call__(self, request: RpcRequest) -> None:
        try:
            result = self._invoke(request)
            response = RpcResponse(request.fn, result, fullname(result))
        except Exception as e:
            logger.exception("Exception while invoking %s", request)
            response = RpcResponse(request.fn, e, fullname(e), error=True)

        self._queue_response(request, response)

    def _queue_response(self, request: RpcRequest, response: RpcResponse) -> None:
        self._bus.queue(request.response_channel).put(response)

    def _invoke(self, request: RpcRequest) -> Any:
        spec = self._fn_spec

        if not spec.args:
            if request.args:
                raise TypeError(
                    "%s takes 0 positional arguments but %d were given"
                    % (request.fn, len(request.args))
                )
        else:
            if spec.args[0] == "self":
                spec.args.remove("self")

        args = list()

        logger.debug("converting args %s to spec %s", request.args, spec)

        for i in range(min(len(request.args), len(spec.args))):
            name = spec.args[i]
            value = request.args[i]

            if name in spec.annotations:
                arg_type = spec.annotations[name]
                value = deep_from_dict(value, arg_type)

            args.append(value)

        return self._fn(*args)


class AbstractEventBus(EventBus, abc.ABC):
    """
    Base class for EventBus implementations that provides common RPC and subscription management logic.

    This class implements the high-level RPC protocol (stubs and skeletons) by leveraging
    the core pub/sub primitives. By generalizing RPC as a combination of a "publish" (for the request)
    and a "queue" (for the response), concrete providers only need to implement the fundamental
    messaging operations.

    Subclasses must implement:
    - ``_publish``: send an event to a channel.
    - ``_subscribe``: register a callback for a channel.
    - ``_unsubscribe``: unregister a callback.
    - ``queue(name)``: provide a queue implementation for the given name.
    """

    _subscribers: Dict[Tuple[str, bool], List[Callable]]
    _remote_fns: Dict[str, Callable]

    def __init__(self) -> None:
        super().__init__()
        self._subscribers = defaultdict(list)
        self._remote_fns = dict()

    def topic(self, name: str, pattern: bool = False) -> Topic:
        return WrapperTopic(self, name, pattern)

    def publish(self, event, channel: str | None = None) -> Optional[int]:
        if channel is None:
            channel = fullname(event)

        return self._publish(event, channel)

    def subscribe(
        self, callback: Callable, channel: str | None = None, pattern: bool = False
    ) -> None:
        if channel is None:
            channel = inspect_listener(callback)
            pattern = False

        logger.debug('adding to channel "%s" a callback %s', channel, callback)

        self._subscribers[(channel, pattern)].append(callback)
        self._subscribe(callback, channel, pattern)

    def unsubscribe(
        self, callback: Callable, channel: str | None = None, pattern: bool = False
    ) -> None:
        if channel is None:
            channel = inspect_listener(callback)
            pattern = False

        callbacks = self._subscribers.get((channel, pattern))

        if callbacks:
            callbacks.remove(callback)
            if len(callbacks) == 0:
                del self._subscribers[(channel, pattern)]

        self._unsubscribe(callback, channel, pattern)

    def stub(
        self, fn: Callable | str, timeout: float | None = None, multi: bool = False
    ) -> StubMethod:
        """
        Creates an RPC stub for the given function or channel name.
        The stub uses a ``StubMethod`` to handle the RPC invocation.
        """
        if callable(fn):
            channel = get_remote_name(fn)
            spec = inspect.getfullargspec(fn)
        elif isinstance(fn, str):
            channel = str(fn)
            spec = None
        else:
            raise TypeError("cannot create stub for fn type %s" % type(fn))

        return self._create_stub_method(channel, spec, timeout, multi)

    def expose(self, fn: Callable, channel: str | None = None) -> None:
        """
        Exposes a function as a remote procedure on the event bus.
        It creates a skeleton method and subscribes it to the RPC channel.
        """
        if channel is None:
            channel = get_remote_name(fn)

        if channel in self._remote_fns:
            raise ValueError("Function on channel %s already exposed" % channel)

        logger.debug('exposing at channel "%s" the function %s', channel, fn)

        skeleton = self._create_skeleton_method(channel, fn)

        self._remote_fns[channel] = skeleton
        self._bind_skeleton_method(skeleton, channel)

    def unexpose(self, fn: Callable):
        """
        Unexposes a previously exposed function.
        """
        if callable(fn):
            channel = get_remote_name(fn)
        elif isinstance(fn, str):
            channel = fn
        else:
            raise TypeError("cannot create stub for fn type %s" % type(fn))

        if channel not in self._remote_fns:
            return

        skeleton = self._remote_fns[channel]
        del self._remote_fns[channel]
        self._unbind_skeleton_method(skeleton, channel)

    def _create_stub_method(
        self, channel: str, spec: inspect.FullArgSpec | None, timeout: float | None, multi: bool
    ) -> StubMethod:
        return DefaultStubMethod(self, channel, spec, timeout, multi)

    def _create_skeleton_method(self, channel: str, fn: Callable) -> Callable[[RpcRequest], None]:
        return DefaultSkeletonMethod(self, channel, fn)

    def _bind_skeleton_method(self, skeleton, channel: str):
        self.subscribe(skeleton, channel, False)

    def _unbind_skeleton_method(self, skeleton, channel: str):
        self.unsubscribe(skeleton, channel, False)

    def _publish(self, event, channel: str) -> Optional[int]:
        raise NotImplementedError

    def _subscribe(self, callback: Callable, channel: str, pattern: bool):
        raise NotImplementedError

    def _unsubscribe(self, callback: Callable, channel: str, pattern: bool):
        raise NotImplementedError
