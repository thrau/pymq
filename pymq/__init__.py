import atexit

from pymq.core import (
    Empty,
    EventBus,
    Queue,
    RpcRequest,
    RpcResponse,
    StubMethod,
    Topic,
    expose,
    init,
    publish,
    queue,
    remote,
    shutdown,
    stub,
    subscribe,
    subscriber,
    topic,
    unexpose,
    unsubscribe,
)
from pymq.exceptions import NoSuchRemoteError, RemoteInvocationError, RpcException

name = "pymq"

__version__ = "0.6.0"

__all__ = [
    # server
    "init",
    "shutdown",
    # core api
    "EventBus",
    # pubsub
    "Topic",
    "topic",
    "publish",
    "subscribe",
    "unsubscribe",
    "subscriber",
    # queue
    "Queue",
    "Empty",
    "queue",
    # rpc
    "stub",
    "expose",
    "unexpose",
    "remote",
    "RpcResponse",
    "RpcRequest",
    "StubMethod",
    # provider
    "provider",
    # exceptions
    "RpcException",
    "NoSuchRemoteError",
    "RemoteInvocationError",
]

atexit.register(shutdown)
