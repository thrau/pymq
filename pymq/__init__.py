"""
pymq: A simple message-oriented middleware library for Python IPC across machine boundaries.
"""

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
from pymq.version import __version__

name = "pymq"

__all__ = [
    "__version__",
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
