import atexit

from pymq.core import init, shutdown, publish, subscribe, unsubscribe, subscriber, EventBus, Queue, Empty, \
    queue, expose, unexpose, stub, StubMethod, remote, RpcRequest, RpcResponse, Topic, subscribe, topic
from pymq.exceptions import RpcException, NoSuchRemoteError, RemoteInvocationError

name = "pymq"

__version__ = "0.5.0.dev1"

__all__ = [
    # server
    'init',
    'shutdown',
    # core api
    'EventBus',
    # pubsub
    'Topic',
    'topic',
    'publish',
    'subscribe',
    'unsubscribe',
    'subscriber',
    # queue
    'Queue',
    'Empty',
    'queue',
    # rpc
    'stub',
    'expose',
    'unexpose',
    'remote',
    'RpcResponse',
    'RpcRequest',
    'StubMethod',
    # provider
    'provider',
    # exceptions
    'RpcException',
    'NoSuchRemoteError',
    'RemoteInvocationError'
]

atexit.register(shutdown)
