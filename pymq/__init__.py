import atexit

from pymq.core import init, shutdown, publish, subscribe, unsubscribe, subscriber, EventBus, Queue, Empty, \
    queue, expose, stub, StubMethod, remote, RpcRequest, RpcResponse, Topic, subscribe, topic

name = "pymq"

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
    'remote',
    'RpcResponse',
    'RpcRequest',
    'StubMethod',
    # provider
    'provider'
]

atexit.register(shutdown)
