import atexit

from pymq.core import init, shutdown, publish, subscribe, unsubscribe, subscriber, Queue, Empty, \
    queue, expose, stub, remote, RpcRequest, RpcResponse, Topic, subscribe, topic

name = "pymq"

__all__ = [
    # server
    'init',
    'shutdown',
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
    # provider
    'provider'
]

atexit.register(shutdown)
