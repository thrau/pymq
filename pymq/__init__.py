import atexit

from pymq.core import init, shutdown, publish, add_listener, remove_listener, listener, Queue, Empty, \
    queue, rpc, expose, remote, RpcRequest, RpcResponse

name = "pymq"

__all__ = [
    # server
    'init',
    'shutdown',
    # pubsub
    'publish',
    'add_listener',
    'remove_listener',
    'listener',
    # queue
    'Queue',
    'Empty',
    'queue',
    # rpc
    'rpc',
    'expose',
    'remote',
    'RpcResponse',
    'RpcRequest',
    # provider
    'provider'
]

atexit.register(shutdown)
