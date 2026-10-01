"""
Exception types raised by the pymq RPC layer.
"""


class RpcException(Exception):
    """
    Base class for all RPC related exceptions.
    """

    pass


class RemoteInvocationError(RpcException):
    """
    Raised when a remote invocation failed, i.e., the remote function raised an exception.
    """

    pass


class NoSuchRemoteError(RpcException):
    """
    Raised when a stub is called but no matching remote function is exposed.
    """

    pass
