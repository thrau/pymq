class RpcException(Exception):
    pass


class RemoteInvocationError(RpcException):
    pass


class NoSuchRemoteError(RpcException):
    pass
