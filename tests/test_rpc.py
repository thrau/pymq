import logging
import time
from typing import List

import pytest

import pymq
from pymq import NoSuchRemoteError
from pymq.exceptions import RemoteInvocationError
from pymq.typing import deep_from_dict, deep_to_dict

logger = logging.getLogger(__name__)


class EchoCommand:
    param: str

    def __init__(self, param: str) -> None:
        super().__init__()
        self.param = param

    def __str__(self) -> str:
        return "EchoCommand(%s)" % self.__dict__


class EchoResponse:
    result: str

    def __init__(self, result: str) -> None:
        super().__init__()
        self.result = result

    def __str__(self) -> str:
        return "EchoResponse(%s)" % self.__dict__


def void_function() -> None:
    pass


def delaying_function() -> None:
    time.sleep(1.5)


def simple_remote_function(param) -> str:
    return f"Hello {param}!"


def simple_multiple_param_function(p1: int, p2: int) -> int:
    return p1 * p2


def simple_multiple_param_default_function(p1: int, p2: int = 3) -> int:
    return p1 * p2


def simple_list_param_function(ls: List[int]) -> int:
    return sum(ls)


def echo_command_function(cmd: EchoCommand) -> str:
    return "Hello %s!" % cmd.param


def echo_command_response_function(cmd: EchoCommand) -> EchoResponse:
    return EchoResponse("Hello %s!" % cmd.param)


def error_function():
    raise ValueError("oh noes")


class RpcHolder:
    def __init__(self, prefix="Hello") -> None:
        super().__init__()
        self.prefix = prefix

    def echo(self, cmd: EchoCommand) -> EchoResponse:
        return EchoResponse("%s %s!" % (self.prefix, cmd.param))


# noinspection PyUnresolvedReferences
@pytest.mark.xfail_provider("init_aws")
class TestRpc:
    @pytest.mark.timeout(60)
    def test_assert_bus(self, bus):
        # gives localstack a chance to start
        assert bus

    @pytest.mark.timeout(2)
    def test_marshall_rpc_request(self, bus):
        request = pymq.RpcRequest("some_function", "callback_queue", ("simple_arg",))

        request_tuple = deep_to_dict(request)
        assert ("some_function", "callback_queue", ("simple_arg",), None) == request_tuple

        request_unmarshalled: pymq.RpcRequest = deep_from_dict(request_tuple, pymq.RpcRequest)

        assert "some_function" == request_unmarshalled.fn
        assert "callback_queue" == request_unmarshalled.response_channel
        assert ("simple_arg",) == request_unmarshalled.args

    @pytest.mark.timeout(2)
    def test_rpc_on_non_exposed_remote_raises_exception(self, bus):
        stub = bus.stub("simple_remote_function")

        with pytest.raises(NoSuchRemoteError):
            stub.rpc("test")

    @pytest.mark.timeout(2)
    def test_call_on_non_exposed_remote_returns_none(self, bus):
        stub = bus.stub("simple_remote_function")
        assert stub("test") is None

    @pytest.mark.timeout(2)
    def test_void_function(self, bus):
        bus.expose(void_function, channel="void_function")

        stub = bus.stub("void_function")
        result = stub()
        assert result is None

    @pytest.mark.timeout(2)
    def test_void_function_error(self, bus):
        bus.expose(void_function, channel="void_function")

        stub = bus.stub("void_function")
        with pytest.raises(RemoteInvocationError):
            stub(1, 2, 3)

    @pytest.mark.timeout(2)
    def test_simple_function(self, bus):
        bus.expose(simple_remote_function, channel="simple_remote_function")

        stub = bus.stub("simple_remote_function")
        result = stub("unittest")
        assert "Hello unittest!" == result

    @pytest.mark.timeout(2)
    def test_simple_multiple_param_function(self, bus):
        bus.expose(simple_multiple_param_function, channel="simple_multiple_param_function")

        stub = bus.stub("simple_multiple_param_function")
        result = stub(2, 3)
        assert 6 == result

    @pytest.mark.timeout(2)
    def test_simple_multiple_param_default_function(self, bus):
        bus.expose(
            simple_multiple_param_default_function, channel="simple_multiple_param_default_function"
        )

        stub = bus.stub("simple_multiple_param_default_function")
        result = stub(2)
        assert 6 == result

    @pytest.mark.timeout(2)
    def test_simple_list_param_function(self, bus):
        bus.expose(simple_list_param_function, channel="simple_list_param_function")

        stub = bus.stub("simple_list_param_function")
        result = stub([2, 3, 4])
        assert 9 == result

    @pytest.mark.timeout(2)
    def test_echo_command_function(self, bus):
        bus.expose(echo_command_function, channel="echo_command_function")

        stub = bus.stub("echo_command_function")
        assert "Hello unittest!" == stub(EchoCommand("unittest"))

    @pytest.mark.timeout(2)
    def test_echo_command_response_function(self, bus):
        bus.expose(echo_command_response_function, channel="echo_command_response_function")

        stub = bus.stub("echo_command_response_function")
        result = stub(EchoCommand("unittest"))

        assert isinstance(result, EchoResponse)
        assert result.result == "Hello unittest!"

    @pytest.mark.timeout(5)
    def test_timeout(self, bus):
        bus.expose(delaying_function, channel="delaying_function")
        stub = bus.stub("delaying_function", timeout=1)
        with pytest.raises(RemoteInvocationError):
            stub()

    @pytest.mark.timeout(2)
    def test_stateful_rpc(self, bus):
        obj = RpcHolder()
        bus.expose(obj.echo)

        stub = bus.stub(RpcHolder.echo)
        result = stub(EchoCommand("unittest"))
        assert isinstance(result, EchoResponse)
        assert "Hello unittest!" == result.result

    @pytest.mark.timeout(2)
    def test_remote_decorator(self, bus):
        @pymq.remote
        def remote_test_fn(param: str) -> str:
            return "hello %s" % param

        stub = bus.stub(remote_test_fn)
        assert "hello unittest" == stub("unittest")

    @pytest.mark.timeout(2)
    def test_error_function(self, bus):
        bus.expose(error_function, channel="error_function")

        stub = bus.stub("error_function")

        with pytest.raises(RemoteInvocationError) as e:
            stub()

        e.match("ValueError")

    @pytest.mark.timeout(2)
    def test_expose_multiple_times_raises_error(self, bus):
        bus.expose(void_function)

        with pytest.raises(ValueError):
            bus.expose(void_function)

    @pytest.mark.timeout(2)
    def test_expose_multiple_by_channel_times_raises_error(self, bus):
        bus.expose(void_function, channel="void_function")

        with pytest.raises(ValueError):
            bus.expose(void_function, channel="void_function")

    @pytest.mark.timeout(2)
    def test_expose_after_unexpose(self, bus):
        bus.expose(void_function)
        bus.unexpose(void_function)
        bus.expose(void_function)

    @pytest.mark.timeout(2)
    def test_expose_after_unexpose_by_channel(self, bus):
        bus.expose(void_function, channel="void_function")
        bus.unexpose("void_function")
        bus.expose(void_function, channel="void_function")

    @pytest.mark.timeout(2)
    def test_rpc_after_unexpose_raises_exception(self, bus):
        bus.expose(simple_remote_function, "simple_remote_function")

        stub = bus.stub("simple_remote_function")
        assert "Hello test!" == stub("test")

        bus.unexpose("simple_remote_function")

        with pytest.raises(NoSuchRemoteError):
            stub.rpc("test")

    @pytest.mark.timeout(5)
    def test_expose_after_unexpose_by_channel_calls_correct_method(self, bus):
        def fn1():
            return 1

        def fn2():
            return 2

        bus.expose(fn1, channel="myfn")
        stub = bus.stub("myfn")
        assert 1 == stub()

        logger.debug("unexposing myfn")
        bus.unexpose("myfn")

        time.sleep(1)  # FIXME i have no idea why this is necessary

        logger.debug("exposing myfn")
        bus.expose(fn2, channel="myfn")

        logger.debug("creating second stub for myfn")
        stub = bus.stub("myfn")
        logger.debug("calling stub for myfn")
        assert 2 == stub()

    @pytest.mark.timeout(5)
    def test_expose_before_init(self, pymq_init):
        def remote_fn():
            return "hello"

        pymq.expose(remote_fn, "remote_fn")

        with pytest.raises(ValueError):
            stub = pymq.stub("remote_fn")
            stub()

        pymq_init()

        stub = pymq.stub("remote_fn")
        assert "hello" == stub()
