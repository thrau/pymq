import abc
import logging
import time
from typing import List

from timeout_decorator import timeout_decorator

import pymq
from pymq import NoSuchRemoteError
from pymq.exceptions import RemoteInvocationError
from pymq.typing import deep_to_dict, deep_from_dict

logger = logging.getLogger(__name__)


class EchoCommand:
    param: str

    def __init__(self, param: str) -> None:
        super().__init__()
        self.param = param

    def __str__(self) -> str:
        return 'EchoCommand(%s)' % self.__dict__


class EchoResponse:
    result: str

    def __init__(self, result: str) -> None:
        super().__init__()
        self.result = result

    def __str__(self) -> str:
        return 'EchoResponse(%s)' % self.__dict__


def void_function() -> None:
    pass


def delaying_function() -> None:
    time.sleep(1.5)


def simple_remote_function(param) -> str:
    return f'Hello {param}!'


def simple_multiple_param_function(p1: int, p2: int) -> int:
    return p1 * p2


def simple_multiple_param_default_function(p1: int, p2: int = 3) -> int:
    return p1 * p2


def simple_list_param_function(ls: List[int]) -> int:
    return sum(ls)


def echo_command_function(cmd: EchoCommand) -> str:
    return 'Hello %s!' % cmd.param


def echo_command_response_function(cmd: EchoCommand) -> EchoResponse:
    return EchoResponse('Hello %s!' % cmd.param)


def error_function():
    raise ValueError('oh noes')


class RpcHolder:

    def __init__(self, prefix='Hello') -> None:
        super().__init__()
        self.prefix = prefix

    def echo(self, cmd: EchoCommand) -> EchoResponse:
        return EchoResponse('%s %s!' % (self.prefix, cmd.param))


# noinspection PyUnresolvedReferences
class AbstractRpcTest(abc.ABC):

    @timeout_decorator.timeout(2)
    def test_marshall_rpc_request(self):
        request = pymq.RpcRequest('some_function', 'callback_queue', ('simple_arg',))

        request_tuple = deep_to_dict(request)
        self.assertEqual(('some_function', 'callback_queue', ('simple_arg',), None), request_tuple)

        request_unmarshalled: pymq.RpcRequest = deep_from_dict(request_tuple, pymq.RpcRequest)

        self.assertEqual('some_function', request_unmarshalled.fn)
        self.assertEqual('callback_queue', request_unmarshalled.response_channel)
        self.assertEqual(('simple_arg',), request_unmarshalled.args)

    @timeout_decorator.timeout(2)
    def test_rpc_on_non_exposed_remote_raises_exception(self):
        stub = pymq.stub('simple_remote_function')
        self.assertRaises(NoSuchRemoteError, stub.rpc, 'test')

    @timeout_decorator.timeout(2)
    def test_call_on_non_exposed_remote_returns_none(self):
        stub = pymq.stub('simple_remote_function')
        self.assertIsNone(stub('test'))

    @timeout_decorator.timeout(2)
    def test_void_function(self):
        pymq.expose(void_function, channel='void_function')

        stub = pymq.stub('void_function')
        result = stub()
        self.assertIsNone(result)

    @timeout_decorator.timeout(2)
    def test_void_function_error(self):
        pymq.expose(void_function, channel='void_function')

        stub = pymq.stub('void_function')
        self.assertRaises(RemoteInvocationError, stub, 1, 2, 3)

    @timeout_decorator.timeout(2)
    def test_simple_function(self):
        pymq.expose(simple_remote_function, channel='simple_remote_function')

        stub = pymq.stub('simple_remote_function')
        result = stub('unittest')
        self.assertEqual('Hello unittest!', result)

    @timeout_decorator.timeout(2)
    def test_simple_multiple_param_function(self):
        pymq.expose(simple_multiple_param_function, channel='simple_multiple_param_function')

        stub = pymq.stub('simple_multiple_param_function')
        result = stub(2, 3)
        self.assertEqual(6, result)

    @timeout_decorator.timeout(2)
    def test_simple_multiple_param_default_function(self):
        pymq.expose(simple_multiple_param_default_function, channel='simple_multiple_param_default_function')

        stub = pymq.stub('simple_multiple_param_default_function')
        result = stub(2)
        self.assertEqual(6, result)

    @timeout_decorator.timeout(2)
    def test_simple_list_param_function(self):
        pymq.expose(simple_list_param_function, channel='simple_list_param_function')

        stub = pymq.stub('simple_list_param_function')
        result = stub([2, 3, 4])
        self.assertEqual(9, result)

    @timeout_decorator.timeout(2)
    def test_echo_command_function(self):
        pymq.expose(echo_command_function, channel='echo_command_function')

        stub = pymq.stub('echo_command_function')
        self.assertEqual('Hello unittest!', stub(EchoCommand('unittest')))

    @timeout_decorator.timeout(2)
    def test_echo_command_response_function(self):
        pymq.expose(echo_command_response_function, channel='echo_command_response_function')

        stub = pymq.stub('echo_command_response_function')
        result = stub(EchoCommand('unittest'))

        self.assertIsInstance(result, EchoResponse)
        self.assertEqual(result.result, 'Hello unittest!')

    @timeout_decorator.timeout(5)
    def test_timeout(self):
        pymq.expose(delaying_function, channel='delaying_function')
        stub = pymq.stub('delaying_function', timeout=1)
        self.assertRaises(RemoteInvocationError, stub)

    @timeout_decorator.timeout(2)
    def test_stateful_rpc(self):
        obj = RpcHolder()
        pymq.expose(obj.echo)

        stub = pymq.stub(RpcHolder.echo)
        result = stub(EchoCommand('unittest'))
        self.assertIsInstance(result, EchoResponse)
        self.assertEqual('Hello unittest!', result.result)

    @timeout_decorator.timeout(2)
    def test_remote_decorator(self):
        @pymq.remote
        def remote_test_fn(param: str) -> str:
            return 'hello %s' % param

        stub = pymq.stub(remote_test_fn)
        self.assertEqual('hello unittest', stub('unittest'))

    @timeout_decorator.timeout(2)
    def test_error_function(self):
        pymq.expose(error_function, channel='error_function')

        stub = pymq.stub('error_function')
        try:
            result = stub()
            self.fail('Should have thrown exception, but received result %s' % result)
        except RemoteInvocationError as e:
            self.assertIn('ValueError', str(e))

    @timeout_decorator.timeout(2)
    def test_expose_multiple_times_raises_error(self):
        pymq.expose(void_function)

        try:
            pymq.expose(void_function)
            self.fail('expected exception when exposing a function on the same channel')
        except ValueError:
            pass

    @timeout_decorator.timeout(2)
    def test_expose_multiple_by_channel_times_raises_error(self):
        pymq.expose(void_function, channel='void_function')

        try:
            pymq.expose(void_function, channel='void_function')
            self.fail('expected exception when exposing a function on the same channel')
        except ValueError:
            pass

    @timeout_decorator.timeout(2)
    def test_expose_after_unexpose(self):
        pymq.expose(void_function)
        pymq.unexpose(void_function)
        pymq.expose(void_function)

    @timeout_decorator.timeout(2)
    def test_expose_after_unexpose_by_channel(self):
        pymq.expose(void_function, channel='void_function')
        pymq.unexpose('void_function')
        pymq.expose(void_function, channel='void_function')

    @timeout_decorator.timeout(2)
    def test_rpc_after_unexpose_raises_exception(self):
        pymq.expose(simple_remote_function, 'simple_remote_function')

        stub = pymq.stub('simple_remote_function')
        self.assertEqual('Hello test!', stub('test'))

        pymq.unexpose('simple_remote_function')
        self.assertRaises(NoSuchRemoteError, stub.rpc, 'test')

    @timeout_decorator.timeout(5)
    def test_expose_after_unexpose_by_channel_calls_correct_method(self):
        def fn1():
            return 1

        def fn2():
            return 2

        pymq.expose(fn1, channel='myfn')
        stub = pymq.stub('myfn')
        self.assertEqual(1, stub())

        logger.debug('unexposing myfn')
        pymq.unexpose('myfn')

        time.sleep(1)  # FIXME i have no idea why this is necessary

        logger.debug('exposing myfn')
        pymq.expose(fn2, channel='myfn')

        logger.debug('creating second stub for myfn')
        stub = pymq.stub('myfn')
        logger.debug('calling stub for myfn')
        self.assertEqual(2, stub())
