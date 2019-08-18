import time
import unittest
from typing import List

from timeout_decorator import timeout_decorator

import eventbus
from eventbus import rpc, expose, remote, RpcResponse, RpcRequest
from eventbus.provider.redis import RedisEventBus, RedisConfig
from eventbus.typing import deep_to_dict, deep_from_dict
from tests.testutils import RedisResource


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
    time.sleep(2)


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


class RpcHolder:

    def __init__(self, prefix='Hello') -> None:
        super().__init__()
        self.prefix = prefix

    def echo(self, cmd: EchoCommand) -> EchoResponse:
        return EchoResponse('%s %s!' % (self.prefix, cmd.param))


class TestRedisRpc(unittest.TestCase):
    redis: RedisResource = RedisResource()

    redis_eventbus: RedisEventBus

    def setUp(self) -> None:
        super().setUp()
        self.redis.setUp()
        self.redis_eventbus = eventbus.init(RedisConfig(self.redis.rds))

    def tearDown(self) -> None:
        super().tearDown()
        eventbus.shutdown()
        self.redis.tearDown()

    @timeout_decorator.timeout(2)
    def test_marshall_rpc_request(self):
        request = eventbus.RpcRequest('some_function', 'callback_queue', ['simple_arg'])

        request_dict = deep_to_dict(request)
        self.assertEqual({'fn': 'some_function', 'callback_queue': 'callback_queue', 'args': ['simple_arg']},
                         request_dict)

        request_unmarshalled = deep_from_dict(request_dict, RpcRequest)

        self.assertEqual('some_function', request_unmarshalled.fn)
        self.assertEqual('callback_queue', request_unmarshalled.callback_queue)
        self.assertEqual(['simple_arg'], request_unmarshalled.args)

    @timeout_decorator.timeout(2)
    def test_void_function(self):
        expose(void_function, channel='void_function')

        result = rpc('void_function')
        self.assertEqual(1, len(result))

        response: RpcResponse = result[0]
        self.assertIsInstance(response, RpcResponse)
        self.assertEqual('void_function', response.fn)
        self.assertFalse(response.error, msg='Did not expected error: %s' % response.result)
        self.assertIsNone(response.result)

    @timeout_decorator.timeout(2)
    def test_void_function_error(self):
        expose(void_function, channel='void_function')

        result = rpc('void_function', 1, 2, 3)
        self.assertEqual(1, len(result))

        response: RpcResponse = result[0]
        self.assertIsInstance(response, RpcResponse)
        self.assertEqual('void_function', response.fn)
        self.assertTrue(response.error, msg='Expected error but result was: %s' % response.result)
        self.assertIn('void_function takes 0 positional arguments but 3 were given', response.result)

    @timeout_decorator.timeout(2)
    def test_simple_function(self):
        expose(simple_remote_function, channel='simple_remote_function')

        result = rpc('simple_remote_function', 'unittest')
        self.assertEqual(1, len(result))

        response: RpcResponse = result[0]
        self.assertIsInstance(response, RpcResponse)
        self.assertEqual('simple_remote_function', response.fn)
        self.assertFalse(response.error, msg='Did not expected error: %s' % response.result)
        self.assertEqual('Hello unittest!', response.result)

    @timeout_decorator.timeout(2)
    def test_simple_multiple_param_function(self):
        expose(simple_multiple_param_function, channel='simple_multiple_param_function')

        result = rpc('simple_multiple_param_function', 2, 3)
        self.assertEqual(1, len(result))

        response: RpcResponse = result[0]
        self.assertIsInstance(response, RpcResponse)
        self.assertEqual('simple_multiple_param_function', response.fn)
        self.assertFalse(response.error, msg='Did not expected error: %s' % response.result)
        self.assertEqual(6, response.result)

    @timeout_decorator.timeout(2)
    def test_simple_multiple_param_default_function(self):
        expose(simple_multiple_param_default_function, channel='simple_multiple_param_default_function')

        result = rpc('simple_multiple_param_default_function', 2)
        self.assertEqual(1, len(result))

        response: RpcResponse = result[0]
        self.assertIsInstance(response, RpcResponse)
        self.assertEqual('simple_multiple_param_default_function', response.fn)
        self.assertFalse(response.error, msg='Did not expected error: %s' % response.result)
        self.assertEqual(6, response.result)

    @timeout_decorator.timeout(2)
    def test_simple_list_param_function(self):
        expose(simple_list_param_function, channel='simple_list_param_function')

        result = rpc('simple_list_param_function', [2, 3, 4])
        self.assertEqual(1, len(result))

        response: RpcResponse = result[0]
        self.assertIsInstance(response, RpcResponse)
        self.assertEqual('simple_list_param_function', response.fn)
        self.assertFalse(response.error, msg='Did not expected error: %s' % response.result)
        self.assertEqual(9, response.result)

    @timeout_decorator.timeout(2)
    def test_echo_command_function(self):
        expose(echo_command_function, channel='echo_command_function')

        result = rpc('echo_command_function', EchoCommand('unittest'))
        self.assertEqual(1, len(result))

        response: RpcResponse = result[0]
        self.assertIsInstance(response, RpcResponse)
        self.assertEqual('echo_command_function', response.fn)
        self.assertFalse(response.error, msg='Did not expected error: %s' % response.result)
        self.assertEqual('Hello unittest!', response.result)

    @timeout_decorator.timeout(2)
    def test_echo_command_response_function(self):
        expose(echo_command_response_function, channel='echo_command_response_function')

        result = rpc('echo_command_response_function', EchoCommand('unittest'))
        self.assertEqual(1, len(result))

        response: RpcResponse = result[0]
        self.assertIsInstance(response, RpcResponse)
        self.assertEqual('echo_command_response_function', response.fn)
        self.assertFalse(response.error, msg='Did not expected error: %s' % response.result)
        self.assertIsInstance(response.result, EchoResponse)
        self.assertEqual('Hello unittest!', response.result.result)

    @timeout_decorator.timeout(5)
    def test_timeout(self):
        expose(delaying_function, channel='delaying_function')

        result = rpc('delaying_function', timeout=1)
        self.assertEqual(1, len(result))

        response: RpcResponse = result[0]
        self.assertIsInstance(response, RpcResponse)
        self.assertEqual('delaying_function', response.fn)
        self.assertTrue(response.error, msg='Expected error but result was: %s' % response.result)
        self.assertIsInstance(response.result, TimeoutError)

    @timeout_decorator.timeout(2)
    def test_stateful_rpc(self):
        obj = RpcHolder()
        expose(obj.echo)

        result = rpc(RpcHolder.echo, EchoCommand('unittest'))
        self.assertEqual(1, len(result))

        response: RpcResponse = result[0]
        self.assertIsInstance(response, RpcResponse)
        self.assertEqual('test_rpc.RpcHolder.echo', response.fn)
        self.assertFalse(response.error, msg='Did not expected error: %s' % response.result)
        self.assertIsInstance(response.result, EchoResponse)
        self.assertEqual('Hello unittest!', response.result.result)

    @timeout_decorator.timeout(2)
    def test_remote_decorator(self):
        @remote
        def remote_test_fn(param: str) -> str:
            return 'hello %s' % param

        result = rpc(remote_test_fn, 'unittest')
        self.assertEqual(1, len(result))
        self.assertEqual('hello unittest', result[0].result)
