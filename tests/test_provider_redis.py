import queue
import threading
import time
import unittest

from timeout_decorator import timeout_decorator

import pymq
from pymq.provider.redis import RedisConfig, RedisEventBus
from tests.base.pubsub import AbstractPubSubTest
from tests.base.queue import AbstractQueueTest
from tests.base.rpc import AbstractRpcTest
from tests.testutils import RedisResource


class MyRedisEvent:
    pass


class RedisEventbusTestBase(unittest.TestCase):
    redis: RedisResource = RedisResource()
    bus: RedisEventBus

    @classmethod
    def setUpClass(cls) -> None:
        cls.redis.setUp()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.redis.tearDown()

    def setUp(self) -> None:
        self.bus = pymq.init(RedisConfig(self.redis.rds))

    def tearDown(self) -> None:
        pymq.shutdown()
        self.redis.rds.flushall()


class RedisQueueTest(RedisEventbusTestBase, AbstractQueueTest):
    pass


class InitSubscribersTest(RedisEventbusTestBase):

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_subscribe_before_init(self):
        invocations = queue.Queue()

        def handler(event: str):
            invocations.put(event)

        pymq.subscribe(handler, channel='early/subscription')

        pymq.publish('hello', channel='early/subscription')  # doesn't do anything

        try:
            super().setUp()
            pymq.publish('hello', channel='early/subscription')
            self.assertEqual('hello', invocations.get(timeout=1))
            self.assertEqual(0, invocations.qsize())
        finally:
            super().tearDown()

    def test_unsubscribe_before_init(self):
        invocations = queue.Queue()

        def handler(event: str):
            invocations.put(event)

        pymq.subscribe(handler, channel='early/subscription')
        pymq.unsubscribe(handler, channel='early/subscription')

        try:
            super().setUp()
            pymq.publish('hello', channel='early/subscription')
            self.assertRaises(queue.Empty, invocations.get, timeout=0.25)
        finally:
            super().tearDown()


class RedisPubSubTest(RedisEventbusTestBase, AbstractPubSubTest):

    def test_add_listener_creates_subscription_correctly(self):
        def listener(event: MyRedisEvent):
            pass

        self.assertEqual(0, len(self.redis.rds.pubsub_channels()),
                         'expected no subscribers, but got %s' % self.redis.rds.pubsub_channels())

        pymq.subscribe(listener)

        channels = self.redis.rds.pubsub_channels()

        self.assertEqual(1, len(channels))
        # event names are encoded in the channels
        self.assertTrue(channels[0].endswith('.MyRedisEvent'))

    def test_remove_listener_also_removes_redis_subscription(self):
        def listener1(event: MyRedisEvent):
            pass

        def listener2(event: MyRedisEvent):
            pass

        pymq.subscribe(listener1)
        pymq.subscribe(listener2)

        self.assertEqual(1, len(self.redis.rds.pubsub_channels()))
        pymq.unsubscribe(listener1)
        self.assertEqual(1, len(self.redis.rds.pubsub_channels()))
        pymq.unsubscribe(listener2)
        self.assertEqual(0, len(self.redis.rds.pubsub_channels()))


class RedisRpcTest(RedisEventbusTestBase, AbstractRpcTest):

    @timeout_decorator.timeout(5)
    def test_channel_expire(self):
        self.bus.rpc_channel_expire = 1
        called = threading.Event()

        def remotefn():
            time.sleep(1.25)
            called.set()

        pymq.expose(remotefn)

        stub = pymq.stub(remotefn, timeout=1)
        stub.rpc()
        keys = self.redis.rds.keys('*rpc*')
        self.assertEqual(0, len(keys), 'Expected no rpc results yet %s' % keys)

        called.wait()

        keys = self.redis.rds.keys('*rpc*')
        self.assertEqual(1, len(keys))

        # wait for key to expire
        time.sleep(1.25)

        keys = self.redis.rds.keys('*rpc*')
        self.assertEqual(0, len(keys), 'key did not expire')


class RedisLateInitTest(unittest.TestCase):
    redis: RedisResource = RedisResource()
    bus: RedisEventBus

    @classmethod
    def setUpClass(cls) -> None:
        cls.redis.setUp()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.redis.tearDown()

    def init(self) -> None:
        self.bus = pymq.init(RedisConfig(self.redis.rds))

    def tearDown(self) -> None:
        pymq.shutdown()
        self.redis.rds.flushall()

    @timeout_decorator.timeout(5)
    def test_early_expose(self):
        def remote_fn():
            return 'hello'

        pymq.expose(remote_fn, 'remote_fn')

        self.init()

        stub = pymq.stub('remote_fn')
        self.assertEqual('hello', stub())

    @timeout_decorator.timeout(5)
    def test_early_subscribe(self):
        event = threading.Event()

        def subscriber(arg):
            event.set()

        pymq.subscribe(subscriber, 'my_channel')

        self.init()

        pymq.publish('hello', 'my_channel')

        self.assertTrue(event.wait(2))

    @timeout_decorator.timeout(10)
    def test_early_subscribe_and_resubscribe(self):
        event = threading.Event()

        def subscriber(arg):
            event.set()

        pymq.subscribe(subscriber, 'my_channel')

        self.init()

        pymq.publish('hello', 'my_channel')
        self.assertTrue(event.wait(2))
        event.clear()

        pymq.unsubscribe(subscriber, 'my_channel')
        self.assertFalse(event.wait(1))

        pymq.subscribe(subscriber, 'my_channel')
        pymq.publish('hello', 'my_channel')
        self.assertTrue(event.wait(2))


del RedisEventbusTestBase

if __name__ == '__main__':
    unittest.main()
