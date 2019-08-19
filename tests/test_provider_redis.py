import queue
import unittest

import pymq
from pymq.provider.redis import RedisConfig
from tests.base.pubsub import AbstractPubSubTest
from tests.base.queue import AbstractQueueTest
from tests.base.rpc import AbstractRpcTest
from tests.testutils import RedisResource


class MyRedisEvent:
    pass


class RedisEventbusTestBase(unittest.TestCase):
    redis: RedisResource = RedisResource()

    @classmethod
    def setUpClass(cls) -> None:
        cls.redis.setUp()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.redis.tearDown()

    def setUp(self) -> None:
        pymq.init(RedisConfig(self.redis.rds))

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
    pass


del RedisEventbusTestBase

if __name__ == '__main__':
    unittest.main()
