import unittest

import pymq
from pymq.provider.redis import RedisConfig, RedisEventBus
from tests.base.pubsub import AbstractPubSubTest
from tests.base.queue import AbstractQueueTest
from tests.base.rpc import AbstractRpcTest
from tests.testutils import RedisResource


class MyRedisEvent:
    pass


class RedisTestHelper:
    redis: RedisResource = RedisResource()

    def setUpEventbus(self) -> RedisEventBus:
        self.redis.setUp()
        return pymq.init(RedisConfig(self.redis.rds))

    def tearDownEventbus(self) -> None:
        pymq.shutdown()
        self.redis.tearDown()


class RedisQueueTest(unittest.TestCase, RedisTestHelper, AbstractQueueTest):
    redis: RedisResource = RedisResource()

    def setUp(self) -> None:
        super().setUp()
        super().setUpEventbus()

    def tearDown(self) -> None:
        super().tearDown()
        super().tearDownEventbus()


class RedisPubSubTest(unittest.TestCase, RedisTestHelper, AbstractPubSubTest):
    redis: RedisResource = RedisResource()

    def setUp(self) -> None:
        super().setUp()
        super().setUpEventbus()

    def tearDown(self) -> None:
        super().tearDown()
        super().tearDownEventbus()

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


class RedisRpcTest(unittest.TestCase, RedisTestHelper, AbstractRpcTest):

    def setUp(self) -> None:
        super().setUp()
        super().setUpEventbus()

    def tearDown(self) -> None:
        super().tearDown()
        super().tearDownEventbus()


if __name__ == '__main__':
    unittest.main()
