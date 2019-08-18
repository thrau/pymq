import threading
import unittest
from typing import NamedTuple

import pymq
from pymq.provider.redis import RedisConfig, RedisEventBus
from tests.base.queue import AbstractQueueTest
from tests.testutils import RedisResource


class Payload(NamedTuple):
    name: str
    value: int


class MyRedisEvent:
    pass


class EventWithPayload:
    payload: Payload

    def __init__(self, payload: Payload) -> None:
        self.payload = payload


class RedisQueueTest(unittest.TestCase, AbstractQueueTest):
    redis: RedisResource = RedisResource()

    def setUp(self) -> None:
        super().setUp()
        self.redis.setUp()
        pymq.init(RedisConfig(self.redis.rds))

    def tearDown(self) -> None:
        super().tearDown()
        pymq.shutdown()
        self.redis.tearDown()


class TestRedisEventbus(unittest.TestCase):
    redis: RedisResource = RedisResource()

    redis_eventbus: RedisEventBus

    def setUp(self) -> None:
        super().setUp()
        pymq.shutdown()
        self.redis.setUp()
        self.redis_eventbus = pymq.init(RedisConfig(self.redis.rds))

    def tearDown(self) -> None:
        super().tearDown()
        pymq.shutdown()
        self.redis.tearDown()

    def test_channel_registers_correctly(self):
        def listener(event: MyRedisEvent):
            pass

        self.assertEqual(0, len(self.redis.rds.pubsub_channels()),
                         'expected no subscribers, but got %s' % self.redis.rds.pubsub_channels())

        pymq.add_listener(listener)

        channels = self.redis.rds.pubsub_channels()

        self.assertEqual(1, len(channels))
        # event names are encoded in the channels
        self.assertTrue(channels[0].endswith('.MyRedisEvent'))

    def test_remove_channel_behaves_correctly(self):
        def listener1(event: MyRedisEvent):
            pass

        def listener2(event: MyRedisEvent):
            pass

        pymq.add_listener(listener1)
        pymq.add_listener(listener2)

        self.assertEqual(1, len(self.redis.rds.pubsub_channels()))
        pymq.remove_listener(listener1)
        self.assertEqual(1, len(self.redis.rds.pubsub_channels()))
        pymq.remove_listener(listener2)
        self.assertEqual(0, len(self.redis.rds.pubsub_channels()))

    def test_pubsub(self):
        called = threading.Event()

        def listener(event: MyRedisEvent):
            called.set()

        pymq.add_listener(listener)
        pymq.publish(MyRedisEvent())

        self.assertTrue(called.wait(2))

    def test_event_serialization(self):
        called = threading.Event()

        def listener(event: EventWithPayload):
            called.payload = event.payload
            called.set()

        pymq.add_listener(listener)
        pymq.publish(EventWithPayload(Payload('foobar', 42)))

        self.assertTrue(called.wait(2))
        self.assertIsInstance(called.payload, Payload)
        self.assertEqual('foobar', called.payload.name)
        self.assertEqual(42, called.payload.value)

    # TODO: test explicit channels


if __name__ == '__main__':
    unittest.main()
