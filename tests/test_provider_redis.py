import threading
import time
import unittest
from typing import NamedTuple

from timeout_decorator import timeout_decorator

import eventbus
from eventbus.provider.redis import RedisConfig, RedisEventBus
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


class TestRedisEventbus(unittest.TestCase):
    redis: RedisResource = RedisResource()

    redis_eventbus: RedisEventBus

    def setUp(self) -> None:
        super().setUp()
        eventbus.shutdown()
        self.redis.setUp()
        self.redis_eventbus = eventbus.init(RedisConfig(self.redis.rds))

    def tearDown(self) -> None:
        super().tearDown()
        eventbus.shutdown()
        self.redis.tearDown()

    def test_channel_registers_correctly(self):
        def listener(event: MyRedisEvent):
            pass

        self.assertEqual(0, len(self.redis.rds.pubsub_channels()), 'expected no subscribers, but got %s' % self.redis.rds.pubsub_channels())

        eventbus.add_listener(listener)

        channels = self.redis.rds.pubsub_channels()

        self.assertEqual(1, len(channels))
        # event names are encoded in the channels
        self.assertTrue(channels[0].endswith('.MyRedisEvent'))

    def test_remove_channel_behaves_correctly(self):
        def listener1(event: MyRedisEvent):
            pass

        def listener2(event: MyRedisEvent):
            pass

        eventbus.add_listener(listener1)
        eventbus.add_listener(listener2)

        self.assertEqual(1, len(self.redis.rds.pubsub_channels()))
        eventbus.remove_listener(listener1)
        self.assertEqual(1, len(self.redis.rds.pubsub_channels()))
        eventbus.remove_listener(listener2)
        self.assertEqual(0, len(self.redis.rds.pubsub_channels()))

    def test_pubsub(self):
        called = threading.Event()

        def listener(event: MyRedisEvent):
            called.set()

        eventbus.add_listener(listener)
        eventbus.publish(MyRedisEvent())

        self.assertTrue(called.wait(2))

    def test_event_serialization(self):
        called = threading.Event()

        def listener(event: EventWithPayload):
            called.payload = event.payload
            called.set()

        eventbus.add_listener(listener)
        eventbus.publish(EventWithPayload(Payload('foobar', 42)))

        self.assertTrue(called.wait(2))
        self.assertIsInstance(called.payload, Payload)
        self.assertEqual('foobar', called.payload.name)
        self.assertEqual(42, called.payload.value)

    # TODO: test explicit channels

    @timeout_decorator.timeout(2)
    def test_queue_put_get(self):
        q = eventbus.queue('test_queue')
        q.put('elem1')
        q.put('elem2')

        self.assertEqual('elem1', q.get())
        self.assertEqual('elem2', q.get())

    @timeout_decorator.timeout(2)
    def test_queue_get_blocking(self):
        q = eventbus.queue('test_queue')
        event = threading.Event()

        def put():
            event.wait()
            q.put('a')

        t = threading.Thread(target=put)
        t.start()

        event.set()
        self.assertEqual('a', q.get(block=True))
        t.join()

    @timeout_decorator.timeout(3)
    def test_queue_get_blocking_timeout(self):
        q = eventbus.queue('test_queue')
        then = time.time()
        self.assertRaises(eventbus.Empty, q.get, timeout=1)
        diff = time.time() - then
        self.assertAlmostEqual(1, diff, delta=0.3)

    @timeout_decorator.timeout(3)
    def test_queue_get_nowait_timeout(self):
        q = eventbus.queue('test_queue')
        then = time.time()
        self.assertRaises(eventbus.Empty, q.get_nowait)
        diff = time.time() - then
        self.assertAlmostEqual(0, diff, delta=0.3)

    @timeout_decorator.timeout(2)
    def test_queue_size(self):
        q1 = eventbus.queue('test_queue_1')
        q2 = eventbus.queue('test_queue_2')
        self.assertEqual(0, q1.qsize())
        self.assertEqual(0, q2.qsize())

        q1.put('elem1')
        self.assertEqual(1, q1.qsize())
        self.assertEqual(0, q2.qsize())

        q1.put('elem2')
        self.assertEqual(2, q1.qsize())
        self.assertEqual(0, q2.qsize())

        q1.get()
        self.assertEqual(1, q1.qsize())
        self.assertEqual(0, q2.qsize())

    def test_queue_primitive_types(self):
        q = eventbus.queue('test_queue')

        q.put('abc')
        self.assertIsInstance(q.get(), str)

        q.put(1)
        self.assertIsInstance(q.get(), int)

        q.put(1.1)
        self.assertIsInstance(q.get(), float)

    def test_queue_collection_types(self):
        q = eventbus.queue('test_queue')

        q.put(('a', 1))
        v = q.get()
        self.assertIsInstance(v, tuple)
        self.assertIsInstance(v[0], str)
        self.assertIsInstance(v[1], int)

        q.put([1, 'v'])
        v = q.get()
        self.assertIsInstance(v, list)
        self.assertIsInstance(v[0], int)
        self.assertIsInstance(v[1], str)

        q.put({'a': 1, 'b': 'c'})
        v = q.get()
        self.assertIsInstance(v, dict)
        self.assertIsInstance(v['a'], int)
        self.assertIsInstance(v['b'], str)

    def test_queue_complex_types(self):
        q = eventbus.queue('test_queue')

        q.put(EventWithPayload(Payload('foo', 42)))
        v = q.get()
        self.assertIsInstance(v, EventWithPayload)
        self.assertIsInstance(v.payload, Payload)
        self.assertIsInstance(v.payload.name, str)
        self.assertIsInstance(v.payload.value, int)


if __name__ == '__main__':
    unittest.main()
