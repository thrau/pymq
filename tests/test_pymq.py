import unittest
from typing import Callable

import pymq
from pymq import EventBus, StubMethod, Queue


class MockedEventBus(EventBus):
    published_events: list
    subscribers: list

    def __init__(self) -> None:
        super().__init__()
        self.published_events = list()
        self.subscribers = list()

    def publish(self, event, channel=None):
        self.published_events.append((event, channel))

    def subscribe(self, callback: Callable, channel, pattern=False):
        self.subscribers.append((callback, channel, pattern))

    def unsubscribe(self, callback: Callable, channel, pattern=False):
        pass

    def queue(self, name: str) -> Queue:
        pass

    def topic(self, name: str, pattern: bool = False):
        pass

    def stub(self, fn, timeout=None, multi=False) -> StubMethod:
        pass

    def expose(self, fn, channel=None):
        pass

    def run(self):
        pass

    def close(self):
        pass


class PyMQTest(unittest.TestCase):
    def test_queue_on_non_started_bus(self):
        self.assertRaises(ValueError, pymq.queue, 'foo')

    def test_start_without_init(self):
        self.assertRaises(ValueError, pymq.core.start)

    def test_lazy_topic_publish(self):
        topic = pymq.topic('my_topic')

        self.assertEqual('my_topic', topic.name)
        self.assertFalse(topic.is_pattern)

        self.assertIsNone(topic.publish('does_nothing'))

        bus: MockedEventBus = pymq.init(MockedEventBus)

        try:
            topic.publish('some_event')

            self.assertEqual(1, len(bus.published_events), 'expected one event to be published')
            self.assertIn(('some_event', 'my_topic'), bus.published_events)
        finally:
            pymq.shutdown()

    def test_lazy_topic_subscribe(self):
        topic = pymq.topic('my_topic')

        def callback():
            pass

        self.assertIsNone(topic.subscribe(callback))

        bus: MockedEventBus = pymq.init(MockedEventBus)

        try:
            self.assertIn((callback, 'my_topic', False), bus.subscribers)
        finally:
            pymq.shutdown()
