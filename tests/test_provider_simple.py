import threading
import unittest

from timeout_decorator import timeout_decorator

import eventbus
from eventbus.provider.simple import SimpleEventBus


class MyEvent:
    pass


def init_listeners():
    @eventbus.listener
    def my_event_listener(event: MyEvent):
        TestEventBus.invocations.append(('my_event_listener', event))

    @eventbus.listener('some/channel')
    def some_event_listener(event):
        TestEventBus.invocations.append(('some_event_listener', event))

    @eventbus.listener('some/other/channel')
    def some_other_event_listener(event):
        TestEventBus.invocations.append(('some_other_event_listener', event))


class StatefulListener:

    def __init__(self) -> None:
        super().__init__()
        eventbus.add_listener(self.my_stateful_event_listener)
        eventbus.add_listener(self.some_stateful_event_listener, 'some/channel')

    def my_stateful_event_listener(self, event: MyEvent):
        TestEventBus.invocations.append(('my_stateful_event_listener', event))

    def some_stateful_event_listener(self, event):
        TestEventBus.invocations.append(('some_stateful_event_listener', event))


class TestEventBus(unittest.TestCase):
    invocations = list()

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        init_listeners()
        eventbus.init(SimpleEventBus)
        StatefulListener()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        eventbus.shutdown()

    def setUp(self) -> None:
        super().setUp()
        TestEventBus.invocations = list()

    def test_publish_with_type(self):
        event = MyEvent()
        eventbus.publish(event)
        self.assertEqual(2, len(self.invocations))
        self.assertIn(('my_event_listener', event), self.invocations)
        self.assertIn(('my_stateful_event_listener', event), self.invocations)

    def test_remove_listener_with_event(self):
        called = threading.Event()

        def listener(event: MyEvent):
            called.set()

        eventbus.add_listener(listener)
        eventbus.remove_listener(listener)
        eventbus.publish(MyEvent())

        self.assertEqual(2, len(self.invocations))  # should be from the other two listeners
        self.assertFalse(called.is_set())

    def test_publish_with_channel(self):
        eventbus.publish('hello', channel='some/channel')
        self.assertEqual(2, len(self.invocations))
        self.assertIn(('some_event_listener', 'hello'), self.invocations)
        self.assertIn(('some_stateful_event_listener', 'hello'), self.invocations)

    @timeout_decorator.timeout(2)
    def test_queue_put_get(self):
        q = eventbus.queue('test_queue')
        q.put('elem1')
        q.put('elem2')

        self.assertEqual('elem1', q.get())
        self.assertEqual('elem2', q.get())

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

    @timeout_decorator.timeout(3)
    def test_queue_get_blocking_timeout(self):
        q = eventbus.queue('test_queue')
        self.assertRaises(eventbus.Empty, q.get, timeout=1)

    def test_queue_get_nowait_on_empty_queue(self):
        q = eventbus.queue('test_queue')
        self.assertRaises(eventbus.Empty, q.get_nowait)


if __name__ == '__main__':
    unittest.main()
