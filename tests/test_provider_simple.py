import threading
import unittest

from timeout_decorator import timeout_decorator

import pymq
from pymq.provider.simple import SimpleEventBus


class MyEvent:
    pass


def init_listeners():
    @pymq.listener
    def my_event_listener(event: MyEvent):
        TestEventBus.invocations.append(('my_event_listener', event))

    @pymq.listener('some/channel')
    def some_event_listener(event):
        TestEventBus.invocations.append(('some_event_listener', event))

    @pymq.listener('some/other/channel')
    def some_other_event_listener(event):
        TestEventBus.invocations.append(('some_other_event_listener', event))


class StatefulListener:

    def __init__(self) -> None:
        super().__init__()
        pymq.add_listener(self.my_stateful_event_listener)
        pymq.add_listener(self.some_stateful_event_listener, 'some/channel')

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
        pymq.init(SimpleEventBus)
        StatefulListener()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        pymq.shutdown()

    def setUp(self) -> None:
        super().setUp()
        TestEventBus.invocations = list()

    def test_publish_with_type(self):
        event = MyEvent()
        pymq.publish(event)
        self.assertEqual(2, len(self.invocations))
        self.assertIn(('my_event_listener', event), self.invocations)
        self.assertIn(('my_stateful_event_listener', event), self.invocations)

    def test_remove_listener_with_event(self):
        called = threading.Event()

        def listener(event: MyEvent):
            called.set()

        pymq.add_listener(listener)
        pymq.remove_listener(listener)
        pymq.publish(MyEvent())

        self.assertEqual(2, len(self.invocations))  # should be from the other two listeners
        self.assertFalse(called.is_set())

    def test_publish_with_channel(self):
        pymq.publish('hello', channel='some/channel')
        self.assertEqual(2, len(self.invocations))
        self.assertIn(('some_event_listener', 'hello'), self.invocations)
        self.assertIn(('some_stateful_event_listener', 'hello'), self.invocations)

    @timeout_decorator.timeout(2)
    def test_queue_put_get(self):
        q = pymq.queue('test_queue')
        q.put('elem1')
        q.put('elem2')

        self.assertEqual('elem1', q.get())
        self.assertEqual('elem2', q.get())

    @timeout_decorator.timeout(2)
    def test_queue_size(self):
        q1 = pymq.queue('test_queue_1')
        q2 = pymq.queue('test_queue_2')
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
        q = pymq.queue('test_queue')
        self.assertRaises(pymq.Empty, q.get, timeout=1)

    def test_queue_get_nowait_on_empty_queue(self):
        q = pymq.queue('test_queue')
        self.assertRaises(pymq.Empty, q.get_nowait)


if __name__ == '__main__':
    unittest.main()
