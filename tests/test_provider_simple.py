import threading
import time
import unittest

import pymq
from pymq.provider.simple import SimpleEventBus
from tests.base.queue import AbstractQueueTest
from tests.base.rpc import AbstractRpcTest


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


class SimpleQueueTest(unittest.TestCase, AbstractQueueTest):

    def setUp(self) -> None:
        super().setUp()
        pymq.init(SimpleEventBus)

    def tearDown(self) -> None:
        super().tearDown()
        pymq.shutdown()


class SimpleRpcTest(unittest.TestCase, AbstractRpcTest):

    def setUp(self) -> None:
        super().setUp()
        pymq.init(SimpleEventBus)

    def tearDown(self) -> None:
        super().tearDown()
        pymq.shutdown()


class TestEventBus(unittest.TestCase):
    invocations = list()

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        pymq.init(SimpleEventBus)
        StatefulListener()
        init_listeners()

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

        time.sleep(0.25)
        self.assertEqual(2, len(self.invocations))  # should be from the other two listeners
        self.assertFalse(called.is_set())

    def test_publish_with_channel(self):
        pymq.publish('hello', channel='some/channel')
        self.assertEqual(2, len(self.invocations))
        self.assertIn(('some_event_listener', 'hello'), self.invocations)
        self.assertIn(('some_stateful_event_listener', 'hello'), self.invocations)


if __name__ == '__main__':
    unittest.main()
