import unittest

import pymq
from pymq.provider.simple import SimpleEventBus
from tests.base.pubsub import AbstractPubSubTest
from tests.base.queue import AbstractQueueTest
from tests.base.rpc import AbstractRpcTest


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


class SimplePubSubTest(unittest.TestCase, AbstractPubSubTest):

    def setUp(self) -> None:
        super().setUp()
        pymq.init(SimpleEventBus)

    def tearDown(self) -> None:
        super().tearDown()
        pymq.shutdown()

    def test_publish_pattern(self):
        # TODO: pattern matching not implemented for simple eventbus
        pass


if __name__ == '__main__':
    unittest.main()
