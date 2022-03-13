import unittest

import pymq
from pymq.provider.ipc import IpcConfig, IpcEventBus, IpcQueue
from tests.base.pubsub import AbstractPubSubTest
from tests.base.queue import AbstractQueueTest
from tests.base.rpc import AbstractRpcTest


class IpcEventBusTestBase(unittest.TestCase):
    bus: IpcEventBus

    def setUp(self) -> None:
        self.bus = pymq.init(IpcConfig())

    def tearDown(self) -> None:
        pymq.shutdown()
        self.bus.close()


class IpcQueueTest(IpcEventBusTestBase, AbstractQueueTest):
    @classmethod
    def tearDownClass(cls) -> None:
        IpcQueue("pymq_global_test_queue").free()
        IpcQueue("pymq_global_test_queue_1").free()
        IpcQueue("pymq_global_test_queue_2").free()


class IpcRpcTest(IpcEventBusTestBase, AbstractRpcTest):
    def setUp(self) -> None:
        super().setUp()


class IpcPubSubTest(IpcEventBusTestBase, AbstractPubSubTest):
    def test_publish_pattern(self):
        pass


del IpcEventBusTestBase

if __name__ == "__main__":
    unittest.main()
