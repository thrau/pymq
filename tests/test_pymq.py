import unittest

import pymq


class PyMQTest(unittest.TestCase):
    def test_queue_on_non_started_bus(self):
        self.assertRaises(ValueError, pymq.queue, 'foo')

    def test_start_without_init(self):
        self.assertRaises(ValueError, pymq.core.start)
