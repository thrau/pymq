import threading
import time
from typing import NamedTuple

import pytest

import pymq


class Payload(NamedTuple):
    name: str
    value: int


class EventWithPayload:
    payload: Payload

    def __init__(self, payload: Payload) -> None:
        self.payload = payload


class TestQueue:
    def test_queue_name(self, bus):
        q = bus.queue("test_queue")

        assert "test_queue" == q.name

    @pytest.mark.timeout(2)
    def test_queue_put_get(self, bus):
        q = bus.queue("test_queue")
        q.put("elem1")
        q.put("elem2")

        assert "elem1" == q.get()
        assert "elem2" == q.get()

    @pytest.mark.timeout(2)
    def test_queue_get_blocking(self, bus):
        q = bus.queue("test_queue")
        event = threading.Event()

        def put():
            event.wait()
            q.put("a")

        t = threading.Thread(target=put)
        t.start()

        event.set()
        assert "a" == q.get(block=True)
        t.join()

    @pytest.mark.timeout(3)
    def test_queue_get_blocking_timeout(self, bus):
        q = bus.queue("test_queue")
        then = time.time()

        with pytest.raises(pymq.Empty):
            q.get(timeout=1)

        diff = time.time() - then
        assert diff == pytest.approx(1.0, 0.3)

    @pytest.mark.timeout(3)
    def test_queue_get_nowait_timeout(self, bus):
        q = bus.queue("test_queue")
        then = time.time()

        with pytest.raises(pymq.Empty):
            q.get_nowait()

        diff = time.time() - then
        assert diff < 0.3

    @pytest.mark.timeout(2)
    def test_queue_size(self, bus):
        q1 = bus.queue("test_queue_1")
        q2 = bus.queue("test_queue_2")
        assert 0 == q1.qsize()
        assert 0 == q2.qsize()

        q1.put("elem1")
        assert 1 == q1.qsize()
        assert 0 == q2.qsize()

        q1.put("elem2")
        assert 2 == q1.qsize()
        assert 0 == q2.qsize()

        q1.get()
        assert 1 == q1.qsize()
        assert 0 == q2.qsize()

        q1.get()
        assert 0 == q1.qsize()
        assert 0 == q2.qsize()

    @pytest.mark.timeout(2)
    def test_get_queue_size_on_same_queue(self, bus):
        q1 = bus.queue("same_test_queue")
        q2 = bus.queue("same_test_queue")

        assert 0 == q1.qsize()
        assert 0 == q2.qsize()

        q1.put("elem1")
        assert 1 == q1.qsize()
        assert 1 == q2.qsize()

        assert "elem1" == q2.get()
        assert 0 == q1.qsize()
        assert 0 == q2.qsize()

    def test_queue_primitive_types(self, bus):
        q = bus.queue("test_queue")

        q.put("abc")
        assert isinstance(q.get(), str)

        q.put(1)
        assert isinstance(q.get(), int)

        q.put(1.1)
        assert isinstance(q.get(), float)

    def test_queue_collection_types(self, bus):
        q = bus.queue("test_queue")

        q.put(("a", 1))
        v = q.get()
        assert isinstance(v, tuple)
        assert isinstance(v[0], str)
        assert isinstance(v[1], int)

        q.put([1, "v"])
        v = q.get()
        assert isinstance(v, list)
        assert isinstance(v[0], int)
        assert isinstance(v[1], str)

        q.put({"a": 1, "b": "c"})
        v = q.get()
        assert isinstance(v, dict)
        assert isinstance(v["a"], int)
        assert isinstance(v["b"], str)

    def test_queue_complex_types(self, bus):
        q = bus.queue("test_queue")

        q.put(EventWithPayload(Payload("foo", 42)))
        v = q.get()
        assert isinstance(v, EventWithPayload)
        assert isinstance(v.payload, Payload)
        assert isinstance(v.payload.name, str)
        assert isinstance(v.payload.value, int)
