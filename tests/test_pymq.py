from typing import Callable

import pytest

import pymq
from pymq import EventBus, Queue, StubMethod


class MockedEventBus(EventBus):

    # TODO: replace with mocks

    published_events: list
    subscribers: list
    exposed_methods: list

    def __init__(self) -> None:
        super().__init__()
        self.published_events = list()
        self.subscribers = list()
        self.exposed_methods = list()

    def publish(self, event, channel=None):
        self.published_events.append((event, channel))

    def subscribe(self, callback: Callable, channel=None, pattern=False):
        self.subscribers.append((callback, channel, pattern))

    def unsubscribe(self, callback: Callable, channel=None, pattern=False):
        pass

    def queue(self, name: str) -> Queue:
        pass

    def topic(self, name: str, pattern: bool = False):
        pass

    def stub(self, fn, timeout=None, multi=False) -> StubMethod:
        pass

    def expose(self, fn, channel=None):
        self.exposed_methods.append((fn, channel))

    def unexpose(self, fn):
        pass

    def run(self):
        pass

    def close(self):
        pass


class TestPymqStub:
    def test_queue_on_non_started_bus(self):
        with pytest.raises(ValueError):
            pymq.queue("foo")

    def test_stub_on_non_started_bus(self):
        def fn():
            pass

        with pytest.raises(ValueError):
            pymq.stub(fn)

    def test_start_without_init(self):
        with pytest.raises(ValueError):
            pymq.core.start()

    def test_lazy_topic_publish(self):
        topic = pymq.topic("my_topic")

        assert "my_topic" == topic.name
        assert not topic.is_pattern
        assert topic.publish("does_nothing") is None

        bus: MockedEventBus = pymq.init(MockedEventBus)

        try:
            topic.publish("some_event")

            assert 1 == len(bus.published_events), "expected one event to be published"
            assert ("some_event", "my_topic") in bus.published_events
        finally:
            pymq.shutdown()

    def test_lazy_topic_subscribe(self):
        topic = pymq.topic("my_topic")

        def callback():
            pass

        assert topic.subscribe(callback) is None

        bus: MockedEventBus = pymq.init(MockedEventBus)

        try:
            assert (callback, "my_topic", False) in bus.subscribers
        finally:
            pymq.shutdown()

    def test_late_expose(self):
        def remote_fn():
            pass

        pymq.expose(remote_fn, channel="late_channel")

        bus: MockedEventBus = pymq.init(MockedEventBus)

        try:
            assert (remote_fn, "late_channel") in bus.exposed_methods
        finally:
            pymq.shutdown()
