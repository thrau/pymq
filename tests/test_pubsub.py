import queue
import threading
import time
from typing import NamedTuple

import pytest

import pymq
from pymq.provider.aws import AwsEventBus
from pymq.provider.ipc import IpcEventBus
from pymq.provider.simple import SimpleEventBus


class SimpleEvent:
    pass


class Payload(NamedTuple):
    name: str
    value: int


class EventWithPayload:
    payload: Payload

    def __init__(self, payload: Payload) -> None:
        self.payload = payload


class StatefulListener:
    invocations: queue.Queue

    def __init__(self) -> None:
        super().__init__()
        self.invocations = queue.Queue()

        pymq.subscribe(self.typed_stateful_event_listener)
        pymq.subscribe(self.stateful_event_listener, "some/channel")

    def typed_stateful_event_listener(self, event: SimpleEvent):
        self.invocations.put(("typed_stateful_event_listener", event))

    def stateful_event_listener(self, event):
        self.invocations.put(("stateful_event_listener", event))


# noinspection PyUnresolvedReferences
class TestPubSub:
    def test_topic(self, bus):
        invocations = queue.Queue()

        def topic_listener(value):
            invocations.put(value)

        bus.topic("some/topic").subscribe(topic_listener)
        bus.topic("some/topic").publish("hello")

        assert "hello" == invocations.get(timeout=2)

    def test_stateful_event_listener(self, bus):
        listener = StatefulListener()

        bus.publish("hello", channel="some/channel")

        title, e = listener.invocations.get(timeout=2)
        assert "stateful_event_listener" == title
        assert "hello" == e

        assert listener.invocations.empty()

    def test_typed_stateful_event_listener(self, bus):
        listener = StatefulListener()

        event = SimpleEvent()
        bus.publish(event)

        title, e = listener.invocations.get(timeout=2)
        assert "typed_stateful_event_listener" == title
        assert isinstance(e, SimpleEvent)

        assert listener.invocations.empty()

    def test_multiple_typed_listeners_are_both_called(self, bus):

        called1 = threading.Event()
        called2 = threading.Event()

        def listener1(event: SimpleEvent):
            called1.set()

        def listener2(event: SimpleEvent):
            called2.set()

        bus.subscribe(listener1)
        bus.subscribe(listener2)

        bus.publish(SimpleEvent())

        assert called1.wait(0.5), "listener1 was not called in time"
        assert called2.wait(0.5), "listener2 was not called in time"

    def test_remove_typed_listener_is_never_called(self, bus):
        called = threading.Event()

        def listener(event: SimpleEvent):
            called.set()

        bus.subscribe(listener)
        bus.unsubscribe(listener)
        bus.publish(SimpleEvent())

        time.sleep(0.2)
        assert not called.is_set()

    def test_unsubscribe_on_non_existing_listener_does_nothing(self, bus):
        def listener(event: SimpleEvent):
            pass

        bus.unsubscribe(listener)

    def test_publish_on_exposed_listener_with_channel(self, bus):
        invocations = queue.Queue()

        @pymq.subscriber("some/channel")
        def listener(event):
            invocations.put(event)

        bus.publish("hello", channel="some/channel")
        assert "hello" == invocations.get(timeout=1)
        assert invocations.empty()

    def test_publish_on_exposed_listener_with_type(self, bus):
        invocations = queue.Queue()

        @pymq.subscriber
        def listener(event: SimpleEvent):
            invocations.put(event)

        bus.publish(SimpleEvent())
        bus.publish(SimpleEvent())
        assert isinstance(invocations.get(timeout=1), SimpleEvent)
        assert isinstance(invocations.get(timeout=1), SimpleEvent)
        assert invocations.empty()

    def test_publish_on_same_channel(self, bus):
        invocations = queue.Queue()

        @pymq.subscriber("some/channel")
        def listener(event):
            invocations.put(event)

        bus.publish("hello1", channel="some/channel")
        bus.publish("hello2", channel="some/channel")
        bus.publish("hello3", channel="some/non-existing/channel")
        assert "hello1" == invocations.get(timeout=1)
        assert "hello2" == invocations.get(timeout=1)
        assert invocations.empty()

    def test_publish_on_channel_routes_correctly(self, bus):
        invocations1 = queue.Queue()
        invocations2 = queue.Queue()

        @pymq.subscriber("channel/1")
        def listener1(event):
            invocations1.put(event)

        @pymq.subscriber("channel/2")
        def listener(event):
            invocations2.put(event)

        bus.publish("event1", channel="channel/1")
        bus.publish("event2", channel="channel/2")

        assert "event1" == invocations1.get(timeout=1)
        assert "event2" == invocations2.get(timeout=1)
        assert invocations1.empty()
        assert invocations2.empty()

    def test_event_serialization(self, bus):
        called = threading.Event()

        def listener(event: EventWithPayload):
            called.payload = event.payload
            called.set()

        bus.subscribe(listener)
        bus.publish(EventWithPayload(Payload("foobar", 42)))

        assert called.wait(2)
        assert isinstance(called.payload, Payload)
        assert "foobar" == called.payload.name
        assert 42 == called.payload.value

    @pytest.mark.xfail_provider("init_ipc", "init_simple", "init_aws")
    def test_publish_pattern(self, bus):
        invocations = queue.Queue()

        @pymq.subscriber("channel/*", True)
        def listener1(event):
            invocations.put(event)

        bus.publish("event1", channel="channel/1")
        bus.publish("event2", channel="channel/2")

        assert "event1" == invocations.get(timeout=1)
        assert "event2" == invocations.get(timeout=1)
        assert invocations.empty()


class TestInitSubscribers:
    @pytest.mark.timeout(60)
    def test_subscribe_before_init(self, pymq_init):
        event = threading.Event()

        def subscriber(arg: str):
            event.set()

        pymq.subscribe(subscriber, "my_channel")
        pymq_init()
        pymq.publish("hello", "my_channel")

        assert event.wait(2)

    @pytest.mark.timeout(60)
    def test_publish_subscribe_before_init(self, pymq_init):
        invocations = queue.Queue()

        def handler(event: str):
            invocations.put(event)

        pymq.subscribe(handler, channel="early/subscription")
        pymq.publish("hello", channel="early/subscription")  # doesn't do anything
        pymq_init()
        pymq.publish("hello", channel="early/subscription")

        assert "hello" == invocations.get(timeout=1)
        assert 0 == invocations.qsize()

    @pytest.mark.timeout(60)
    def test_unsubscribe_before_init(self, pymq_init):
        invocations = queue.Queue()

        def handler(event: str):
            invocations.put(event)

        pymq.subscribe(handler, channel="early/subscription")
        pymq.unsubscribe(handler, channel="early/subscription")

        pymq_init()
        pymq.publish("hello", channel="early/subscription")
        with pytest.raises(queue.Empty):
            invocations.get(timeout=0.25)

    @pytest.mark.timeout(60)
    def test_subscribe_before_init_and_unsubscribe(self, pymq_init):
        event = threading.Event()

        def subscriber(arg):
            event.set()

        pymq.subscribe(subscriber, "my_channel")

        pymq_init()

        pymq.publish("hello", "my_channel")
        assert event.wait(2)
        event.clear()

        pymq.unsubscribe(subscriber, "my_channel")
        assert not event.wait(1)

        pymq.subscribe(subscriber, "my_channel")
        pymq.publish("hello", "my_channel")
        assert event.wait(2)
