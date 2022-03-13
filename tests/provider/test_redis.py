import threading
import time

import pytest

import pymq


class MyRedisEvent:
    pass


class TestRedisPubSub:
    def test_add_listener_creates_subscription_correctly(self, pymq_redis, redislite):
        def listener(event: MyRedisEvent):
            pass

        assert 0 == len(redislite.pubsub_channels()), (
            "expected no subscribers, but got %s" % redislite.pubsub_channels()
        )

        pymq_redis.subscribe(listener)

        channels = redislite.pubsub_channels()

        assert 1 == len(channels)
        # event names are encoded in the channels
        assert channels[0].endswith(".MyRedisEvent")

    def test_remove_listener_also_removes_redis_subscription(self, pymq_redis, redislite):
        def listener1(event: MyRedisEvent):
            pass

        def listener2(event: MyRedisEvent):
            pass

        pymq.subscribe(listener1)
        pymq.subscribe(listener2)

        assert 1 == len(redislite.pubsub_channels())
        pymq.unsubscribe(listener1)
        assert 1 == len(redislite.pubsub_channels())
        pymq.unsubscribe(listener2)
        assert 0 == len(redislite.pubsub_channels())


class TestRedisRpc:
    @pytest.mark.timeout(5)
    def test_channel_expire(self, pymq_redis, redislite):
        bus = pymq_redis.core._bus
        bus.rpc_channel_expire = 1
        called = threading.Event()

        def remotefn():
            time.sleep(1.25)
            called.set()

        pymq.expose(remotefn)

        stub = pymq.stub(remotefn, timeout=1)
        stub.rpc()
        keys = redislite.keys("*rpc*")
        assert 0 == len(keys), "Expected no rpc results yet %s" % keys

        called.wait()

        keys = redislite.keys("*rpc*")
        assert 1 == len(keys)

        # wait for key to expire
        time.sleep(1.25)

        keys = redislite.keys("*rpc*")
        assert 0 == len(keys), "key did not expire"
