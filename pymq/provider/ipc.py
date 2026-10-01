"""
Event bus provider built on POSIX message queues via the ``posix_ipc`` package.

This module implements the :class:`~pymq.core.EventBus` abstraction for inter-process
communication on a single machine. Pub/sub routing is realized through a shared file
tree of symlinks pointing at per-subscriber message queues, while RPC responses are
routed through temporary message queues.
"""

import inspect
import logging
import os
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, Callable, List, NamedTuple

import posix_ipc as ipc

import pymq.json as json
from pymq.core import Empty, Full, Queue, RpcRequest, RpcResponse
from pymq.provider.base import (
    AbstractEventBus,
    DefaultSkeletonMethod,
    DefaultStubMethod,
    invoke_function,
)

EVENT_PUBSUB = 0
EVENT_RPC_RESPONSE = 1

logger = logging.getLogger(__name__)


def _serialize(item: Any) -> str:
    """
    Serialize an item into a JSON string with type information.
    """
    return json.dumps(item, cls=json.DeepDictEncoder)


def _deserialize(item: str) -> Any:
    """
    Deserialize a JSON string produced by :func:`_serialize`.
    """
    return json.loads(item, cls=json.DeepDictDecoder)


class IpcEvent(NamedTuple):
    """
    A protocol message sent through the event loop queue.

    The ``event_type`` distinguishes pub/sub events from other internal messages, ``channel``
    is the target channel, and ``payload`` contains the serialized event.
    """

    event_type: int
    channel: str
    payload: str


class IpcQueue(Queue):
    """
    Wraps a posix_ipc MessageQueue as a Queue.
    """

    def __init__(self, name: str, mqname: str | None = None) -> None:
        """
        Create a queue wrapper.

        :param name: the logical name of the queue
        :param mqname: the underlying POSIX message queue name, defaults to ``/<name>``
        """
        super().__init__()
        self._name = name
        self._mqname = mqname or "/%s" % (name.lstrip("/"))
        self._mq: ipc.MessageQueue | None = None

    @property
    def name(self) -> str:
        """
        :return: the logical name of the queue
        """
        return self._name

    @property
    def mqname(self) -> str:
        """
        :return: the POSIX message queue name
        """
        return self._mqname

    def qsize(self) -> int:
        """
        :return: the number of messages currently in the queue
        """
        return self._get_mq().current_messages

    def get(self, block: bool = True, timeout: float | None = None) -> Any:
        """
        Receive an item from the message queue.

        :param block: if True, wait for an item to become available
        :param timeout: timeout in seconds
        :return: the received item
        :raises Empty: if the queue is empty or the timeout was reached
        """
        if not block:
            timeout = 0

        try:
            response = self._get_mq().receive(timeout=timeout)
        except ipc.BusyError:
            raise Empty

        if response is None:
            raise Empty

        msg, priority = response
        return _deserialize(msg)

    def put(self, item: Any, block: bool = True, timeout: float | None = None) -> None:
        """
        Send an item to the message queue.

        :param item: the item to send
        :param block: if True, wait until the queue has space
        :param timeout: timeout in seconds
        :raises Full: if the queue is full or the timeout was reached
        """
        if not block:
            timeout = 0

        data = _serialize(item)
        logger.debug("putting into %s the item %s as data %s", self._mqname, item, data)
        try:
            return self._get_mq().send(data, timeout=timeout)
        except ipc.BusyError:
            raise Full

    def exists(self) -> bool:
        """
        Check whether the underlying POSIX message queue exists.

        :return: True if the queue exists, False otherwise
        """
        return os.path.exists("/dev/mqueue%s" % self._mqname)

    def close(self) -> None:
        """
        Close the underlying message queue handle without unlinking it.
        """
        if self._mq is not None:
            self._mq.close()
            self._mq = None

    def free(self) -> None:
        """
        Unlink the underlying POSIX message queue.
        """
        logger.debug("unlinking queue %s", self._mqname)
        try:
            ipc.unlink_message_queue(self._mqname)
        except ipc.ExistentialError:
            logger.debug("queue %s did not exist", self._mqname)

    def _get_mq(self) -> ipc.MessageQueue:
        """
        :return: the underlying message queue, opening it lazily on first access
        """
        if not self._mq:
            self._open()
        return self._mq

    def _open(self) -> None:
        """
        Open the underlying POSIX message queue.
        """
        if self._mq is None:
            logger.debug("opening message queue %s", self._mqname)
            self._mq = ipc.MessageQueue(name=self._mqname, flags=ipc.O_CREAT)


class IpcStubMethod(DefaultStubMethod):
    """
    Special StubMethod implementation that unlinks the response queue once it's no longer needed.
    """

    def _finalize_response_queue(self, queue: IpcQueue) -> None:
        """
        Close and unlink the response queue once the stub is done with it.

        :param queue: the response queue to release
        """
        super()._finalize_response_queue(queue)
        queue.close()
        queue.free()


class IpcSkeletonMethod(DefaultSkeletonMethod):
    """
    Skeleton method for the IPC provider that verifies the response queue still exists before
    sending the response back. If the stub has already timed out and removed the queue, the
    response is dropped.
    """

    def _queue_response(self, request: RpcRequest, response: RpcResponse) -> None:
        """
        Send the response back to the caller, dropping it if the response queue no longer exists.

        :param request: the original request
        :param response: the response to send
        """
        queue = self._bus.queue(request.response_channel)
        try:
            if not queue.exists():
                # if the queue does not exist, it means that the stub method has deleted the queue because its timeout
                # was reached
                raise TimeoutError("Response channel timed out")

            queue.put(response)
        finally:
            queue.close()


class RoutingTable:
    """
    Uses a file tree to manage a pub/sub routing table for IpcEventBus.
    """

    bus: "IpcEventBus"
    ramdisk = "/run/shm"

    def __init__(self, bus: "IpcEventBus") -> None:
        """
        Create a routing table for the given event bus.

        :param bus: the event bus this routing table belongs to
        """
        super().__init__()
        self.subscriber_queue = bus.event_loop_name.lstrip("/")
        self.tree = os.path.join(self.ramdisk, bus.namespace, "subscribers")
        self.subscriptions: set[str] = set()

        # prepare tree
        os.makedirs(self.tree, exist_ok=True)

    def get_subscribers(self, channel: str) -> List[str]:
        """
        Get the subscriber queue names for the given channel.

        :param channel: the channel to look up
        :return: the names of the subscriber queues
        """
        topic_path = os.path.join(self.tree, channel)

        if not os.path.exists(topic_path):
            return []

        return [
            path.name
            for path in os.scandir(topic_path)
            if path.is_symlink() and path.is_file(follow_symlinks=True)
        ]

    def subscribe(self, channel: str) -> None:
        """
        Add a symlink for this bus' event loop queue to the channel's routing directory.

        :param channel: the channel to subscribe to
        """
        logger.debug("subscribing %s to %s", self.subscriber_queue, channel)

        self.subscriptions.add(channel)
        topic_path = os.path.join(self.tree, channel)
        mq_path = os.path.join("/dev/mqueue", self.subscriber_queue)

        logger.debug(f"mkdir -p {topic_path}")
        os.makedirs(topic_path, exist_ok=True)

        src = mq_path
        dst = os.path.join(topic_path, os.path.basename(mq_path))

        if os.path.exists(dst):
            return

        try:
            logger.debug(f"ln -s {src} {dst}")
            os.symlink(src, dst)
        except FileExistsError:
            logger.warning("Race condition on creating subscriber %s", src)

    def unsubscribe(self, channel: str) -> None:
        """
        Remove the symlink of this bus' event loop queue from the channel's routing directory.

        :param channel: the channel to unsubscribe from
        """
        # check if subscriber callbacks are empty for this topic, if so, remove the queue link
        logger.debug("unsubscribing from %s", channel)

        topic_path = os.path.join(self.tree, channel)
        link_path = os.path.join(topic_path, self.subscriber_queue)

        try:
            self.subscriptions.remove(channel)
        except KeyError:
            pass

        try:
            logger.debug("unlink %s", link_path)
            os.unlink(link_path)
        except FileNotFoundError:
            logger.debug("tried to unlink %s, but did not exist", link_path)

    def clear(self) -> None:
        """
        Remove all subscriptions from the routing table.
        """
        logger.debug("removing subscriptions")
        channels = list(self.subscriptions)
        for channel in channels:
            try:
                self.unsubscribe(channel)
            except Exception as e:
                logger.error("Error while unsubscribing from %s: %s", channel, e)


class IpcEventBus(AbstractEventBus):
    """
    Event bus implementation based on POSIX message queues.

    Each bus instance has its own event loop queue. Subscribers are tracked in a shared
    file tree of symlinks (see :class:`RoutingTable`), and publishing an event writes it
    to every subscriber queue linked to the channel.
    """

    POISON = "__STOP_EVENTBUS__"

    def __init__(
        self, namespace: str = "global", dispatcher: ThreadPoolExecutor | None = None
    ) -> None:
        """
        Create an IPC event bus.

        :param namespace: the namespace used to isolate message queue names
        :param dispatcher: optional thread pool used to dispatch events
        """
        super().__init__()
        self.namespace = namespace
        self._closed = False
        self.event_loop: IpcQueue | None = None
        self.dispatcher: ThreadPoolExecutor | None = dispatcher
        self.rtable = RoutingTable(self)

    @property
    def event_loop_name(self) -> str:
        """
        :return: the POSIX message queue name of this bus' event loop
        """
        return self.to_mqueue_name("$%d" % os.getpid())

    def to_mqueue_name(self, name: str) -> str:
        """
        Convert a logical queue name to a namespaced POSIX message queue name.

        :param name: the logical name
        :return: the POSIX message queue name
        """
        return "/pymq_%s_%s" % (self.namespace, name)

    def run(self) -> None:
        """
        Run the event loop. This blocks until :meth:`close` is called, reading messages
        from the event loop queue and dispatching pub/sub events to subscribers.
        """
        # TODO: locking

        if self.dispatcher is None:
            self.dispatcher = ThreadPoolExecutor(1)

        # prepare event loop queue, use pid as mq event loop name
        event_loop = IpcQueue(name="eventloop_%s" % self.namespace, mqname=self.event_loop_name)
        self.event_loop = event_loop

        try:
            while not self._closed:
                logger.debug("waiting for next event loop message")
                msg = event_loop.get()
                if msg == self.POISON:
                    logger.debug("event loop received poison, breaking loop")
                    break

                logger.debug("event loop got message %s", msg)
                event_type, channel, payload = msg

                if event_type is EVENT_PUBSUB:
                    logger.info("got pubsub event on channel %s", channel)
                    key = (channel, False)

                    if key not in self._subscribers:
                        logger.warning("inconsistent state: no listeners for %s", key)
                        continue

                    for fn in self._subscribers[key]:
                        logger.debug("dispatching %s to %s", payload, fn)
                        self.dispatcher.submit(IpcEventBus._call_listener, fn, payload)
                else:
                    logger.error("Unknown event type %s", event_type)

        finally:
            # TODO: cleanup
            event_loop.close()
            self._cleanup()

    def _cleanup(self) -> None:
        """
        Shut down the dispatcher and unlink the event loop queue and routing table links.
        """
        logger.debug("shutting down dispatcher")
        self.dispatcher.shutdown()

        logger.debug("unlinking event loop message queue")
        self.event_loop.free()

        logger.debug("clearing routing table")
        self.rtable.clear()

    def close(self) -> None:
        """
        Signal the event loop to stop by sending the poison message.
        """
        if self._closed:
            return

        self._closed = True
        if self.event_loop:
            self.event_loop.put(self.POISON)

    def queue(self, name: str) -> IpcQueue:
        """
        Get an IPC queue by name.

        :param name: the name of the queue
        :return: the queue instance
        """
        return IpcQueue(name, mqname=self.to_mqueue_name(name))

    def _publish(self, event: Any, channel: str) -> int:
        """
        Serialize an event and write it to every subscriber queue of the given channel.

        :param event: the event to publish
        :param channel: the channel to publish on
        :return: the number of subscriber queues the event was written to
        """
        # create a protocol message
        msg = IpcEvent(EVENT_PUBSUB, channel, _serialize(event))
        subscribers = self.rtable.get_subscribers(channel)

        if not subscribers:
            return 0

        queues = ["/" + subscriber for subscriber in subscribers]
        logger.debug("publishing event in %s into %s", channel, queues)
        for queue in queues:
            q = IpcQueue(queue)
            try:
                q.put(msg)
            finally:
                q.close()

        return len(queues)

    def _subscribe(self, callback: Callable, channel: str, pattern: bool) -> None:
        """
        Add this bus' event loop queue to the channel's routing table. Pattern subscriptions are not supported.
        """
        if pattern:
            raise NotImplementedError
        self.rtable.subscribe(channel)

    def _unsubscribe(self, callback: Callable, channel: str, pattern: bool) -> None:
        """
        Remove this bus' event loop queue from the channel's routing table. Pattern subscriptions are not supported.
        """
        if pattern:
            raise NotImplementedError
        self.rtable.unsubscribe(channel)

    @staticmethod
    def _call_listener(fn: Callable, data: str) -> None:
        """
        Dispatch a serialized event to a listener function.
        """
        invoke_function(fn, data)

    def _create_stub_method(
        self, channel: str, spec: inspect.FullArgSpec | None, timeout: float | None, multi: bool
    ) -> IpcStubMethod:
        """
        :return: an IPC-specific stub method that cleans up its response queue
        """
        return IpcStubMethod(self, channel, spec, timeout, multi)

    def _create_skeleton_method(self, channel: str, fn: Callable) -> Callable[[RpcRequest], None]:
        """
        :return: an IPC-specific skeleton method
        """
        return IpcSkeletonMethod(self, channel, fn)


class IpcConfig:
    """
    Configuration factory for the IPC event bus.

    Example::

        import pymq
        from pymq.provider.ipc import IpcConfig

        pymq.init(IpcConfig())
    """

    def __call__(self) -> IpcEventBus:
        """
        Create a new IPC event bus.

        :return: a new IPC event bus
        """
        return IpcEventBus()
