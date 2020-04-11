import logging
import os
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Callable, NamedTuple

import posix_ipc as ipc

import pymq.json as json
from pymq.core import Queue, Empty, Full
from pymq.provider.base import AbstractEventBus, invoke_function, DefaultStubMethod, DefaultSkeletonMethod

EVENT_PUBSUB = 0
EVENT_RPC_RESPONSE = 1

logger = logging.getLogger(__name__)


class IpcEvent(NamedTuple):
    event_type: int
    channel: str
    payload: str


class IpcQueue(Queue):
    """
    Wraps a posix_ipc MessageQueue as a Queue.
    """

    def __init__(self, name: str, mqname: str = None) -> None:
        super().__init__()
        self._name = name
        self._mqname = mqname or '/%s' % (name.lstrip('/'))
        self._mq = None

    @property
    def name(self):
        return self._name

    @property
    def mqname(self):
        return self._mqname

    def qsize(self):
        return self._get_mq().current_messages

    def get(self, block=True, timeout=None):
        if not block:
            timeout = 0

        try:
            response = self._get_mq().receive(timeout=timeout)
        except ipc.BusyError:
            raise Empty

        if response is None:
            raise Empty

        msg, priority = response
        return self._deserialize(msg)

    def put(self, item, block=True, timeout=None):
        if not block:
            timeout = 0

        data = self._serialize(item)
        logger.debug('putting into %s the item %s as data %s', self._mqname, item, data)
        try:
            return self._get_mq().send(data, timeout=timeout)
        except ipc.BusyError:
            raise Full

    def exists(self):
        return os.path.exists('/dev/mqueue%s' % self._mqname)

    def close(self):
        if self._mq is not None:
            self._mq.close()
            self._mq = None

    def free(self):
        logger.debug('unlinking queue %s', self._mqname)
        try:
            ipc.unlink_message_queue(self._mqname)
        except ipc.ExistentialError:
            logger.debug('queue %s did not exist', self._mqname)

    def _get_mq(self):
        if not self._mq:
            self._open()
        return self._mq

    def _open(self):
        if self._mq is None:
            logger.debug('opening message queue %s', self._mqname)
            self._mq = ipc.MessageQueue(name=self._mqname, flags=ipc.O_CREAT)

    def _serialize(self, item):
        return json.dumps(item, cls=json.DeepDictEncoder)

    def _deserialize(self, item):
        return json.loads(item, cls=json.DeepDictDecoder)


class IpcStubMethod(DefaultStubMethod):
    """
    Special StubMethod implementation that unlink the response queue once it's no longer needed.
    """

    def _finalize_response_queue(self, queue: IpcQueue):
        super()._finalize_response_queue(queue)
        queue.close()
        queue.free()


class IpcSkeletonMethod(DefaultSkeletonMethod):

    def _queue_response(self, request, response):
        queue = self._bus.queue(request.response_channel)
        try:
            if not queue.exists():
                # if the queue does not exist, it means that the stub method has deleted the queue because its timeout
                # was reached
                raise TimeoutError("Response channel timed out")

            queue.put(response)
        finally:
            queue.close()


class IpcEventBus(AbstractEventBus):
    POISON = '__STOP_EVENTBUS__'

    ramdisk = '/run/shm'

    def __init__(self, namespace='global', dispatcher=None) -> None:
        super().__init__()
        self.namespace = namespace
        self._closed = False
        self.event_loop = None
        self.dispatcher: ThreadPoolExecutor = dispatcher

        self._tmpdir = os.path.join(self.ramdisk, 'pymq', self.namespace)

    @property
    def _event_loop_name(self):
        return self._get_mqueue_name('$%d' % os.getpid())

    def _get_mqueue_name(self, name):
        return self.create_mqueue_name(self.namespace, name)

    @staticmethod
    def create_mqueue_name(namespace, name):
        return '/pymq_%s_%s' % (namespace, name)

    def run(self):
        # TODO: locking

        if self.dispatcher is None:
            self.dispatcher = ThreadPoolExecutor(1)

        # prepare pubsub engine
        os.makedirs(self._tmpdir, exist_ok=True)

        # prepare event loop queue, use pid as mq event loop name
        event_loop = IpcQueue(name='eventloop_%s' % self.namespace, mqname=self._event_loop_name)
        self.event_loop = event_loop

        try:
            while not self._closed:
                logger.debug('waiting for next event loop message')
                msg = event_loop.get()
                if msg == self.POISON:
                    logger.debug('event loop received poison, breaking loop')
                    break

                logger.debug('event loop got message %s', msg)
                event_type, channel, payload = msg

                if event_type is EVENT_PUBSUB:
                    logger.info('got pubsub event on channel %s', channel)
                    key = (channel, False)

                    if key not in self._subscribers:
                        logger.warning('inconsistent state: no listeners for %s', key)
                        continue

                    for fn in self._subscribers[key]:
                        logger.debug('dispatching %s to %s', payload, fn)
                        self.dispatcher.submit(IpcEventBus._call_listener, fn, payload)
                else:
                    logger.error("Unknown event type %s", event_type)

        finally:
            # TODO: cleanup
            event_loop.close()
            self._cleanup()

    def _cleanup(self):
        logger.debug('shutting down dispatcher')
        self.dispatcher.shutdown()

        logger.debug('unlinking event loop message queue')
        self.event_loop.free()

        logger.debug('removing subscriptions')
        for t, pattern in self._subscribers:
            topic_path = os.path.join(self._tmpdir, 'subscribers', t)
            link_path = os.path.join(topic_path, self._event_loop_name.lstrip('/'))

            logger.debug('unlink %s', link_path)
            os.unlink(link_path)

    def close(self):
        if self._closed:
            return

        self._closed = True
        if self.event_loop:
            self.event_loop.put(self.POISON)

    def queue(self, name: str) -> IpcQueue:
        return IpcQueue(name, mqname=self._get_mqueue_name(name))

    def _publish(self, event, channel: str) -> int:
        # create a protocol message
        msg = IpcEvent(EVENT_PUBSUB, channel, self._serialize(event))

        topic_path = os.path.join(self._tmpdir, 'subscribers', channel)
        if not os.path.exists(topic_path):
            return 0

        queues = ['/' + path.name for path in os.scandir(topic_path) if path.is_symlink()]

        logger.debug('publishing event in %s into %s', channel, queues)

        for queue in queues:
            q = IpcQueue(queue)
            try:
                q.put(msg)
            finally:
                q.close()

        return len(queues)

    def _subscribe(self, callback: Callable, channel: str, pattern: bool):
        if pattern:
            raise NotImplementedError

        logger.debug('subscribing to %s', channel)

        topic_path = os.path.join(self._tmpdir, 'subscribers', channel)
        mq_path = os.path.join('/dev/mqueue', self._event_loop_name.lstrip('/'))

        logger.debug(f'mkdir -p {topic_path}')
        os.makedirs(topic_path, exist_ok=True)

        src = mq_path
        dst = os.path.join(topic_path, os.path.basename(mq_path))

        if os.path.exists(dst):
            return

        try:
            logger.debug(f'ln -s {src} {dst}')
            os.symlink(src, dst)
        except FileExistsError:
            logger.warning('Race condition on creating subscriber %s', src)

    def _unsubscribe(self, callback: Callable, channel: str, pattern: bool):
        if pattern:
            raise NotImplementedError

        # check if subscriber callbacks are empty for this topic, if so, remove the queue link
        logger.debug('unsubscribing from %s', channel)

        topic_path = os.path.join(self._tmpdir, 'subscribers', channel)
        link_path = os.path.join(topic_path, self._event_loop_name.lstrip('/'))

        if os.path.exists(link_path):
            logger.debug('unlink %s', link_path)
            os.unlink(link_path)

    def _serialize(self, item):
        return json.dumps(item, cls=json.DeepDictEncoder)

    def _deserialize(self, item):
        return json.loads(item, cls=json.DeepDictDecoder)

    @staticmethod
    def _call_listener(fn, data: str):
        invoke_function(fn, data)

    def _create_stub_method(self, channel, spec, timeout, multi):
        return IpcStubMethod(self, channel, spec, timeout, multi)

    def _create_skeleton_method(self, channel, fn):
        return IpcSkeletonMethod(self, channel, fn)


class IpcConfig:
    def __call__(self):
        return IpcEventBus()
