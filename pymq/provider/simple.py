import logging
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue as PythonQueue
from typing import Callable, Optional

from pymq.core import Queue
from pymq.provider.base import AbstractEventBus

logger = logging.getLogger(__name__)


class SimpleEventBus(AbstractEventBus):
    """
    This class illustrates the abstraction of the eventbus module and the role of an EventBus implementation: it hides
    the transport and acts as dispatcher.
    """

    def __init__(self) -> None:
        super().__init__()
        self.queues = dict()
        self.dispatcher: Optional[ThreadPoolExecutor] = None

    def run(self):
        self.dispatcher = ThreadPoolExecutor(max_workers=1)

    def close(self):
        if self.dispatcher:
            self.dispatcher.shutdown()

    def queue(self, name: str) -> Queue:
        # queues are never be garbage collected

        if name not in self.queues:
            q = PythonQueue()
            q.name = name
            self.queues[name] = q

        return self.queues[name]

    def _publish(self, event, channel: str):
        # TODO: pattern matching
        key = (channel, False)

        subscribers = 0
        for fn in self._subscribers[key]:
            logger.debug('dispatching %s to %s', event, fn)
            try:
                subscribers += 1
                self.dispatcher.submit(fn, event)
            except Exception as e:
                logger.exception('error while executing callback', e)

        return subscribers

    def _subscribe(self, callback: Callable, channel: str, pattern: bool):
        pass

    def _unsubscribe(self, callback: Callable, channel: str, pattern: bool):
        pass
