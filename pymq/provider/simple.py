import logging
from queue import Queue as PythonQueue

from pymq.core import EventBus, Queue

logger = logging.getLogger(__name__)


class SimpleEventBus(EventBus):
    """
    This class illustrates the abstraction of the eventbus module and the role of an EventBus implementation: it hides
    the transport and acts as dispatcher.
    """

    def __init__(self) -> None:
        super().__init__()
        self.queues = dict()

    def publish(self, event, channel):
        # TODO: pattern matching
        key = (channel, False)

        subscribers = 0
        for fn in self.listeners[key]:
            logger.debug('dispatching %s to %s', event, fn)
            try:
                subscribers += 1
                fn(event)
            except Exception as e:
                logger.exception('error while executing callback', e)

        return subscribers

    def queue(self, name: str) -> Queue:
        if name not in self.queues:
            self.queues[name] = PythonQueue()
        return self.queues[name]

    def add_listener(self, callback, channel, pattern):
        pass

    def remove_listener(self, callback, channel, pattern):
        pass

    def run(self):
        pass

    def close(self):
        pass
