"""
A minimal in-process event bus provider used mainly for testing and examples.
"""

import logging
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Queue as PythonQueue
from typing import Any, Callable, Optional

from pymq.core import Queue
from pymq.provider.base import AbstractEventBus

logger = logging.getLogger(__name__)


class SimpleEventBus(AbstractEventBus):
    """
    This class illustrates the abstraction of the eventbus module and the role of an EventBus implementation: it hides
    the transport and acts as dispatcher.
    """

    def __init__(self) -> None:
        """
        Create a simple in-process event bus.
        """
        super().__init__()
        self.queues = dict()
        self.dispatcher: Optional[ThreadPoolExecutor] = None

    def run(self) -> None:
        """
        Start the dispatcher used to invoke subscribers.
        """
        self.dispatcher = ThreadPoolExecutor(max_workers=1)

    def close(self) -> None:
        """
        Shut down the dispatcher.
        """
        if self.dispatcher:
            self.dispatcher.shutdown()

    def queue(self, name: str) -> Queue:
        """
        Get (or create) an in-process queue by name.

        :param name: the name of the queue
        :return: the queue instance
        """
        # queues are never be garbage collected

        if name not in self.queues:
            q = PythonQueue()
            q.name = name
            self.queues[name] = q

        return self.queues[name]

    def _publish(self, event: Any, channel: str) -> int:
        """
        Dispatch an event to all subscribers of the given channel.

        :param event: the event to dispatch
        :param channel: the channel to dispatch on
        :return: the number of subscribers the event was dispatched to
        """
        # TODO: pattern matching
        key = (channel, False)

        subscribers = 0
        for fn in self._subscribers[key]:
            logger.debug("dispatching %s to %s", event, fn)
            try:
                subscribers += 1
                self.dispatcher.submit(fn, event)
            except Exception as e:
                logger.exception("error while executing callback", e)

        return subscribers

    def _subscribe(self, callback: Callable, channel: str, pattern: bool) -> None:
        """
        No-op: subscribers are tracked by the base class, no transport subscription is needed.
        """
        pass

    def _unsubscribe(self, callback: Callable, channel: str, pattern: bool) -> None:
        """
        No-op: subscribers are tracked by the base class, no transport subscription is needed.
        """
        pass
