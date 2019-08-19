import abc
import inspect
import logging
from collections import defaultdict
from typing import Dict, Callable, List, Tuple

from pymq.core import EventBus, Topic
from pymq.typing import fullname

logger = logging.getLogger(__name__)

def inspect_listener(fn) -> str:
    spec = inspect.getfullargspec(fn)

    if hasattr(fn, '__self__'):
        # method is bound to an object
        if len(spec.args) != 2:
            raise ValueError('Listener functions need exactly one arguments')

        if spec.args[1] not in spec.annotations:
            raise ValueError('Please annotate the event class with an appropriate type')

        event_type = spec.annotations[spec.args[1]]
        return fullname(event_type)

    else:
        if len(spec.args) != 1:
            raise ValueError('Listener functions need exactly one arguments')

        if spec.args[0] not in spec.annotations:
            raise ValueError('Please annotate the event class with an appropriate type')

        event_type = spec.annotations[spec.args[0]]
        return fullname(event_type)


class WrapperTopic(Topic):
    _bus: EventBus
    _name: str
    _is_pattern: bool

    def __init__(self, bus: EventBus, name, is_pattern) -> None:
        super().__init__()
        self._bus = bus
        self._name = name
        self._is_pattern = is_pattern

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_pattern(self) -> bool:
        return self._is_pattern

    def publish(self, event) -> int:
        if self.is_pattern:
            raise ValueError('Cannot publish to pattern topic')
        else:
            return self._bus.publish(event, self.name)

    def subscribe(self, callback):
        return self._bus.subscribe(callback, self.name, self.is_pattern)


class AbstractEventBus(EventBus, abc.ABC):
    _subscribers: Dict[Tuple[str, bool], List[Callable]]

    def __init__(self) -> None:
        super().__init__()
        self._subscribers = defaultdict(list)

    def topic(self, name: str, pattern: bool = False):
        return WrapperTopic(self, name, pattern)

    def publish(self, event, channel=None):
        if channel is None:
            channel = fullname(event)

        return self._publish(event, channel)

    def subscribe(self, callback, channel=None, pattern=False):
        if channel is None:
            channel = inspect_listener(callback)
            pattern = False

        logger.debug('adding to channel "%s" a callback %s', channel, callback)

        self._subscribers[(channel, pattern)].append(callback)
        self._subscribe(callback, channel, pattern)

    def unsubscribe(self, callback, channel, pattern=False):
        if channel is None:
            channel = inspect_listener(callback)
            pattern = False

        callbacks = self._subscribers.get((channel, pattern))

        if callback:
            callbacks.remove(callback)
            if len(callbacks) == 0:
                del self._subscribers[(channel, pattern)]

        self._unsubscribe(callback, channel, pattern)

    def _publish(self, event, channel: str):
        raise NotImplementedError

    def _subscribe(self, callback: Callable, channel: str, pattern: bool):
        raise NotImplementedError

    def _unsubscribe(self, callback: Callable, channel: str, pattern: bool):
        raise NotImplementedError
