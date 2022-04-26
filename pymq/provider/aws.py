import functools
import json
import logging
import math
import re
import threading
import time
import typing as t
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, lru_cache
from typing import NamedTuple

import boto3
from botocore.exceptions import ClientError
from botocore.utils import ArnParser

from pymq.core import Empty, QItem, Queue
from pymq.json import DeepDictDecoder, DeepDictEncoder
from pymq.provider.base import AbstractEventBus, WrapperTopic, invoke_function

if t.TYPE_CHECKING:
    from mypy_boto3_sns import SNSClient
    from mypy_boto3_sqs import SQSClient

logger = logging.getLogger(__name__)
_re_topic = re.compile("^[a-zA-Z0-9_\-]{1,256}$")


def short_uid() -> str:
    return str(uuid.uuid4())[:8]


def validate_topic_name(name: str):
    """
    Validates whether the given topic name is valid for SNS (matching the regex ``^[a-zA-Z0-9_\-]{1,256}$``).

    :param name: the proposed topic name
    :raises ValueError: if the topic name is invalid
    """
    if not _re_topic.match(name):
        raise ValueError(
            f"invalid topic name {name}: topic names must be made up of only uppercase and lowercase ASCII letters, "
            "numbers, underscores, and hyphens, and must be between 1 and 256 characters long"
        )


def encode_topic_name(original: str) -> str:
    """
    Encodes a channel name into a valid SNS topic name. Common characters in topics are '*', '/', '.', ':'. Those are
    translated into markers like '_WCD_'.

    :param original: the original topic name (e.g., 'my.program/foo')
    :return: encoded topic name safe for SNS (e.g., my_DOT_program_FWS_foo
    """
    encoded = original
    encoded = encoded.replace("*", "_WCD_")
    encoded = encoded.replace("/", "_FWS_")
    encoded = encoded.replace(".", "_DOT_")
    encoded = encoded.replace(":", "_COL_")
    return encoded


def decode_topic_name(encoded: str) -> str:
    """
    Reverses ``encode_topic_name``.

    :param encoded: the encoded SNS topic name
    :return: the decoded channel name
    """
    decoded = encoded
    decoded = decoded.replace("_WCD_", "*")
    decoded = decoded.replace("_FWS_", "/")
    decoded = decoded.replace("_DOT_", ".")
    decoded = decoded.replace("_COL_", ":")
    return decoded


def serialize(item: t.Any) -> str:
    """
    Uses DeepDictEncoder and json.dumps to serialize an object.

    :param item: the object to serialize
    :return: a string-encoded JSON document
    """
    return json.dumps(item, cls=DeepDictEncoder)


def deserialize(item: str) -> t.Any:
    """
    Uses DeepDictDecoder and json.loads to deserialize a JSON string.

    :param item: the string-encoded JSON
    :return: the object the JSON represents
    """
    return json.loads(item, cls=DeepDictDecoder)


class Event:
    """
    An event in the AwsEventBus event loop.
    """

    type: str


class CloseEvent(Event):
    """
    A close event.
    """

    type: str = "close"


class AwsConfig:
    """
    A EventBus factory that creates AwsEventBus instances from a boto session. The session requires full admin access
    to SQS and SNS.
    """

    session: boto3.Session

    def __init__(self, session: boto3.Session = None):
        self.session = session or boto3.Session()
        self.client = self.session.client

    def __call__(self):
        return AwsEventBus(
            sqs=self.client("sqs"),
            sns=self.client("sns"),
        )


class LocalstackConfig(AwsConfig):
    """
    A special case of AwsConfig for LocalStack.
    """

    def __init__(self, endpoint_url: str = None, region: str = None):
        """
        Creates a new AwsEventBus factory to run the event bus on LocalStack.

        :param endpoint_url: the endpoint url, defaults to ``http://localhost:4566``
        :param region: the region (default = None => us-east-1)
        """
        self.endpoint_url = endpoint_url or "http://localhost:4566"

        session = boto3.Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name=region,
        )
        super(LocalstackConfig, self).__init__(session)
        self.client = functools.partial(self.session.client, endpoint_url=self.endpoint_url)


class AwsQueue(Queue[QItem]):
    def __init__(self, sqs: "SQSClient", queue_url: str) -> None:
        self.sqs = sqs
        self._url = queue_url
        self._condition = threading.Condition()

    @property
    def url(self) -> str:
        return self._url

    @cached_property
    def arn(self):
        attributes = self.sqs.get_queue_attributes(QueueUrl=self.url, AttributeNames=["QueueArn"])
        return attributes["Attributes"]["QueueArn"]

    @cached_property
    def name(self):
        return self.url.split("/")[-1]

    def get(self, block: bool = True, timeout: float = None):
        # wait to receive the message
        kwargs = {
            "QueueUrl": self.url,
            "MaxNumberOfMessages": 1,
            "VisibilityTimeout": 20,
        }

        timeout = math.inf if timeout is None else timeout

        # block guard loop
        while True:
            then = time.time()
            if block:
                # you can wait at most 20 seconds with long polling, see:
                # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling
                wait = max(0.0, min(20.0, timeout))
                kwargs["WaitTimeSeconds"] = round(wait)

            message_response = self.sqs.receive_message(**kwargs)
            messages = message_response.get("Messages", [])

            # re-calculated timeout based on time already waited
            timeout = timeout - (time.time() - then)

            if not messages:
                if not block or timeout < 1:
                    raise Empty

                # we're blocking and there's still wait time left
                continue
            else:
                break

        message = messages[0]
        body = message["Body"]
        receipt = message["ReceiptHandle"]
        logger.debug("received message from queue %s: %s", self.name, message)

        # mark message as received
        self.sqs.delete_message(QueueUrl=self.url, ReceiptHandle=receipt)

        # then deserialize
        return self._deserialize(body)

    def put(self, item: QItem, block=False, timeout=None):
        if block:
            raise NotImplementedError
        # SQS queues are never full
        body = self._serialize(item)
        response = self.sqs.send_message(QueueUrl=self.url, MessageBody=body)
        logger.debug("sqs send message response: %s", response)

    def qsize(self) -> int:
        response = self.sqs.get_queue_attributes(
            QueueUrl=self.url, AttributeNames=["ApproximateNumberOfMessages"]
        )
        return int(response["Attributes"]["ApproximateNumberOfMessages"])

    def free(self):
        logger.debug("deleting queue %s", self.url)
        self.sqs.delete_queue(QueueUrl=self.url)

    def _serialize(self, item: QItem) -> str:
        return serialize(item)

    def _deserialize(self, item: str) -> QItem:
        return deserialize(item)


class AwsTopic(WrapperTopic):
    arn: str

    def __init__(self, bus: "AwsEventBus", arn: str) -> None:
        self.arn = arn
        super().__init__(bus, None, False)

    @cached_property
    def name(self) -> str:
        return decode_topic_name(ArnParser().parse_arn(self.arn)["resource"])


class Subscription(NamedTuple):
    """
    Internal representation of an SNS subscription and the associated callback.
    """

    channel: str
    pattern: bool
    callback: t.Callable
    subscription_arn: str


class AwsEventBus(AbstractEventBus):
    def __init__(
        self, namespace="global", sqs: "SQSClient" = None, sns: "SNSClient" = None
    ) -> None:
        super().__init__()
        self.namespace = namespace
        self.sqs = sqs or boto3.client("sqs")
        self.sns = sns or boto3.client("sns")

        self.id = f"pymq-{namespace}-{short_uid()}"

        self._closed = threading.Event()
        self._event_queue: t.Optional[AwsQueue[Event]] = None
        self._lock = threading.RLock()
        self._subscriptions: t.List[Subscription] = []
        self.dispatcher: t.Optional[ThreadPoolExecutor] = None

    def queue(self, name: str) -> AwsQueue:
        return AwsQueue(self.sqs, self._create_sqs_queue(name))

    @lru_cache()  # FIXME: need a strategy when topics are deleted
    def topic(self, name: str, pattern: bool = False) -> AwsTopic:
        if pattern:
            raise NotImplementedError("aws provider does not support pattern topics")

        return AwsTopic(self, self._create_sns_topic(name))

    def subscribe(self, callback, channel=None, pattern=False):
        with self._lock:
            super().subscribe(callback, channel, pattern)

    def unsubscribe(self, callback, channel=None, pattern=False):
        with self._lock:
            super().unsubscribe(callback, channel, pattern)

    def run(self):
        with self._lock:
            if not self.dispatcher:
                self.dispatcher = ThreadPoolExecutor(4)

            self._event_queue = self.queue(self.id)

        event_queue = self._event_queue

        try:
            logger.debug("starting AwsEventBus event loop on queue %s", event_queue.url)

            while not self._closed.is_set():
                try:
                    logger.debug("waiting for next event on %s", event_queue.arn)
                    event = event_queue.get()

                    logger.info("processing event %s", event)

                    if isinstance(event, Event):
                        if event.type == "close":
                            logger.info("received closed event %s", event)
                            return

                    if not isinstance(event, dict):
                        logger.warning("unknown event type %s", type(event))
                        continue

                    if event.get("Type") == "Notification":
                        self._dispatch_event(event)
                        continue

                    logger.warning("unhandled event %s", event)

                except Exception:
                    logger.exception("error processing event")
        finally:
            logger.debug("returning from event loop")
            with self._lock:
                event_queue.free()
                self._event_queue = None
            logger.info("exiting event loop")

    def close(self):
        with self._lock:
            if self._closed.is_set():
                return

            self._closed.set()

            self._unsubscribe_all()

            if self._event_queue:
                self._event_queue.put(CloseEvent())

            if self.dispatcher:
                self.dispatcher.shutdown()

    def wait_until_closed(self, timeout: t.Optional[float] = None):
        return self._closed.wait(timeout)

    def _get_subscribers_for_topic(self, topic_arn: str) -> t.List[t.Callable]:
        return self._subscribers[(AwsTopic(self, topic_arn).name, False)]

    def _dispatch_event(self, event: t.Dict[str, t.Any]):
        topic_arn = event.get("TopicArn")

        subscribers = self._get_subscribers_for_topic(topic_arn)

        if not subscribers:
            logger.debug("no subscribers for topic %s", topic_arn)
            return

        for fn in subscribers:
            self.dispatcher.submit(invoke_function, fn, event["Message"])

    def _create_sns_topic(self, name: str) -> str:
        encoded = encode_topic_name(name)
        validate_topic_name(encoded)

        logger.info("creating topic %s (encoded: %s)", name, encoded)
        response = self.sns.create_topic(Name=encoded)

        logger.debug("create topic response for %s: %s", encoded, response)
        return response["TopicArn"]

    def _create_sqs_queue(self, name: str) -> str:
        """
        Get or create an SQS Queue by name.

        :param name: the Queue name
        :return: the Queue URL
        """
        try:
            url = self.sqs.create_queue(QueueName=name)["QueueUrl"]
            logger.debug("created queue %s", url)
        except ClientError as e:
            if e.response["Error"]["Code"] == "QueueAlreadyExists":
                url = self.sqs.get_queue_url(QueueName=name)["QueueUrl"]
                logger.debug("re-using existing queue %s", url)
            else:
                raise

        return url

    def _publish(self, event: t.Any, channel: str) -> int:
        arn = self.topic(channel).arn

        response = self.sns.publish(Message=serialize(event), TopicArn=arn)
        logger.debug("publish response: %s", response)

        # can't determine number of recipients reliably
        return None

    def _subscribe(self, callback: t.Callable, channel: str, pattern: bool):
        if pattern:
            raise NotImplementedError("aws provider does not support pattern topics")

        topic_arn = self.topic(channel).arn
        queue_arn = self._event_queue.arn
        response = self.sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
        self._subscriptions.append(
            Subscription(channel, pattern, callback, response["SubscriptionArn"])
        )
        logger.debug("subscribed topic %s -> queue %s: %s", topic_arn, queue_arn, response)

    def _unsubscribe(self, callback: t.Callable, channel: str, pattern: bool):
        if pattern:
            raise NotImplementedError("aws provider does not support pattern topics")

        to_remove = list()

        for sub in self._subscriptions:
            if sub.channel == channel and sub.pattern == pattern and sub.callback == callback:
                to_remove.append(sub)

        for sub in to_remove:
            self.sns.unsubscribe(SubscriptionArn=sub.subscription_arn)
            self._subscriptions.remove(sub)

    def _unsubscribe_all(self):
        for sub in self._subscriptions:
            self.sns.unsubscribe(SubscriptionArn=sub.subscription_arn)

        self._subscriptions.clear()
