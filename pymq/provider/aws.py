import functools
import json
import logging
import math
import threading
import time
import typing as t
import uuid
from functools import cached_property
from typing import Callable

import boto3
from botocore.exceptions import ClientError

from pymq.core import Empty, QItem, Queue
from pymq.json import DeepDictDecoder, DeepDictEncoder
from pymq.provider.base import AbstractEventBus

if t.TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient

logger = logging.getLogger(__name__)


class AwsConfig:
    session: boto3.Session

    def __init__(self, session: boto3.Session = None):
        self.session = session or boto3.Session()
        self.client = self.session.client

    def __call__(self):
        return AwsEventBus(
            sqs=self.client("sqs"),
        )


class LocalstackConfig(AwsConfig):
    def __init__(self, endpoint_url: str = None, region: str = None):
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

    def qsize(self) -> int:
        response = self.sqs.get_queue_attributes(
            QueueUrl=self.url, AttributeNames=["ApproximateNumberOfMessages"]
        )
        return int(response["Attributes"]["ApproximateNumberOfMessages"])

    def free(self):
        self.sqs.delete_queue(QueueUrl=self.url)

    def _serialize(self, item: QItem) -> str:
        return json.dumps(item, cls=DeepDictEncoder)

    def _deserialize(self, item: str) -> QItem:
        return json.loads(item, cls=DeepDictDecoder)


class AwsEventBus(AbstractEventBus):
    def __init__(self, namespace="global", sqs: "SQSClient" = None) -> None:
        super().__init__()
        self.namespace = namespace
        self.sqs = sqs or boto3.client("sqs")

        self.id = f"pymq-{namespace}-{str(uuid.uuid4())[8]}"

        self._closed = threading.Event()

    def run(self):
        event_queue: Queue = self.queue(self.id)
        try:
            self._closed.wait()
        finally:
            event_queue.free()

    def close(self):
        self._closed.set()

    def queue(self, name: str) -> Queue:
        # get or create queue
        try:
            url = self.sqs.create_queue(QueueName=name)["QueueUrl"]
            logger.debug("created queue %s", url)
        except ClientError as e:
            if e.response["Error"]["Code"] == "QueueAlreadyExists":
                url = self.sqs.get_queue_url(QueueName=name)["QueueUrl"]
                logger.debug("re-using existing queue %s", url)
            else:
                raise

        return AwsQueue(self.sqs, url)

    def _publish(self, event, channel: str) -> int:
        raise NotImplementedError

    def _subscribe(self, callback: Callable, channel: str, pattern: bool):
        raise NotImplementedError

    def _unsubscribe(self, callback: Callable, channel: str, pattern: bool):
        raise NotImplementedError
