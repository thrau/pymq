import time
import uuid

from api import WorkItem, WorkResult

import pymq
from pymq import Queue
from pymq.provider.aws import LocalstackConfig

worker = str(uuid.uuid4())[:8]


def do_work(item: WorkItem):
    print(f"worker {worker} doing hard calculations...")
    time.sleep(2)
    result = WorkResult(worker, item.a * item.b)
    print(f"worker {worker} publishing result")
    pymq.publish(result)


def main():
    pymq.init(LocalstackConfig())

    try:
        queue: Queue[WorkItem] = pymq.queue("work-items")
        print(f"worker {worker} listening on work queue...")
        while True:
            event = queue.get()
            do_work(event)

    except KeyboardInterrupt:
        pass
    finally:
        print(f"worker {worker} exiting")
        pymq.shutdown()


if __name__ == '__main__':
    main()
