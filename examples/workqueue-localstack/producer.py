import random
import time

from api import WorkItem, WorkResult

import pymq
from pymq import Queue
from pymq.provider.aws import LocalstackConfig


@pymq.subscriber
def print_result(event: WorkResult):
    print(f"<< {event}: {event.result}")


def main():
    pymq.init(LocalstackConfig())

    queue: Queue[WorkItem] = pymq.queue("work-items")
    try:
        while True:
            item = WorkItem(random.randint(1, 100), random.randint(1, 100))
            print(f">> {item} ({queue.qsize()} items queued)")
            queue.put(item)

            time.sleep(1.2)

    except KeyboardInterrupt:
        pass
    finally:
        queue.free()
        pymq.shutdown()


if __name__ == '__main__':
    main()
