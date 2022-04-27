# Producer/Worker pattern

This example implements a producer/worker application, where work items produced by `producer.py` are balanced between `worker.py` instances using a
[work queue](https://www.oreilly.com/library/view/designing-distributed-systems/9781491983638/ch10.html).
After completing their work, workers then publish the result into a result topic.

## Structure

* `api.py`: shared code with event classes
* `producer.py`: puts items in the work queue and subscribes to the result topic
* `worker.py`: gets items from the work queue and publishes results into the result topic

## Setup

The default provider in this example is [LocalStack](https://github.com/localstack/localstack) to emulate AWS.
To start the application, first run

    pip install localstack pymq[aws]

and then start up LocalStack using

    localstack start -d

## Start the producer

    python3 producer.py

You'll start seeing output that work items are being produced in regular intervals:

```
>> WorkItem(a=84, b=11) (0 items queued)
>> WorkItem(a=18, b=68) (1 items queued)
>> WorkItem(a=74, b=24) (2 items queued)
...
```

## Start a worker

In new terminal windows, start as many workers as you want

    python3 worker.py

You should see workers starting to pick work items from the queue

```
worker 53a76083 listening on work queue...
worker 53a76083 doing hard calculations...
worker 53a76083 publishing result
...
```

In the producer terminal, you should see work results being received:

```
<< WorkResult(worker='53a76083', result=2964): 2964
```

Start additional workers to empty the work queue faster.
