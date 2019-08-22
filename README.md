PyMQ
====

[![PyPI Version](https://badge.fury.io/py/pymq.svg)](https://badge.fury.io/py/pymq)
[![PyPI License](https://img.shields.io/pypi/l/pymq.svg)](https://img.shields.io/pypi/l/pymq.svg)
[![Build Status](https://travis-ci.org/thrau/pymq.svg?branch=master)](https://travis-ci.org/thrau/pymq)
[![Coverage Status](https://coveralls.io/repos/github/thrau/pymq/badge.svg?branch=master)](https://coveralls.io/github/thrau/pymq?branch=master)

PyMQ is a simple message-oriented middleware library for implementing Python IPC across machine boundaries. The API
enables different styles of remoting via Pub/Sub, Queues, and synchronous RPC.

With PyMQ, developers can integrate Python applications running on different machines in a loosely coupled way over
existing transport mechanisms. PyMQ currently provides a Redis backend, and an in-memory backend for testing. The API is
extensible and other transports can be plugged in.

Using PyMQ
----------

### Initialize PyMQ

The core module manages a global eventbus instance that provides the remoting primitives. The default Redis
implementation uses an event loop over a pubsub object. The global eventbus is initialized via `pymq.init` and by
passing a provider factory.

```python
import pymq
from pymq.provider.redis import RedisConfig

# starts a new thread with a Redis event loop
pymq.init(RedisConfig())

# main application control loop

pymq.shutdown()
```
This will create an eventbus instance on top of a local Redis server.

### Pub/Sub

Pub/Sub allows asynchronous event-based communication. Event classes are used to transport state and identify channels.

```python
import pymq

# common code
class MyEvent:
    pass

# subscribe code
@pymq.subscriber
def on_event(event: MyEvent):
    print('event received')

# publisher code
pymq.publish(MyEvent())
```

### Queues

Queues are straight forward, as they are compatible to the Python `queue.Queue` specification.

```python
import pymq

queue = pymq.queue('my_queue') 
queue.put('obj')
print(queue.get()) # outputs 'obj'
```

### RPC

Server code

```python
import pymq

@pymq.remote('product_remote')
def product(a: int, b: int) -> int: # pymq relies on type hints for marshalling
    return a * b
```

Client code
```python
import pymq

product = pymq.stub('product_remote')
product(2, 4) # 8
```

With a shared code-base, methods can also be exposed and called by passing the callable. For example,
```python
import pymq

# common code
class Remote:
    def echo(self, param) -> None:
        return 'echo: ' + param

# server
obj = Remote()
pymq.expose(obj.echo)

# client
echo = pymq.stub(Remote.echo)
echo('pymq') # "echo: pymq"
```

If there are multiple providers of the same object, then a stub can be initialized with `multi=True` to get a list of
results. It may be useful to use a timeout in this case.

```python
remote = pymq.stub('remote_method', multi=True, timeout=2)

result = remote() # result will be a list containing the results of all invocations of available remote objects
```

Known Limitations
-----------------

* JSON serialization relies heavily on type hints. Sending complex types without type hints will cause type errors.
* There is currently no support for polymorphism with JSON serialization
* Pattern-based topic matching does not work for the in-memory eventbus
* Subscriptions by foreign components to RPC channels will cause issues in multi-call scenarios

Background
----------

Originally part of the [Symmetry](https://git.dsg.tuwien.ac.at/mc2/symmetry) project, was extracted as a standalone
library.
