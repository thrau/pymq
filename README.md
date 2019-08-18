PyMQ
====
PyMQ is a simple message-oriented middleware library for implementing Python IPC across machine boundaries. The API
enables different styles of remoting via Pub/Sub, Queues, and synchronous RPC.

With PyMQ, developers can integrate Python applications running on different machines in a loosely coupled way over
existing transport mechanisms. PyMQ currently provides a Redis backend.

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
@pymq.listener
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

Server code (suppose this is the module)

```python
import pymq

@pymq.remote('product_remote')
def product(a: int, b: int) -> int: # eventbus relies on type hints for marshalling
    return a * b
```

Client code
```python
import pymq

result: 'List[eventbus.RpcResponse]' = pymq.rpc('product_remote', 2, 4)
print(result[0].result) # 8
```

With a shared code-base methods can also be exposed and called by passing the callable. For example,
```python
import pymq

# common code
class Remote:
    def remote_fn(self) -> None:
        pass

# server
obj = Remote()
pymq.expose(obj.remote_fn)

# client
pymq.rpc(Remote.remote_fn)

```

Known Limitations
-----------------

Quite a few. TODO: document ;-)

Background
----------

Originally part of the [Symmetry](https://git.dsg.tuwien.ac.at/mc2/symmetry) project, was extracted as a standalone
library.
