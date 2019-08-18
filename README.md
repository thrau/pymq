Symmetry Eventbus
=================

Symmetry Eventbus is a message-oriented middleware library intended for enabling Python RPC over Redis using different
styles of remoting.
It provides an abstraction for Pub/Sub and Queues, and synchronous RPC over that abstraction.
It also provides a basic implementation over Redis. 


Using Eventbus
--------------

### Initialize a Redis eventbus

The default Redis implementation uses an event loop over a pubsub object. It is initialized via the python 

```python
import eventbus
from eventbus.provider.redis import RedisConfig

# starts a new thread with a Redis event loop
eventbus.init(RedisConfig())

# main application control loop

eventbus.shutdown()
```


### Pub/Sub

Subscriber code would look like this
```python
import eventbus

class MyEvent:
    pass

@eventbus.listener
def on_event(event: MyEvent):
    print('event received')
```

Publisher code would look like this
```python
import eventbus

eventbus.publish(MyEvent())
```

### Queues

Queues are straight forward, as they are compatible to the Python `queue.Queue` specification.

```python
import eventbus

queue = eventbus.queue('my_queue') 
queue.put('obj')
print(queue.get()) # outputs 'obj'
```

### RPC

Server code (suppose this is the module)

```python
import eventbus

@eventbus.remote('product_remote')
def product(a: int, b: int) -> int: # eventbus relies on type hints for marshalling
    return a * b
```

Client code
```python
import eventbus

result: 'List[eventbus.RpcResponse]' = eventbus.rpc('product_remote', 2, 4)
print(result[0].result) # 8
```

With a shared code-base methods can also be exposed and called by passing the callable. For example,
```python
import eventbus

# common code
class Remote:
    def my_remote(self) -> None:
        pass

# server
eventbus.expose(Remote().my_remote)

# client
eventbus.rpc(Remote.my_remote)

```

Known Limitations
-----------------

Quite a few. TODO: document ;-)

Background
----------

Originally part of the [Symmetry](https://git.dsg.tuwien.ac.at/mc2/symmetry) project, was extracted as a standalone
library.
