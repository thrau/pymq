# Metrics Example using Multi-RPC

This example demonstrates a pull-based metrics system using PyMQ's multi-RPC capabilities.
A central server pulls metrics from multiple available clients at once.

## Setup

This example uses Redis as backend.
Either start redis via the CLI if you have the `redis-server` installed, or you can use Docker:

```bash
docker run -p 6379:6379 redis
```

## Running the Example

### Run the Metrics Server

In one terminal, start the server:

```bash
python -m server
```

The server will poll all available clients every 2 seconds.

### Run Metrics Clients

In other terminals, start one or more clients. Each client needs a unique ID:

```bash
python -m client client-1
```

```bash
python -m client client-2
```

Each client exposes a remote function that the server calls to retrieve the current metric values.

## How it works

The server uses a **multi-stub** to call a remote function that is provided by multiple clients:

```python
fetch_metrics = pymq.stub("metrics_provider", multi=True, timeout=2)
results = fetch_metrics() # returns a list of results from all clients
```

Each client exposes its local method to the shared channel:

```python
pymq.init(RedisConfig())
pymq.expose(self.get_metrics, channel="metrics_provider")
```
