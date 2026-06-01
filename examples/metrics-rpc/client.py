import random
import sys
import time

from model import MetricReport

import pymq
from pymq.provider.redis import RedisConfig


class MetricsClient:
    def __init__(self, client_id: str):
        self.client_id = client_id

    def get_metrics(self) -> MetricReport:
        val = random.uniform(0, 100)
        print(f"Client {self.client_id} providing metric: {val:.2f}")
        return MetricReport(
            client_id=self.client_id,
            timestamp=time.time(),
            value=val
        )

    def run(self):
        # Expose the get_metrics method as a remote function
        # All clients expose the same "metrics_provider" channel
        pymq.expose(self.get_metrics, channel="metrics_provider")
        print(f"Metrics client {self.client_id} is running. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass

def main():
    if len(sys.argv) < 2:
        print("Usage: python client.py [client_id]")
        sys.exit(1)

    client_id = sys.argv[1]

    # Initialize PyMQ with Redis backend
    pymq.init(RedisConfig())

    try:
        client = MetricsClient(client_id)
        client.run()
    finally:
        pymq.shutdown()

if __name__ == "__main__":
    main()
