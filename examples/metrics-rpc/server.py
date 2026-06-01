import time

from model import MetricReport

import pymq
from pymq.provider.redis import RedisConfig


class MetricsServer:
    def __init__(self):
        # Create a multi-stub to call all providers on the "metrics_provider" channel
        # We specify the return type hint via the model
        self.fetch_metrics = pymq.stub("metrics_provider", multi=True, timeout=2)

    def run(self):
        print("Metrics server is running. Pulling metrics every 2 seconds. Press Ctrl+C to stop.")
        try:
            while True:
                print("Pulling metrics...")
                # Calling the multi-stub returns a list of results from all available clients
                reports = self.fetch_metrics()
                
                if not reports:
                    print("No metrics received.")
                else:
                    for report in reports:
                        print(f"  - Received from {report.client_id}: {report.value:.2f} at {time.ctime(report.timestamp)}")
                
                time.sleep(2)
        except KeyboardInterrupt:
            pass

def main():
    # Initialize PyMQ with Redis backend
    pymq.init(RedisConfig())

    try:
        server = MetricsServer()
        server.run()
    finally:
        pymq.shutdown()

if __name__ == "__main__":
    main()
