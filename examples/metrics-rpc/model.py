from typing import NamedTuple


class MetricReport(NamedTuple):
    client_id: str
    timestamp: float
    value: float
