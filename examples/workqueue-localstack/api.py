import dataclasses


@dataclasses.dataclass
class WorkItem:
    a: int
    b: int


@dataclasses.dataclass
class WorkResult:
    worker: str
    result: int


class ShutdownEvent:
    pass
