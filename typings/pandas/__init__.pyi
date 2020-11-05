from typing import Optional, Union, Protocol
import numpy

class OrderableScalar(Protocol):
    def __lt__(self, other): ...

T = TypeVar("T", OrderableScalar)

class Interval(Generic[T]):
    left: T
    right: T
    closed: str
    def __init__(self, left: T, right: T, closed: str = "right"): ...

class Timedelta:
    @property
    def asm8(self) -> numpy.timedelta64: ...

class Timestamp:
    def __init__(self, value: Union[int, str], tz: Optional[str] = None): ...
    @property
    def asm8(self) -> numpy.datetime64: ...
    def isoformat(self) -> str: ...
    @staticmethod
    def utcnow() -> Timestamp: ...
    def __sub__(self, other: Timestamp) -> Timedelta: ...
