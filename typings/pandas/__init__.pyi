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
    asm8: numpy.timedelta64

class DateOffset:
    def __init__(
        self,
        years: Optional[int] = None,
        months: Optional[int] = None,
        days: Optional[int] = None,
    ): ...
    def __mul__(self, other: int) -> DateOffset: ...

class Timestamp:
    year: int
    month: int
    day: int
    asm8: numpy.datetime64
    def __init__(self, value: Union[int, str], tz: Optional[str] = None): ...
    def isoformat(self) -> str: ...
    @staticmethod
    def utcnow() -> Timestamp: ...
    def __sub__(self, other: Timestamp) -> Timedelta: ...
    def __add__(self, other: DateOffset) -> Timestamp: ...
