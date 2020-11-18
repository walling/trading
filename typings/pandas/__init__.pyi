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
    def __init__(
        self,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
    ): ...
    def __eq__(self, other: Any) -> bool: ...
    def __gt__(self, other: Timedelta) -> bool: ...
    def __lt__(self, other: Timedelta) -> bool: ...
    def __ge__(self, other: Timedelta) -> bool: ...
    def __le__(self, other: Timedelta) -> bool: ...

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
    hour: int
    minute: int
    second: int
    microsecond: int
    nanosecond: int
    asm8: numpy.datetime64
    def __init__(
        self,
        ts_input: Optional[Union[int, str]] = None,
        tz: Optional[str] = None,
        unit: Optional[str] = None,
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
        hour: Optional[int] = None,
        minute: Optional[int] = None,
        second: Optional[int] = None,
        microsecond: Optional[int] = None,
        nanosecond: Optional[int] = None,
    ): ...
    def isoformat(self) -> str: ...
    @staticmethod
    def utcnow() -> Timestamp: ...
    def tzname(self) -> str: ...
    def strftime(self, format: str) -> str: ...
    def __eq__(self, other: Any) -> bool: ...
    def __gt__(self, other: Timestamp) -> bool: ...
    def __lt__(self, other: Timestamp) -> bool: ...
    def __ge__(self, other: Timestamp) -> bool: ...
    def __le__(self, other: Timestamp) -> bool: ...
    def __sub__(self, other: Timestamp) -> Timedelta: ...
    def __add__(self, other: Union[DateOffset, Timedelta]) -> Timestamp: ...
