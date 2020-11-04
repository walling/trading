from typing import Optional, Union, Protocol

class OrderableScalar(Protocol):
    def __lt__(self, other): ...

T = TypeVar("T", OrderableScalar)

class Interval(Generic[T]):
    left: T
    right: T
    closed: str
    def __init__(self, left: T, right: T, closed: str = "right"): ...

class Timestamp:
    def __init__(self, value: Union[int, str], tz: Optional[str] = None): ...
