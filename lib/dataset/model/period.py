from typing import Union
from .types import TimeInterval

PeriodLike = Union[TimeInterval, str]


def parse_period(period: PeriodLike) -> TimeInterval:
    raise RuntimeError("parse_period: Not implemented")  # todo
