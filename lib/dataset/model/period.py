from typing import Union
from pandas import Timestamp, DateOffset
import re
from .types import parse_timestamp, TimeInterval

PeriodLike = Union[TimeInterval, str]

RE_YEAR = r"(?P<year>\d{4})"
RE_QUARTER = r"(?P<quarter>\d{1})"
RE_QUARTER_FULL = rf"{RE_YEAR}[Qq]{RE_QUARTER}"
RE_MONTH = r"(?P<month>\d{1,2})"
RE_MONTH_FULL = rf"{RE_YEAR}-{RE_MONTH}"
RE_DAY = r"(?P<day>\d{1,2})"
RE_DAY_FULL = rf"{RE_YEAR}-{RE_MONTH}-{RE_DAY}"

YEAR_REGEX = re.compile(RE_YEAR)
QUARTER_REGEX = re.compile(RE_QUARTER_FULL)
MONTH_REGEX = re.compile(RE_MONTH_FULL)
DAY_REGEX = re.compile(RE_DAY_FULL)


def parse_period_item(item: str, end_of=False):
    match = YEAR_REGEX.fullmatch(item)
    if match:
        year = parse_timestamp(item) + DateOffset(years=1) * int(end_of)
        return year

    match = QUARTER_REGEX.fullmatch(item)
    if match:
        quarter = parse_timestamp(item) + DateOffset(months=3) * int(end_of)
        return quarter

    match = MONTH_REGEX.fullmatch(item)
    if match:
        month = parse_timestamp(item) + DateOffset(months=1) * int(end_of)
        return month

    match = DAY_REGEX.fullmatch(item)
    if match:
        day = parse_timestamp(item) + DateOffset(days=1) * int(end_of)
        return day

    return None


def parse_period(period: PeriodLike) -> TimeInterval:
    if isinstance(period, TimeInterval):
        return period

    parts = period.split("/", 1)
    if len(parts) > 2:
        raise ValueError(f"Invalid format for period: {period}")

    if len(parts) == 2:
        start = parse_period_item(parts[0]) or parse_timestamp(parts[0])
        end = parse_period_item(parts[1], end_of=True) or parse_timestamp(parts[1])
    else:
        start = parse_period_item(period)
        end = parse_period_item(period, end_of=True)

    if not (start and end):
        raise ValueError(f"Unknown period format: {period}")

    return TimeInterval(start, end)


if __name__ == "__main__":
    print(parse_period("2019"))
    print(parse_period("2019-08-04/2019-08-07 15:00"))
