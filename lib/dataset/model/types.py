from typing import Union
from pandas import Timestamp, DateOffset, Interval

TimestampLike = Union[Timestamp, str, int]


class Market:
    """
    Represents a specific market, i.e. a given instrument traded on a given exchange.

    >>> btc_eur = Market('kraken', 'btc/eur')
    >>> btc_eur
    Market('kraken', 'btc/eur')
    >>> str(btc_eur)
    'kraken:btc/eur'
    >>> btc_eur.symbol
    'kraken:btc/eur'
    >>> btc_eur.exchange
    'kraken'
    >>> btc_eur.instrument
    'btc/eur'
    """

    def __init__(self, exchange: str, instrument: str):
        self._exchange = exchange
        self._instrument = instrument

    @property
    def symbol(self) -> str:
        return f"{self._exchange}:{self._instrument}"

    @property
    def exchange(self) -> str:
        return self._exchange

    @property
    def instrument(self) -> str:
        return self._instrument

    def __str__(self) -> str:
        return self.symbol

    def __repr__(self) -> str:
        return "Market(%r, %r)" % (self._exchange, self._instrument)


def parse_timestamp(t: TimestampLike) -> Timestamp:
    """
    Convert values to timestamp. If the value is already a timestamp, it will be returned as-is.

    >>> t1 = Timestamp("2017-03-07")
    >>> parse_timestamp(t1)
    Timestamp('2017-03-07 00:00:00')
    >>> parse_timestamp(t1) == t1
    True
    >>> parse_timestamp("2017-03-07")
    Timestamp('2017-03-07 00:00:00+0000', tz='UTC')
    >>> parse_timestamp("2017-03Z")
    Timestamp('2017-03-01 00:00:00+0000', tz='UTC')
    >>> parse_timestamp(1488844800000000000)
    Timestamp('2017-03-07 00:00:00+0000', tz='UTC')
    """

    if isinstance(t, Timestamp):
        return t
    if isinstance(t, str):
        return Timestamp(t.replace("Z", ""), tz="UTC")
    return Timestamp(t, tz="UTC")


def format_timestamp(t: Timestamp) -> str:
    """
    Displays a timestamp in a short ISO8601-compatible format by leaving out "null" elements.

    >>> format_timestamp(parse_timestamp("2020-03-24 15:34:55"))
    '2020-03-24T15:34:55Z'
    >>> format_timestamp(parse_timestamp("2020-03-24 15:34:00"))
    '2020-03-24T15:34Z'
    >>> format_timestamp(parse_timestamp("2020-03-24 15:00:00"))
    '2020-03-24T15Z'
    >>> format_timestamp(parse_timestamp("2020-03-24 00:00:00"))
    '2020-03-24Z'
    >>> format_timestamp(parse_timestamp("2020-03-01 00:00:00"))
    '2020-03Z'
    >>> format_timestamp(parse_timestamp("2020-01-01 00:00:00"))
    '2020Z'
    """

    return (
        t.isoformat()
        .replace("+00:00", "Z")
        .replace(":00Z", "Z")
        .replace(":00Z", "Z")
        .replace("T00Z", "Z")
        .replace("-01Z", "Z")
        .replace("-01Z", "Z")
    )


def format_timestamp_full(t: Timestamp) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%S.%f") + ("%03d" % t.nanosecond) + "Z"


class TimeInterval(Interval):
    """
    Represents an open time interval [start; end).
    The start is inclusive, the end is exclusive.

    >>> TimeInterval(Timestamp("2019-08-20"), Timestamp("2019-11-03"))
    TimeInterval('2019-08-20T00:00:00', '2019-11-03T00:00:00')
    >>> august = TimeInterval("2019-08-01", "2019-09-01")
    >>> august
    TimeInterval('2019-08Z', '2019-09Z')
    >>> august == TimeInterval('2019-08Z', '2019-09Z')
    True
    >>> str(august)
    '2019-08Z/2019-09Z'
    >>> august.start
    Timestamp('2019-08-01 00:00:00+0000', tz='UTC')
    >>> august.start.asm8
    numpy.datetime64('2019-08-01T00:00:00.000000000')
    >>> august.end
    Timestamp('2019-09-01 00:00:00+0000', tz='UTC')
    >>> august.end.asm8
    numpy.datetime64('2019-09-01T00:00:00.000000000')
    >>> august.closed
    'left'
    """

    def __init__(self, start: TimestampLike, end: TimestampLike):
        super().__init__(parse_timestamp(start), parse_timestamp(end), closed="left")

    @property
    def start(self) -> Timestamp:
        return self.left

    @property
    def end(self) -> Timestamp:
        return self.right

    def with_start(self, start: Timestamp) -> "TimeInterval":
        return TimeInterval(start, self.end)

    def with_end(self, end: Timestamp) -> "TimeInterval":
        return TimeInterval(self.start, end)

    def format(self, short=False) -> str:
        if short:
            start = format_timestamp(self.start)
            end = format_timestamp(self.end)

            if len(start) == 5 and self.end == self.start + DateOffset(years=1):
                return start[0:4]

            if len(start) <= 8 and self.end == self.start + DateOffset(months=1):
                return start[0:7]

            if len(start) <= 11 and self.end == self.start + DateOffset(days=1):
                return start[0:10]

        start = format_timestamp_full(self.start)
        end = format_timestamp_full(self.end)
        return "/".join([start, end])

    @property
    def is_year(self):
        start = format_timestamp(self.start)
        end = format_timestamp(self.end)

        return len(start) == 5 and self.end == self.start + DateOffset(years=1)

    @property
    def is_month(self):
        start = format_timestamp(self.start)
        end = format_timestamp(self.end)

        return len(start) <= 8 and self.end == self.start + DateOffset(months=1)

    @property
    def is_day(self):
        start = format_timestamp(self.start)
        end = format_timestamp(self.end)

        return len(start) <= 11 and self.end == self.start + DateOffset(days=1)

    def __str__(self) -> str:
        return f"{format_timestamp(self.left)}/{format_timestamp(self.right)}"

    def __repr__(self) -> str:
        closed = self.closed
        closed = "" if closed == "left" else ", closed=%r" % closed
        left = format_timestamp(self.left)
        right = format_timestamp(self.right)
        return "TimeInterval(%r, %r%s)" % (left, right, closed)
