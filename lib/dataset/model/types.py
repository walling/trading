from typing import Union
from pandas import Timestamp, Interval

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


def timestamp(t: TimestampLike) -> Timestamp:
    """
    Convert values to timestamp. If the value is already a timestamp, it will be returned as-is.

    >>> t1 = Timestamp("2017-03-07")
    >>> timestamp(t1)
    Timestamp('2017-03-07 00:00:00')
    >>> timestamp(t1) == t1
    True
    >>> timestamp("2017-03-07")
    Timestamp('2017-03-07 00:00:00+0000', tz='UTC')
    >>> timestamp("2017-03Z")
    Timestamp('2017-03-01 00:00:00+0000', tz='UTC')
    >>> timestamp(1488844800000000000)
    Timestamp('2017-03-07 00:00:00+0000', tz='UTC')
    """

    if isinstance(t, Timestamp):
        return t
    if isinstance(t, str):
        return Timestamp(t.replace("Z", ""), tz="UTC")
    return Timestamp(t, tz="UTC")


def timestamp_format(t: Timestamp) -> str:
    """
    Displays a timestamp in a short ISO8601-compatible format by leaving out "null" elements.

    >>> timestamp_format(timestamp("2020-03-24 15:34:55"))
    '2020-03-24T15:34:55Z'
    >>> timestamp_format(timestamp("2020-03-24 15:34:00"))
    '2020-03-24T15:34Z'
    >>> timestamp_format(timestamp("2020-03-24 15:00:00"))
    '2020-03-24T15Z'
    >>> timestamp_format(timestamp("2020-03-24 00:00:00"))
    '2020-03-24Z'
    >>> timestamp_format(timestamp("2020-03-01 00:00:00"))
    '2020-03Z'
    >>> timestamp_format(timestamp("2020-01-01 00:00:00"))
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
        super().__init__(timestamp(start), timestamp(end), closed="left")

    @property
    def start(self) -> Timestamp:
        return self.left

    @property
    def end(self) -> Timestamp:
        return self.right

    def __str__(self) -> str:
        return f"{timestamp_format(self.left)}/{timestamp_format(self.right)}"

    def __repr__(self) -> str:
        closed = self.closed
        closed = "" if closed == "left" else ", closed=%r" % closed
        left = timestamp_format(self.left)
        right = timestamp_format(self.right)
        return "TimeInterval(%r, %r%s)" % (left, right, closed)
