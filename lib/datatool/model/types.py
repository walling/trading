from dataclasses import dataclass
from typing import Protocol, Optional, Union
import pandas, re, datetime

SYMBOL_REGEX = re.compile(r"\A[a-z][a-z0-9]*(?:-[a-z][a-z0-9]*)*\Z")
SUBJECT_SYMBOLS = [
    "trades",
    "ohlc:1m",
    "ohlc:1h",
    "ohlc:1d",
    "book:full",
    "book:spread",
    "book:diff",
]


@dataclass(repr=False, order=True, frozen=True)
class Symbol:
    symbol: str

    def __post_init__(self):
        if not SYMBOL_REGEX.match(self.symbol):
            raise ValueError(f"invalid symbol: {repr(self.symbol)}")

    def __repr__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, self.symbol)

    def __str__(self) -> str:
        return self.symbol


@dataclass(repr=False, order=True, frozen=True)
class SubjectSymbol(Symbol):
    symbol: str

    def __post_init__(self):
        if self.symbol not in SUBJECT_SYMBOLS:
            raise ValueError(f"invalid subject symbol: {repr(self.symbol)}")

    def __repr__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, self.symbol)

    def __str__(self) -> str:
        return self.symbol


class AssetSymbol(Symbol):
    pass


class ExchangeSymbol(Symbol):
    pass


class SourceSymbol(Symbol):
    pass


class IsYear(Protocol):
    year: int


class IsMonth(Protocol):
    year: int
    month: int


class IsDay(Protocol):
    year: int
    month: int
    day: int


@dataclass(repr=False, order=True, frozen=True)
class Year:
    year: int

    def __post_init__(self):
        if not (1900 <= self.year <= 2100):
            raise ValueError(f"year out of range: {self.year}")

    @staticmethod
    def from_timestamp(timestamp: IsYear) -> "Year":
        return Year(timestamp.year)

    def as_timestamp(self) -> "Timestamp":
        return Timestamp.from_year(self)

    def isoformat(self) -> str:
        return "%04d" % self.year

    def __repr__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, self.year)

    def __str__(self) -> str:
        return self.isoformat()


@dataclass(repr=False, order=True, frozen=True)
class Month:
    year: int
    month: int

    def __post_init__(self):
        if not (1900 <= self.year <= 2100):
            raise ValueError(f"year out of range: {self.year}")
        datetime.date(self.year, self.month, 1)

    @staticmethod
    def from_timestamp(timestamp: IsMonth) -> "Month":
        return Month(timestamp.year, timestamp.month)

    def as_timestamp(self) -> "Timestamp":
        return Timestamp.from_month(self)

    def isoformat(self) -> str:
        return "%04d-%02d" % (self.year, self.month)

    def __repr__(self) -> str:
        return "%s(%r, %r)" % (self.__class__.__name__, self.year, self.month)

    def __str__(self) -> str:
        return self.isoformat()


@dataclass(repr=False, order=True, frozen=True)
class Day:
    year: int
    month: int
    day: int

    def __post_init__(self):
        if not (1900 <= self.year <= 2100):
            raise ValueError(f"year out of range: {self.year}")
        datetime.date(self.year, self.month, self.day)

    @staticmethod
    def from_timestamp(timestamp: IsDay) -> "Day":
        return Day(timestamp.year, timestamp.month, timestamp.day)

    def as_timestamp(self) -> "Timestamp":
        return Timestamp.from_day(self)

    def isoformat(self) -> str:
        return "%04d-%02d-%02d" % (self.year, self.month, self.day)

    def __repr__(self) -> str:
        return "%s(%r, %r, %r)" % (
            self.__class__.__name__,
            self.year,
            self.month,
            self.day,
        )

    def __str__(self) -> str:
        return self.isoformat()


@dataclass(repr=False, order=True, frozen=True)
class Timestamp:
    timestamp: pandas.Timestamp

    def __post_init__(self):
        if self.timestamp.tzname() != "UTC":
            raise ValueError(f"timestamp must be UTC: {repr(self.timestamp)}")

    @staticmethod
    def from_year(timestamp: IsYear) -> "Timestamp":
        return Timestamp(
            pandas.Timestamp(
                year=timestamp.year,
                month=1,
                day=1,
                tz="UTC",
            )
        )

    @staticmethod
    def from_month(timestamp: IsMonth) -> "Timestamp":
        return Timestamp(
            pandas.Timestamp(
                year=timestamp.year,
                month=timestamp.month,
                day=1,
                tz="UTC",
            )
        )

    @staticmethod
    def from_day(timestamp: IsDay) -> "Timestamp":
        return Timestamp(
            pandas.Timestamp(
                year=timestamp.year,
                month=timestamp.month,
                day=timestamp.day,
                tz="UTC",
            )
        )

    @property
    def year(self) -> int:
        return self.timestamp.year

    @property
    def month(self) -> int:
        return self.timestamp.month

    @property
    def day(self) -> int:
        return self.timestamp.day

    def isoformat(self) -> str:
        t = self.timestamp
        return t.strftime("%Y-%m-%dT%H:%M:%S.%f") + ("%03dZ" % t.nanosecond)

    def __repr__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, self.timestamp)

    def __str__(self) -> str:
        return self.isoformat()


@dataclass(repr=False, order=True, frozen=True)
class InstrumentSymbol:
    base: AssetSymbol
    quote: AssetSymbol
    extension: Optional[Symbol] = None

    @staticmethod
    def from_symbol(symbol: str) -> "InstrumentSymbol":
        parts = symbol.lower().split("/")
        if len(parts) == 2:
            return InstrumentSymbol(
                AssetSymbol(parts[0]),
                AssetSymbol(parts[1]),
            )
        elif len(parts) == 3:
            return InstrumentSymbol(
                AssetSymbol(parts[0]),
                AssetSymbol(parts[1]),
                Symbol(parts[2]),
            )
        else:
            raise ValueError(f"invalid instrument symbol: {repr(symbol)}")

    @property
    def symbol(self) -> str:
        if self.extension:
            return f"{self.base}/{self.quote}/{self.extension}"
        else:
            return f"{self.base}/{self.quote}"

    def __repr__(self) -> str:
        return "%s.from_symbol(%r)" % (self.__class__.__name__, self.symbol)

    def __str__(self) -> str:
        return self.symbol


@dataclass(repr=False, order=True, frozen=True)
class MarketSymbol:
    exchange: ExchangeSymbol
    instrument: InstrumentSymbol

    @staticmethod
    def from_symbol(symbol: str) -> "MarketSymbol":
        parts = symbol.split(":")
        if len(parts) == 2:
            return MarketSymbol(
                ExchangeSymbol(parts[0].lower()),
                InstrumentSymbol.from_symbol(parts[1]),
            )
        else:
            raise ValueError(f"invalid market symbol: {repr(symbol)}")

    @property
    def symbol(self) -> str:
        return f"{self.exchange}:{self.instrument}"

    def __repr__(self) -> str:
        return "%s.from_symbol(%r)" % (self.__class__.__name__, self.symbol)

    def __str__(self) -> str:
        return self.symbol


@dataclass(order=True, frozen=True)
class FileId:
    subject: SubjectSymbol
    source: SourceSymbol
    market: MarketSymbol
    time: Union[Year, Month, Day, Timestamp]
