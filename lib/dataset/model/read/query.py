from typing import List, Tuple, Dict, Union, Optional, Callable
from pandas import Timestamp
import pyarrow.dataset as ds
import re
from ..types import TimeInterval, TimestampLike, parse_timestamp
from ..period import PeriodLike, parse_period
from .result import Result

StrArgsOrList = Union[str, List[str]]

RE_WORD = r"[a-z][a-z0-9\-]*[a-z]"
RE_SOURCE = r"[a-z]\w*[a-z]"
RE_ASSET = rf"{RE_WORD}"
RE_EXCHANGE = rf"{RE_WORD}"
RE_INSTRUMENT = rf"{RE_ASSET}/{RE_ASSET}(?:/{RE_WORD})?"
SOURCE_REGEX = re.compile(rf"(?i){RE_SOURCE}")
EXCHANGE_REGEX = re.compile(rf"(?i){RE_EXCHANGE}")
INSTRUMENT_REGEX = re.compile(rf"(?i){RE_INSTRUMENT}")

SUBJECTS = set(["trades"])
COLUMNS = {
    "trades": [
        "time",
        "price",
        "amount",
        "side",
        "order",
        "extra_json",
        "year",
        "exchange",
        "instrument",
        "source",
    ]
}


def validate_subject(name: str):
    if name not in SUBJECTS:
        raise ValueError(f"Invalid subject: {repr(name)}")


def validate_column(subject: Optional[str], name: str):
    if not subject:
        return
    if name not in COLUMNS[subject]:
        raise ValueError(f"Invalid column: {repr(name)}")


def validate_source(symbol: str):
    if not SOURCE_REGEX.fullmatch(symbol):
        raise ValueError(f"Invalid source: {repr(symbol)}")


def validate_exchange(symbol: str):
    if not EXCHANGE_REGEX.fullmatch(symbol):
        raise ValueError(f"Invalid exchange: {repr(symbol)}")


def validate_instrument(symbol: str):
    if not INSTRUMENT_REGEX.fullmatch(symbol):
        raise ValueError(f"Invalid instrument: {repr(symbol)}")


def validate_market(symbol: str):
    parts = symbol.split(":")
    if len(parts) != 2:
        raise ValueError(
            f"Invalid market (use exchange:instrument format): {repr(symbol)}"
        )
    exchange, instrument = parts
    if exchange == "*" and instrument == "*":
        raise ValueError(f"Invalid market (matching everything): {repr(symbol)}")
    if exchange != "*":
        validate_exchange(exchange)
    if instrument != "*":
        validate_instrument(instrument)


class QueryBuilder:
    def __init__(self):
        self._subject: Optional[str] = None
        self._filters: Dict[str, List[str]] = {
            "columns": [],
            "sources": [],
            "exchanges": [],
            "instruments": [],
            "markets": [],
        }
        self._time: Dict[str, Optional[Timestamp]] = {
            "start": None,
            "end": None,
        }

    def build(self) -> "Query":
        exchanges = self._filters["exchanges"]
        instruments = self._filters["instruments"]
        if exchanges and instruments:
            markets = [
                f"{exchange}:{instrument}"
                for exchange in exchanges or ["*"]
                for instrument in instruments or ["*"]
            ]
        else:
            markets = []

        return Query(
            subject=self._subject,
            columns=self._filters["columns"],
            sources=self._filters["sources"],
            markets=markets + self._filters["markets"],
            start=self._time["start"],
            end=self._time["end"],
        )

    def run(self) -> Result:
        return self.build().run()

    def subject(self, name: str) -> "QueryBuilder":
        validate_subject(name)
        self._subject = str(name)
        return self._sort_and_validate_columns()

    def trades(self) -> "QueryBuilder":
        return self.subject("trades")

    def columns(self, *names: StrArgsOrList) -> "QueryBuilder":
        self._update("columns", names)
        return self._sort_and_validate_columns()

    def sources(self, *symbols: StrArgsOrList) -> "QueryBuilder":
        return self._update("sources", symbols, validate=validate_source)

    def exchanges(self, *symbols: StrArgsOrList) -> "QueryBuilder":
        self.reset("markets")
        return self._update("exchanges", symbols, validate=validate_exchange)

    def instruments(self, *symbols: StrArgsOrList) -> "QueryBuilder":
        self.reset("markets")
        return self._update("instruments", symbols, validate=validate_instrument)

    def markets(self, *symbols: StrArgsOrList) -> "QueryBuilder":
        self.reset("exchanges", "instruments")
        return self._update("markets", symbols, validate=validate_market)

    def start(self, time: TimestampLike) -> "QueryBuilder":
        return self._update_time("start", time)

    def end(self, time: TimestampLike) -> "QueryBuilder":
        return self._update_time("end", time)

    def period(self, value: PeriodLike) -> "QueryBuilder":
        interval = parse_period(value)
        self._update_time("start", interval.start)
        return self._update_time("end", interval.end)

    def reset(self, *keys: StrArgsOrList) -> "QueryBuilder":
        keys_list = self._args_or_list(keys)

        if len(keys_list) == 0:
            self._subject = None
            for key in self._filters.keys():
                self._filters[key] = []
            for key in self._time.keys():
                self._time[key] = None
            return self

        for key in keys_list:
            if key == "subject":
                self._subject = None
            elif key in ["time", "period"]:
                self._time["start"] = None
                self._time["end"] = None
            elif key in self._time:
                self._time[key] = None
            elif key in self._filters:
                self._filters[key] = []
            else:
                raise ValueError(f"reset: Invalid key: {key}")
        return self

    def _update(
        self,
        key: str,
        values: Tuple[StrArgsOrList, ...],
        validate: Optional[Callable] = None,
    ) -> "QueryBuilder":
        values_list = self._args_or_list(values)
        [validate(value) for value in values_list if validate]
        self._filters[key] = values_list
        return self

    def _update_time(self, key: str, value: TimestampLike) -> "QueryBuilder":
        self._time[key] = parse_timestamp(value)
        return self

    def _sort_and_validate_columns(self) -> "QueryBuilder":
        subject = self._subject or ""

        def sort_key(name: str):
            try:
                return COLUMNS[subject].index(name)
            except ValueError:
                raise ValueError(f"Invalid column: {repr(name)}") from None

        if subject:
            self._filters["columns"] = sorted(
                set(self._filters["columns"]),
                key=sort_key,
            )

        return self

    def _args_or_list(self, items: Tuple[StrArgsOrList, ...]) -> List[str]:
        if len(items) == 1 and isinstance(items[0], (list, tuple)):
            return list(items[0])
        return list(map(str, items))

    def __str__(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        return repr(self.build())


class Query:
    def __init__(
        self,
        subject: Optional[str] = None,
        columns: List[str] = [],
        sources: List[str] = [],
        markets: List[str] = [],
        start: Optional[Timestamp] = None,
        end: Optional[Timestamp] = None,
    ):
        self._subject = subject
        self._columns = columns
        self._sources = sources
        self._markets = markets
        self._start = start
        self._end = end

    def run(self) -> Result:
        if not self._subject:
            raise ValueError(f"Query: you must specify the subject")
        return Result(
            subject=self._subject,
            columns=self._columns,
            dataset_filter=self.dataset_filter(),
        )

    @property
    def subject(self) -> Optional[str]:
        return self._subject

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def sources(self) -> List[str]:
        return self._sources

    @property
    def markets(self) -> List[str]:
        return self._markets

    @property
    def start(self) -> Optional[Timestamp]:
        return self._start

    @property
    def end(self) -> Optional[Timestamp]:
        return self._end

    @property
    def interval(self) -> Optional[TimeInterval]:
        if self._start and self._end:
            return TimeInterval(self._start, self._end)
        return None

    def dataset_filter(self):
        source_filter = None
        for source in self._sources:
            f = ds.field("source") == source
            if source_filter:
                source_filter = source_filter | f
            else:
                source_filter = f

        market_filter = None
        for market in self._markets:
            exchange, instrument = market.split(":", 1)
            instrument = instrument.replace("/", "_")
            if instrument == "*":
                f = ds.field("exchange") == exchange
            elif exchange == "*":
                f = ds.field("instrument") == instrument
            else:
                f1 = ds.field("exchange") == exchange
                f2 = ds.field("instrument") == instrument
                f = f1 & f2

            if market_filter:
                market_filter = market_filter | f
            else:
                market_filter = f

        start_filter = None
        if self._start:
            f1 = ds.field("year") >= self._start.year
            f2 = ds.field("time") >= self._start.isoformat().replace("+00:00", "Z")
            start_filter = f1 & f2

        end_filter = None
        if self._end:
            f1 = ds.field("year") <= self._end.year
            f2 = ds.field("time") < self._end.isoformat().replace("+00:00", "Z")
            end_filter = f1 & f2

        combined_filter = None
        for f in [source_filter, market_filter, start_filter, end_filter]:
            if not f:
                continue
            if combined_filter:
                combined_filter = combined_filter & f
            else:
                combined_filter = f

        return combined_filter

    def __str__(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        lines = [
            self.subject and f"  subject: {self.subject}",
            self.columns and f"  columns: {', '.join(self.columns)}",
            self.sources and f"  sources: {', '.join(self.sources)}",
            self.markets and f"  markets: {', '.join(self.markets)}",
            self.interval and f"  interval: {self.interval}",
            not self.interval and self.start and f"  start: {self.start}",
            not self.interval and self.end and f"  end: {self.end}",
        ]
        lines = "\n".join([line for line in lines if line])
        return lines and f"<Query>:\n{lines}" or "<Query>: matches all"


if __name__ == "__main__":

    def test():
        result = (
            QueryBuilder()
            .trades()
            .columns("time", "price", "side")
            .markets("kraken:btc/eur")
            .run()
        )
        print(result.column_numpy("time"))
        print(result.column_numpy("price"))
        print(result.column_numpy("side"))

    test()
