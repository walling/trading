import pyarrow as pa
from pandas import Timestamp
from decimal import Decimal
import krakenex
import threading
import time
import json

# -- Kraken API request timeout --
DEFAULT_TIMEOUT = 15
DEFAULT_WAIT_TIME = 3

# -- Fix asset symbols according to Cryptowatch API --
FIX_ASSET_SYMBOLS = {
    "xbt": "btc",
    "xdg": "doge",
}

# -- Trade taker sides --
TAKER_SIDES = {
    "b": "buy",
    "s": "sell",
}

# -- Trade order types --
ORDER_TYPES = {
    "m": "market",
    "l": "limit",
}

# -- Maximum number of decimals for time stored in a 64-bit float --
TIME_FLOAT_FORMAT = Decimal("0.00000")


def fix_asset(symbol):
    symbol = symbol.lower()
    return FIX_ASSET_SYMBOLS.get(symbol, symbol)


def fix_time(time_float):
    nanoseconds = int(Decimal(time_float).quantize(TIME_FLOAT_FORMAT).scaleb(9))
    return Timestamp(nanoseconds, tz="UTC")


class KrakenRESTSource:
    """
    Connects to Kraken REST API to download trading data.

    >>> KrakenRESTSource()
    KrakenRESTSource()
    """

    def __init__(self):
        self._client = krakenex.API()
        self._instruments = None
        self._instrument_assets = None
        self._query_time = None
        self._query_lock = threading.Lock()

    @property
    def source(self):
        return "kraken_rest"

    @property
    def exchanges(self):
        return ["kraken"]

    @property
    def assets(self):
        self._init_instruments()
        return sorted(self._instrument_assets or [])

    @property
    def instruments(self):
        self._init_instruments()
        return sorted(self._instruments.keys())

    @property
    def markets(self):
        return [f"kraken:{symbol}" for symbol in self.instruments]

    def trades(self, instrument: str, since=None):
        # TODO: pass market instead of instrument
        # TODO: support more complete query API instead of just since

        if isinstance(since, bytes):
            since = Timestamp(since.decode())
        elif isinstance(since, str):
            since = Timestamp(since)
        elif isinstance(since, int):
            since = Timestamp(since, tz="UTC")

        self._init_instruments()

        pair = self._instruments[instrument]
        result = self._query_trades(pair["name"], since)
        raw_trades = result[pair["name"]]

        price_scale = pair["pair_decimals"]
        amount_scale = pair["lot_decimals"]

        raw_price, raw_amount, raw_time, raw_side, raw_order, raw_misc = (
            zip(*raw_trades) if raw_trades else [[], [], [], [], [], []]
        )
        price = [int(Decimal(n).normalize().scaleb(price_scale)) for n in raw_price]
        amount = [int(Decimal(n).normalize().scaleb(amount_scale)) for n in raw_amount]
        time = [fix_time(n) for n in raw_time]
        side = [TAKER_SIDES.get(n) for n in raw_side]
        order = [ORDER_TYPES.get(n) for n in raw_order]

        extra_json = []
        for index, s, o in zip(range(len(raw_trades)), side, order):
            entry = {}
            if s == None:
                entry["side"] = raw_side[index]
            if o == None:
                entry["order"] = raw_order[index]
            if raw_misc[index]:
                entry["misc"] = raw_misc[index]

            if len(entry.keys()) > 0:
                extra_json.append(json.dumps({"kraken_rest": entry}))
            else:
                extra_json.append(None)

        # TODO: Extract the schema to a model, it's the same for all trades
        table = pa.Table.from_arrays(
            [
                pa.array(time, type=pa.timestamp("ns", tz="UTC")),
                pa.array(price, type=pa.uint64()),
                pa.array(amount, type=pa.uint64()),
                pa.array(side, type=pa.dictionary(pa.int8(), pa.string())),
                pa.array(order, type=pa.dictionary(pa.int8(), pa.string())),
                pa.array(extra_json, type=pa.string()),
            ],
            names=["time", "price", "amount", "side", "order", "extra_json"],
            metadata={
                "source": "kraken_rest",
                "exchange": "kraken",
                "assets": ",".join(map(str, sorted(set(instrument.split("/"))))),
                "instrument": instrument,
                "market": f"kraken:{instrument}",
                "pagination_next_since": Timestamp(
                    int(result["last"]), tz="UTC"
                ).isoformat(),
            },
        )

        return table

    def _init_instruments(self):
        if self._instruments:
            return

        self._instruments = {}
        self._instrument_assets = set()
        for name, data in self._query("AssetPairs").items():
            if "wsname" not in data:
                continue
            data["name"] = name
            base, quote = map(fix_asset, data["wsname"].split("/"))
            data["symbol"] = "/".join([base, quote])
            self._instruments[data["symbol"]] = data
            self._instrument_assets.add(base)
            self._instrument_assets.add(quote)

    def _query_trades(self, pair: str, since: Timestamp):
        return self._query(
            "Trades",
            {
                "pair": pair,
                "since": since and int(since.asm8) or 0,
            },
        )

    def _query(self, method: str, data=None):
        try:
            return self._query_raw(method, data)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            try:
                print("-- backoff attempt 1 --")
                return self._query_raw(method, data, 2)
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                print("-- backoff attempt 2 --")
                return self._query_raw(method, data, 4)

    def _query_raw(self, method: str, data, backoff_multiplier=1):
        with self._query_lock:
            try:
                if self._query_time:
                    elapsed_time = (
                        float((Timestamp.utcnow() - self._query_time).asm8) / 1e9
                    )
                    wait_time = max(0.0, DEFAULT_WAIT_TIME - elapsed_time)
                    time.sleep(
                        wait_time * backoff_multiplier + (backoff_multiplier - 1)
                    )

                result = self._client.query_public(
                    method, data=data, timeout=DEFAULT_TIMEOUT
                )
                if "error" in result and len(result["error"]) > 0:
                    raise Exception(f'KrakenRESTSource: {result["error"]}')
                return result["result"]
            finally:
                self._query_time = Timestamp.utcnow()

    def __str__(self):
        return "<KrakenRESTSource>"

    def __repr__(self):
        return "KrakenRESTSource()"
