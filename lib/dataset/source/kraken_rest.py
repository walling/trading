import pyarrow as pa
from pandas import Timestamp
from decimal import Decimal
from typing import Optional
import random
import time
import json
import asyncio
from ..infrastructure.request import request_context, request_client
from ..model.series import TableSeries
from ..model.partition import Partition
from ..model.types import Market, TimeInterval

# -- Kraken API request timeout --
DEFAULT_TIMEOUT = 15
DEFAULT_WAIT_TIME = 3
API_VERSION = "0"
EXCHANGE_SYMBOL = "kraken"
SOURCE_SYMBOL = "kraken_rest"

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

    source = SOURCE_SYMBOL

    def __init__(self):
        self._client = None
        self._instruments = None
        self._instrument_assets = None

    async def exchanges(self):
        return [EXCHANGE_SYMBOL]

    async def assets(self):
        await self._init_instruments()
        return sorted(self._instrument_assets or [])

    async def instruments(self):
        await self._init_instruments()
        return sorted(self._instruments.keys())

    async def markets(self):
        return [Market(EXCHANGE_SYMBOL, symbol) for symbol in await self.instruments()]

    async def trades(
        self,
        market: str,
        since: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
        until_now: bool = True,
    ):
        # TODO: support more complete query API instead of just since

        start_time = time.time()

        while True:
            series = await self.trades_series(market, since)
            if len(series) > 0:
                since = series.partition.period.end
                yield series

            if until_now and len(series) == 0:
                break

            elapsed_time = time.time() - start_time
            if timeout and elapsed_time > timeout - DEFAULT_WAIT_TIME:
                break

    async def trades_series(self, market: str, since: Optional[Timestamp] = None):
        exchange_symbol, instrument = market.split(":", 1)
        if exchange_symbol != EXCHANGE_SYMBOL:
            raise ValueError(f"{SOURCE_SYMBOL}: market not supported: {market}")

        await self._init_instruments()

        pair = self._instruments[instrument]
        result = await self.request_trades(pair["name"], since)
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
                extra_json.append(json.dumps({SOURCE_SYMBOL: entry}))
            else:
                extra_json.append(None)

        pagination_next = Timestamp(int(result["last"]), tz="UTC")
        partition = Partition(
            source=SOURCE_SYMBOL,
            market=Market(EXCHANGE_SYMBOL, instrument),
            period=TimeInterval(since or time[0], pagination_next),
        )

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
        )

        return TableSeries(table, partition)

    async def _init_instruments(self):
        if self._instruments:
            return

        asset_pairs = await self.request("AssetPairs")
        instruments = {}
        instrument_assets = set()

        for name, data in asset_pairs.items():
            if "wsname" not in data:
                continue
            data["name"] = name
            base, quote = map(fix_asset, data["wsname"].split("/"))
            data["symbol"] = "/".join([base, quote])
            instruments[data["symbol"]] = data
            instrument_assets.add(base)
            instrument_assets.add(quote)

        self._instruments = instruments
        self._instrument_assets = instrument_assets

    async def request_trades(self, pair: str, since: Optional[Timestamp] = None):
        return await self.request(
            "Trades",
            pair=pair,
            since=int(since.asm8) if since else 0,
        )

    async def request(self, method: str, **data):
        return await self._request_backoff(method, data)

    # TODO: Refactor back-off algorithm to request module
    async def _request_backoff(self, method: str, data={}, tries=3, current_try=0):
        if not self._client:
            self._client = request_client(
                throttle_wait=DEFAULT_WAIT_TIME,
                timeout=DEFAULT_TIMEOUT,
            )

        try:
            return await self._request_single(method, data)
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            raise
        except:
            current_try += 1
            if current_try >= tries:
                raise

            # Back-off wait time and try again.
            wait_time = DEFAULT_WAIT_TIME * (random.random() + 2 ** current_try)
            await asyncio.sleep(wait_time)
            return await self.request(
                method=method,
                data=data,
                tries=tries,
                current_try=current_try,
            )

    async def _request_single(self, method: str, data={}):
        url = f"https://api.kraken.com/{API_VERSION}/public/{method}"
        response = await self._client.post(url, json=data)

        error = response["error"]
        if error:
            error_message = "; ".join(error) if isinstance(error, list) else str(error)
            raise RuntimeError(f"{SOURCE_SYMBOL}: {error_message}")

        return response["result"]

    def __str__(self):
        return "<KrakenRESTSource>"

    def __repr__(self):
        return "KrakenRESTSource()"


if __name__ == "__main__":

    async def test():
        async with request_context():
            kraken = KrakenRESTSource()
            since = Timestamp("2020-11-05 17:00", tz="UTC")
            async for t in kraken.trades("kraken:btc/eur", since=since):
                print(t)
            # print(await request("Time"))
            # print(await request("AssetPairs"))
            # print(await request("Trades", {"pair": "XXBTZEUR"}))

    asyncio.run(test())
