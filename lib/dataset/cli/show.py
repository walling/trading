from ..infrastructure.request import request_context
from ..source import source_instance
from ..model.types import parse_timestamp
from typing import List, Optional
from pandas import Timestamp
import asyncio
import click


async def show_async(market: str, since: Optional[Timestamp] = None):
    if since:
        print(f"{market} trades since {since}:")
    else:
        print(f"{market} trades since the beginning:")

    source = source_instance("kraken_rest")
    async with request_context():
        async for series in source.trades(market, since):
            print("- - -")
            print(series)
            print(series.to_pandas())


@click.command()
@click.option("--market", help="Market to show trades for.")
@click.option("--since", help="Start time to show trades for.")
def show(market: Optional[str] = None, since: Optional[str] = None):
    since_time = parse_timestamp(since) if since else None
    asyncio.run(show_async(market or "kraken:btc/eur", since_time))
