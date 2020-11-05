from ..source import source_instance
from ..model.types import timestamp
from ..infrastructure.request import request_context
from typing import List, Optional
import click
import asyncio


async def fetch_async(market: str, since: Optional[str] = None):
    since = timestamp(since) if since else None
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
@click.option("--market", help="Market to fetch trades for.")
@click.option("--since", help="Start time to fetch trades for.")
def fetch(market: Optional[str] = None, since: Optional[str] = None):
    asyncio.run(fetch_async(market or "kraken:btc/eur", since))


def main(args: List[str] = [], prog_name="dataset"):
    fetch(args=args, prog_name=prog_name, standalone_mode=True)
