from ..infrastructure.request import request_context
from ..source import source_instance
from ..model.write.writer import DatasetWriter
import asyncio, time, click
from pathlib import Path


async def download_async(data_dir: str, timeout: float):
    start_time = time.time()

    writer = DatasetWriter(data_dir)
    source = source_instance("kraken_rest")

    async with request_context():
        for market in await source.markets():
            elapsed_time = time.time() - start_time
            remaining_time = timeout - elapsed_time
            if remaining_time < 5:
                break

            print(f"Downloading trading data for {market}:")
            await writer.write_trades(str(market), source, timeout=remaining_time)


@click.command()
@click.option(
    "-d",
    "--data-dir",
    default="./data",
    envvar="DATASET_DIR",
    help="Directory to save trading data.",
)
@click.option(
    "-t",
    "--timeout",
    default=5,
    help="Maximum number of minutes to download trading data.",
    type=float,
)
def download(data_dir: str, timeout: float):
    asyncio.run(
        download_async(
            data_dir=data_dir,
            timeout=timeout * 60,
        )
    )
