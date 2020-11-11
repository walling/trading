from ..model.write.writer import DatasetWriter
import asyncio, time, click
from pathlib import Path


async def index_async(data_dir: str):
    writer = DatasetWriter(data_dir)
    await writer.index()


@click.command()
@click.option(
    "-d",
    "--data-dir",
    default="./data",
    envvar="DATASET_DIR",
    help="Directory to index trading data.",
)
def index(data_dir: str):
    asyncio.run(index_async(data_dir=data_dir))
