from typing import Optional
from pathlib import Path
from pandas import Timestamp, Timedelta
from secrets import token_hex
import pyarrow as pa
import pyarrow.parquet as pq
from ..source import Source, split_per_day
from ..period import parse_period
from ..partition import Partition
from ..types import TimeInterval


class DatasetWriter:
    def __init__(self, path: str):
        self._path = Path(path).resolve()

    @property
    def path(self) -> Path:
        return self._path

    async def write_trades(self, market: str, source: Source, **kwargs):
        partition = None
        index_paths = set()
        since = self._get_since("trades", market)
        async for series in split_per_day(source.trades(market, since=since, **kwargs)):
            if not partition:
                partition = series.partition
            path = self._path / series.partition.path("trades")
            index_paths.add(path.parent)
            pq.write_table(
                series.table,
                path,
                compression="ZSTD",
                version="2.0",
                data_page_version="2.0",
            )
            print(series)

        for index_path in index_paths:
            print(f"INDEXING {index_path}:")
            await self.index_path(index_path, "trades", partition)
            await self.index_path(index_path, "trades", partition)

    async def index_path(self, path: Path, subject: str, partition: Partition):
        full_period = None
        files_to_index = {}
        files_to_remove = {}

        now = Timestamp.utcnow()
        daily = Timestamp(year=now.year, month=now.month, day=now.day, tz="UTC")
        monthly = Timestamp(year=now.year, month=now.month, day=1, tz="UTC")
        yearly = Timestamp(year=now.year, month=1, day=1, tz="UTC")

        def add_file_to_index(period, file):
            if period.start < yearly:
                if period.is_year:
                    return
                index_period = "%04d" % period.start.year
            elif period.start < monthly:
                if period.is_month:
                    return
                index_period = "%04d-%02d" % (period.start.year, period.start.month)
            else:
                if period.start < daily and period.is_day:
                    return
                index_period = "%04d-%02d-%02d" % (
                    period.start.year,
                    period.start.month,
                    period.start.day,
                )

            if index_period not in files_to_index:
                files_to_index[index_period] = []
            files_to_index[index_period].append((period, file))

        def add_file_to_remove(shadow_file, file):
            if shadow_file not in files_to_remove:
                files_to_remove[shadow_file] = []
            files_to_remove[shadow_file].append(file)

        def sort_by_period_start_and_duration(period_file):
            period, file = period_file
            return (period.start, period.start - period.end)

        dir_files = map(
            lambda p: (self._get_filename_period(p.stem), p), path.glob("*.parquet")
        )

        for period, file in sorted(dir_files, key=sort_by_period_start_and_duration):
            if not full_period:
                full_period = period
                shadow_file = file
                add_file_to_index(period, file)
                continue

            if full_period.end < period.end:
                full_period = full_period.with_end(period.end)
                shadow_file = file
                add_file_to_index(period, file)
                continue

            add_file_to_remove(shadow_file, file)

        now = Timestamp.utcnow()
        for shadow_file, files in files_to_remove.items():
            modified = now - Timestamp(shadow_file.stat().st_mtime, tz="UTC", unit="s")
            if modified <= Timedelta(minutes=5):
                print(f"- SHADOW STILL FRESH: {shadow_file.stem}")
                continue
            for file in files:
                file_period = self._get_filename_period(file.stem)
                shadow_period = self._get_filename_period(shadow_file.stem)
                outside = (
                    file_period.start < shadow_period.start
                    or file_period.end > shadow_period.end
                )
                if outside:
                    print(f"- NOT REMOVING (OUTSIDE SHADOW): {file.stem}")
                    continue
                file.unlink(missing_ok=True)
                print(f"- REMOVED: {file.stem}")

        for index_period, files in files_to_index.items():
            if len(files) == 1:
                print(f"- ALREADY INDEXED: {files[0][1].stem}")
                continue

            file_period = TimeInterval(files[0][0].start, files[-1][0].end)
            table = pa.concat_tables([pq.read_table(file) for period, file in files])
            filename = partition.with_period(file_period).path(
                subject,
                period_short=True,
            )
            tmp_filename = self._path / f"{filename}.new_{token_hex(6)}"

            pq.write_table(
                table,
                tmp_filename,
                compression="ZSTD",
                version="2.0",
                data_page_version="2.0",
            )
            tmp_filename.rename(self._path / filename)
            print(f"- INDEXED: {(self._path / filename).stem}")
        exit()

    def _get_since(self, subject: str, market: str) -> Optional[Timestamp]:
        exchange, instrument = market.split(":", 1)
        path_pattern = f"{subject}/year=*/exchange={exchange}/instrument={instrument.replace('/', '_')}/source=*/*.parquet"

        paths = sorted(self._path.glob(path_pattern))
        if not paths:
            return None

        return self._get_filename_period(paths[-1].stem).end

    def _get_filename_period(self, name: str) -> Timestamp:
        parts = name.split(".")
        if len(parts[0]) > 10:
            period = f"{parts[0]}.{parts[1]}/{parts[2]}.{parts[3]}"
        else:
            period = parts[0]
        return parse_period(period)


if __name__ == "__main__":
    from ...infrastructure.request import request_context
    from ...source import source_instance
    import asyncio

    async def test():
        async with request_context():
            kraken_rest = source_instance("kraken_rest")
            writer = DatasetWriter("./data")
            await writer.write_trades("kraken:btc/eur", kraken_rest, timeout=300)

    asyncio.run(test())
