from typing import Protocol, Optional, List, Iterator, AsyncIterator
from abc import abstractmethod
from pandas import Timestamp, Timedelta
import pyarrow as pa
import pyarrow.compute as pc
import asyncio, threading
from queue import Queue
from .series import TableSeries
from .types import TimeInterval, Market


class Source(Protocol):
    async def trades(
        self,
        market: str,
        since: Optional[Timestamp] = None,
        timeout: Optional[float] = None,
        until_now: bool = True,
    ) -> AsyncIterator[TableSeries]:
        ...

    async def markets(self) -> List[Market]:
        ...


async def split_per_day(
    iterator: AsyncIterator[TableSeries],
) -> AsyncIterator[TableSeries]:
    async for series in iterator:
        frame = series.table.to_pandas()
        start_time = series.partition.period.start
        for (date, group) in frame.groupby(frame["time"].dt.date):
            time = Timestamp(date, tz="UTC")
            if time > start_time:
                start_time = time

            end_time = time + Timedelta(days=1)
            if end_time > series.partition.period.end:
                end_time = series.partition.period.end

            partition = series.partition.with_period(TimeInterval(start_time, end_time))
            start_time = time

            table = pa.Table.from_pandas(group).drop(["__index_level_0__"])
            yield TableSeries(table, partition)
