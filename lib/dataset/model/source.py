from typing import Protocol, Optional, AsyncIterator
from abc import abstractmethod
from pandas import Timestamp
import pyarrow as pa
from .series import TableSeries


class Source(Protocol):
    @abstractmethod
    async def trades(
        self,
        instrument: str,
        since: Optional[Timestamp] = None,
    ) -> AsyncIterator[TableSeries]:
        ...
