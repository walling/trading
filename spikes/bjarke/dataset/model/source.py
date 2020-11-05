from typing import Protocol
from abc import abstractmethod
import pyarrow as pa


class Source(Protocol):
    @abstractmethod
    def trades(self, instrument: str, since=None) -> pa.Table:
        ...
