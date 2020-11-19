from typing import Protocol, Optional, Tuple, Iterator, Iterable
from pyarrow import Table
from .types import SubjectSymbol, FileId


class RecordsWriter(Protocol):
    def write(self, records: Table) -> "RecordsWriter":
        ...

    def close(self) -> "RecordsWriter":
        ...


class RecordsRepository(Protocol):
    def get(self, file: FileId) -> Table:
        ...

    def find(self, subject: Optional[SubjectSymbol] = None) -> Iterator[FileId]:
        ...

    def writer(self, file: FileId) -> RecordsWriter:
        ...
