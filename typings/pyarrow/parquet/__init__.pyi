from typing import Union
from os import PathLike
from .. import Table, Schema

_Path = Union[str, PathLike]

def read_table(path: _Path): ...
def read_metadata(path: _Path): ...
def write_table(
    table: Table,
    path: _Path,
    schema: Optional[Schema] = None,
    compression: Optional[str] = None,
    version: str = "1.0",
    data_page_version: str = "1.0",
): ...

class ParquetWriter:
    def __init__(
        self,
        path: _Path,
        schema: Optional[Schema] = None,
        compression: Optional[str] = None,
        version: str = "1.0",
        data_page_version: str = "1.0",
    ): ...
    def write_table(self, table: Table): ...
    def close(self): ...
