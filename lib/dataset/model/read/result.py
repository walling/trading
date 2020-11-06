from typing import List, Optional
from pathlib import Path
import pyarrow.dataset as ds
import numpy as np

# Trades data location.
ROOT_DIR = (Path(__file__).parent / ".." / ".." / ".." / "..").resolve()
DATA_DIR = ROOT_DIR / "data"


class Result:
    def __init__(
        self,
        subject: str,
        columns: Optional[List[str]] = None,
        dataset_filter=None,
    ):
        self._subject = subject
        self._columns = columns
        self._path = DATA_DIR / subject
        self._dataset = ds.dataset(self._path, format="parquet", partitioning="hive")
        self._table = self._dataset.to_table(filter=dataset_filter, columns=columns)

    @property
    def subject(self) -> str:
        return self._subject

    @property
    def columns(self) -> Optional[List[str]]:
        return self._columns

    @property
    def num_rows(self):
        return len(self._table)

    def raw_table_pandas(self):
        return self._table.to_pandas()

    def raw_table_numpy(self):
        return self._table.to_numpy()

    def raw_table_arrow(self):
        return self._table

    def column_numpy(self, name, decimals_as_floats=True, timestamps_as_floats=True):
        values = self._table[name].to_numpy()

        metadata = self._table.schema.field(name).metadata
        if decimals_as_floats and metadata and b"scale" in metadata:
            scale = int(metadata[b"scale"].decode())
            return values / (10.0 ** scale)

        if timestamps_as_floats and np.issubdtype(values.dtype, np.datetime64):
            return values.astype(np.float64) / 1e9

        return values

    def __len__(self) -> int:
        return self.num_rows

    def __str__(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        return f"<Result subject={repr(self._subject)} num_rows={len(self)}>"
