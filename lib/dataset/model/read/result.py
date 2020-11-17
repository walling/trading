from typing import List, Optional
from pathlib import Path
import pyarrow.dataset as ds
import pyarrow as pa
import numpy as np

# Trades data location.
ROOT_DIR = (Path(__file__).parent / ".." / ".." / ".." / "..").resolve()
DATA_DIR = ROOT_DIR / "data"

PARTITIONING = ds.partitioning(field_names=["source", "exchange", "instrument", "year"])


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
        self._dataset = ds.dataset(
            self._path,
            format="parquet",
            partitioning=PARTITIONING,
        )
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

    def column_numpy(self, name, decimals_as_floats=False, timestamps_as_floats=False):
        field = self._table.schema.field(name)

        if isinstance(field.type, pa.TimestampType):
            if timestamps_as_floats:
                return self._table[name].cast(pa.float64).to_numpy() / 1e9
            else:
                return self._table[name].to_numpy()

        if isinstance(field.type, pa.StructType):
            series = self._table[name].flatten()
            if decimals_as_floats:
                return series[0].to_numpy() / (10.0 ** series[1].to_numpy())
            else:
                return series[0].to_numpy()

        return self._table[name].to_numpy()

    def __len__(self) -> int:
        return self.num_rows

    def __str__(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        return f"<Result subject={repr(self._subject)} num_rows={len(self)}>"
