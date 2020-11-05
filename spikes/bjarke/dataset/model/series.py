from .partition import Partition
import pyarrow as pa


class TableSeries:
    def __init__(self, table: pa.Table, partition: Partition):
        self._table = table
        self._partition = partition

    @property
    def table(self) -> pa.Table:
        return self._table

    @property
    def partition(self) -> Partition:
        return self._partition

    @property
    def num_rows(self) -> int:
        return len(self._table)

    def __len__(self) -> int:
        return self.num_rows
