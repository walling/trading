import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
from secrets import token_hex
from typing import Iterator, Optional, List
from ..model.types import SubjectSymbol, FileId
from .splitter import DataSplitter

ROW_GROUP_SIZE = 1000000

WRITER_OPTIONS = {
    "compression": "ZSTD",
    "version": "2.0",
    "data_page_version": "2.0",
}


class RecordsWriter:
    def __init__(self, path: Path, *, row_group_size=ROW_GROUP_SIZE):
        self._tmp_path = path.with_name(path.name + f".tmp_{token_hex(5)}")
        self._path = path
        self._row_group_size = row_group_size
        self._splitter = None
        self._writer = None

    def write(self, records: pa.Table):
        if not self._writer:
            self._splitter = DataSplitter(
                batch_size=self._row_group_size,
                concat_fn=pa.concat_tables,
            )
            self._writer = pq.ParquetWriter(
                self._tmp_path,
                schema=records.schema,
                **WRITER_OPTIONS,
            )

        self._splitter.add(records)
        for batch in self._splitter.batches():
            self._writer.write_table(batch)

        return self

    def close(self):
        if self._writer:
            self.flush()
            self._writer.close()
            self._writer = None
            self._tmp_path.rename(self._path)

        return self

    def flush(self):
        if self._writer:
            self._splitter.flush()
            for batch in self._splitter.batches():
                self._writer.write_table(batch)

        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getstate__(self):
        if not (self._splitter is None and self._writer is None):
            raise RuntimeError("RecordsWriter: can not be pickled after writing")

        state = self.__dict__.copy()
        del state["_splitter"]
        del state["_writer"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._splitter = None
        self._writer = None


class RecordsRepository:
    def __init__(self, path: Path):
        self._path = path

    def get(self, file: FileId) -> pa.Table:
        return pq.read_table(self._fileid_to_path(file))

    def find(self, subject: Optional[SubjectSymbol] = None) -> Iterator[FileId]:
        if subject:
            glob_pattern = f"{subject}/*/*/*/*.{subject}.parquet"
        else:
            glob_pattern = "*/*/*/*/*.parquet"

        last_file = None
        for path in self._path.glob(glob_pattern):
            file = self._path_to_fileid(path)
            if file:
                if last_file:
                    assert file > last_file, "expect files to be ordered"  # type: ignore
                yield file
                last_file = file

    def writer(self, file: FileId) -> RecordsWriter:
        return RecordsWriter(self._fileid_to_path(file))

    def _fileid_to_path(self, file: FileId):
        return self._path.joinpath(
            str(file.subject),
            str(file.source),
            str(file.market.exchange),
            str(file.market.instrument),
            str(file.time.year),
            "_".join(
                [
                    str(file.time),
                    str(file.source),
                    str(file.market.exchange),
                    str(file.market.instrument),
                ],
            )
            + f".{file.subject}.parquet",
        )

    def _path_to_fileid(self, path: Path):
        pass  # todo
