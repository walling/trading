import pytest
import atexit, tempfile, shutil, string
import pickle, math, re
import asyncio
import pyarrow.parquet as pq
import pyarrow as pa
import pandas
from calendar import monthrange
from secrets import token_urlsafe
from random import Random
from pathlib import Path
from ...model.types import FileId
from ..records import ROW_GROUP_SIZE, RecordsWriter, RecordsRepository


# Mock data series
_RANDOM = Random(382)
_SIZE_1 = 17078
_SIZE_2 = 111
_ARRAY_INT = pa.array([_RANDOM.randint(-(10 ** 18), 10 ** 18) for i in range(_SIZE_1)])
_ARRAY_STR = pa.array(
    [
        "".join(_RANDOM.choices(string.ascii_uppercase + string.digits, k=12))
        for i in range(_SIZE_1)
    ]
)

# Small mock records
_RECORDS_SMALL = pa.table(
    {
        "int": _ARRAY_INT,
        "str": _ARRAY_STR,
    }
)[0:123]

# Big mock records
_RECORDS_BIG = pa.table(
    {
        "int": pa.chunked_array([_ARRAY_INT] * _SIZE_2),
        "str": pa.chunked_array([_ARRAY_STR] * _SIZE_2),
    }
)

# Setup temporary directory and delete everything on exit
_TMP_DIR = Path(tempfile.mkdtemp())
atexit.register(shutil.rmtree, str(_TMP_DIR), ignore_errors=True)


class FileIdHelper:
    def _year(self):
        return str(_RANDOM.randint(1900, 2100))

    def _month(self):
        return "%s-%02d" % (_RANDOM.randint(1900, 2100), _RANDOM.randint(1, 12))

    def _day(self):
        y = _RANDOM.randint(1900, 2100)
        m = _RANDOM.randint(1, 12)
        d = _RANDOM.randint(1, monthrange(y, m)[1])
        return "%s-%02d-%02d" % (y, m, d)

    def _timestamp(self):
        return pandas.Timestamp(_RANDOM.randint(0, 4000000000000000000)).isoformat()

    def _time(self):
        time_fn = _RANDOM.choice([self._year, self._month, self._day, self._timestamp])
        return time_fn()

    def _symbol(self, n=5):
        return "".join(_RANDOM.choices(string.ascii_lowercase, k=n))

    def random(self, n=1, *, n_same_root=5):
        fileids = [
            fileid
            for i in range(math.ceil(n / n_same_root))
            for fileid in self.random_with_different_times(n=n_same_root)
        ]
        return fileids[0:n]

    def random_with_different_times(self, n=5):
        """
        Generate a valid, but random, FileId object.
        """

        extension = ""
        if _RANDOM.randint(0, 1):
            extension = f"/ext-{self._symbol()}-{self._symbol()}"

        isize = _RANDOM.randint(3, 4)
        parts = [
            "trades",
            f"src-{self._symbol(_RANDOM.randint(3, 5))}",
            f"ex-{self._symbol(_RANDOM.randint(7, 9))}",
            f"{self._symbol(isize)}/{self._symbol(isize)}{extension}",
        ]

        for i in range(n):
            yield FileId.from_str(":".join(parts + [self._time()]))


_FILEID = FileIdHelper()


class TempDirectory:
    """
    Helper class to create cheap temp directories. It uses the global _TMP_DIR
    as a base and creates new random generated directories inside. The newly
    created temp directories are thus empty.

    On exit the whole _TMP_DIR directory tree will be deleted.

    The helper class is used as a context manager and returns a Path object:

    >>> with TempDirectory() as path:
    ...     path.is_dir()  # use temp directory path
    True
    """

    def __enter__(self):
        path = _TMP_DIR / token_urlsafe()
        path.mkdir()
        return path

    def __exit__(self, exc_type, exc_value, traceback):
        pass


def test_RecordsWriter_init():
    with TempDirectory() as tmp:
        RecordsWriter(tmp / "test.parquet")
        assert [] == list(tmp.iterdir()), "no files"


def test_RecordsWriter_write_simple():
    with TempDirectory() as tmp:
        records = _RECORDS_SMALL
        filename = tmp / "test.parquet"

        writer = RecordsWriter(filename)
        writer.write(records)

        files = list(map(lambda p: p.name, tmp.iterdir()))
        assert 1 == len(files), "temporary file"
        assert re.fullmatch(r"test\.parquet\.tmp_[\w\-]+", files[0]), "temporary file"

        writer.close()

        files = list(map(lambda p: p.name, tmp.iterdir()))
        assert ["test.parquet"] == files, "parquet file"
        assert records == pq.read_table(filename), "records content"
        assert 1 == pq.read_metadata(filename).num_row_groups, "row groups"


def test_RecordsWriter_write_multiple():
    with TempDirectory() as tmp:
        repeats = 5
        records = _RECORDS_SMALL
        filename = tmp / "test.parquet"

        writer = RecordsWriter(filename)
        for i in range(repeats):
            writer.write(records)
        writer.close()

        written_records = pa.concat_tables([records] * repeats)
        assert written_records == pq.read_table(filename), "records content"


def test_RecordsWriter_write_big():
    with TempDirectory() as tmp:
        repeats = 3
        records = _RECORDS_BIG
        filename = tmp / "test.parquet"

        writer = RecordsWriter(filename)
        for i in range(repeats):
            writer.write(records)
        writer.close()

        written_records = pa.concat_tables([records] * repeats)
        assert written_records == pq.read_table(filename), "records content"

        count = math.ceil(len(records) * repeats / ROW_GROUP_SIZE)
        assert count > 1, "expected to test multiple row groups"
        assert count == pq.read_metadata(filename).num_row_groups, "row groups"


def test_RecordsWriter_pickle():
    with TempDirectory() as tmp:
        records = _RECORDS_SMALL
        filename = tmp / "test.parquet"

        writer = RecordsWriter(filename)

        writer2 = pickle.loads(pickle.dumps(writer))
        writer2.write(records)
        writer2.close()

        files = list(map(lambda p: p.name, tmp.iterdir()))
        assert ["test.parquet"] == files, "parquet file"

        with pytest.raises(RuntimeError):
            pickle.dumps(writer2)


def test_RecordsRepository_init():
    with TempDirectory() as tmp:
        RecordsRepository(tmp)
        assert [] == list(tmp.iterdir()), "no files"


def test_RecordsRepository_get():
    with TempDirectory() as tmp:
        (file,) = _FILEID.random(1)
        records = _RECORDS_SMALL

        repository = RecordsRepository(tmp)
        repository.writer(file).write(records).close()

        assert records == repository.get(file), "records content"


def test_RecordsRepository_find():
    with TempDirectory() as tmp:
        files = sorted(_FILEID.random(47))
        records = _RECORDS_SMALL
        print("\n".join(map(str, files)))

        repository = RecordsRepository(tmp)
        for file in files:
            repository.writer(file).write(records).close()

        assert files == list(repository.find()), "list of files"


def test_RecordsRepository_writer():
    with TempDirectory() as tmp:
        file1, file2, file3 = _FILEID.random(n=3, n_same_root=2)

        repository = RecordsRepository(tmp)
        writer1 = repository.writer(file1)
        writer2 = repository.writer(file2)
        writer1.write(_RECORDS_SMALL)
        writer2.write(_RECORDS_BIG)
        writer3 = repository.writer(file3)
        writer1.close()
        writer3.write(_RECORDS_SMALL)
        writer2.close()
        writer3.close()

        assert 3 == len(list(tmp.rglob("*.parquet"))), "list of files"
        assert _RECORDS_SMALL == repository.get(file1), "file 1 records"
        assert _RECORDS_BIG == repository.get(file2), "file 2 records"
        assert _RECORDS_SMALL == repository.get(file3), "file 3 records"
