import asyncio, tempfile, shutil
import pyarrow as pa
from random import Random
from pathlib import Path
from ..records import RecordsWriter

_RANDOM = Random(382)
_ARRAY1 = pa.array([_RANDOM.randint(1, 1000000000) for i in range(10000)])
_ARRAY2 = pa.array([_RANDOM.choice(["a", "b", "c", "d", "e"]) for i in range(10000)])
_TABLE = pa.table(
    {
        "x": _ARRAY1,
        "y": _ARRAY2,
    }
)


class TempDir:
    def __init__(self):
        self._path = None

    def __enter__(self):
        self._path = Path(tempfile.mkdtemp())
        return self._path

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(str(self._path), ignore_errors=True)


def test_RecordsWriter_init():
    with TempDir() as path:
        RecordsWriter(path / "test.parquet")
        assert [] == list(path.iterdir()), "no files expected"


def test_RecordsWriter_write_close():
    with TempDir() as path:
        writer = RecordsWriter(path / "test.parquet")
        writer.write(_TABLE)
        writer.close()

        files = list(path.iterdir())
        assert 1 == len(files), "expected 1 file"
        assert "test.parquet" == files[0].name, "expected test.parquet file"
