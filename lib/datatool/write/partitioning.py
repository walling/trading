import pyarrow.compute as pc
import pyarrow as pa
import numpy as np
from typing import Iterator, Tuple, List
from ..model.types import (
    SubjectSymbol,
    SourceSymbol,
    ExchangeSymbol,
    InstrumentSymbol,
    MarketSymbol,
    Timestamp,
    FileId,
)

DAY_NANOSECONDS = 24 * 60 * 60 * 10 ** 9
PARTITION_COLUMNS = ["subject", "source", "exchange", "instrument"]


def _hash_array(array: pa.ChunkedArray) -> Tuple[int, pa.ChunkedArray]:
    if isinstance(array.type, pa.DictionaryType):
        encoded = array
    else:
        encoded = array.dictionary_encode()
    return (
        len(encoded.chunk(0).dictionary),
        pa.chunked_array(
            [chunk.indices.cast(pa.int64()) for chunk in encoded.iterchunks()]
        ),
    )


def _hash_arrays(arrays: List[pa.ChunkedArray]) -> Tuple[int, pa.ChunkedArray]:
    if len(arrays) == 1:
        return _hash_array(arrays[0])

    head_max, head_hashes = _hash_array(arrays[0])
    tail_max, tail_hashes = _hash_arrays(arrays[1:])

    if head_max == 1:
        return (tail_max, tail_hashes)

    return (
        head_max * tail_max,
        pc.add(pc.multiply(head_hashes, tail_max), tail_hashes),
    )


def _run_length_encoding(arrays: List[pa.ChunkedArray]) -> Iterator[Tuple[int, int]]:
    size, hashes = _hash_arrays(arrays)

    # Inspired by https://gist.github.com/nvictus/66627b580c13068589957d6ab0919e66
    where = np.flatnonzero
    starts = np.r_[0, where(np.diff(hashes.to_numpy())) + 1]
    stops = np.r_[starts[1:], len(hashes)]
    return zip(starts.tolist(), stops.tolist())


def _fileid_from_chunk(chunk: pa.Table) -> FileId:
    timestamp = Timestamp(chunk["time"][0].as_py())
    subject = SubjectSymbol(chunk["subject"][0].as_py())
    source = SourceSymbol(chunk["source"][0].as_py())
    exchange = ExchangeSymbol(chunk["exchange"][0].as_py())
    instrument = InstrumentSymbol.from_symbol(chunk["instrument"][0].as_py())
    market = MarketSymbol(exchange, instrument)
    file = FileId(subject, source, market, timestamp)
    return file


def partition_records(records: pa.Table) -> Iterator[Tuple[FileId, pa.Table]]:
    time = records["time"].cast(pa.int64())
    assert records["time"].type.tz == "UTC", "time column must be in UTC"
    assert np.all(np.diff(time.to_numpy()) >= 0), "time column must be monotonic"

    date = pc.divide(time, DAY_NANOSECONDS)

    partition_columns = [records[name] for name in PARTITION_COLUMNS] + [date]
    for start, stop in _run_length_encoding(partition_columns):
        chunk = records[start:stop]
        yield _fileid_from_chunk(chunk), chunk.drop(PARTITION_COLUMNS)
