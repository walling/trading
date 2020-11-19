from typing import Iterator, List, Callable, Protocol, Generic, TypeVar

T = TypeVar("T", bound="_Sliceable")


class _Sliceable(Protocol[T]):
    def __getitem__(self, s: slice) -> T:
        ...

    def __len__(self) -> int:
        ...


class DataSplitter(Generic[T]):
    def __init__(self, *, batch_size: int, concat_fn: Callable[[List[T]], T]):
        self._buffer: List[T] = []
        self._buffer_size: int = 0
        self._batches: List[T] = []
        self._batch_size = batch_size
        self._concat_fn = concat_fn

    def add(self, records: T):
        size = self._batch_size
        self._buffer.append(records)
        self._buffer_size += len(records)

        if self._buffer_size >= size:
            buf = self._concat_fn(self._buffer)
            while len(buf) >= size:
                self._batches.append(buf[0:size])
                buf = buf[size:]
            self._buffer = [buf]
            self._buffer_size = len(buf)

    def batches(self) -> Iterator[T]:
        batches = self._batches
        self._batches = []
        yield from batches

    def flush(self):
        if not self._buffer_size:
            return

        batch = self._concat_fn(self._buffer)
        self._buffer = []
        self._buffer_size = 0
        self._batches.append(batch)
