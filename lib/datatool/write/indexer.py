import asyncio
from concurrent.futures import ProcessPoolExecutor
from ..model.records import RecordsRepository
from .session import Session


class DatasetIndexer:
    def __init__(
        self,
        data: RecordsRepository,
        write_ahead_log: RecordsRepository,
        *,
        session: Session = Session(),
    ):
        self._data = data
        self._write_ahead_log = write_ahead_log
        self._session = session

    async def index(self):
        async with self._session.resource(IndexAsyncPool) as pool:
            await pool.index_async(self._data, self._write_ahead_log)


def index_sync(data: RecordsRepository, write_ahead_log: RecordsRepository):
    # TODO: Implement merging algorithm
    wal_files = list(write_ahead_log.find())
    for file in data.find():
        while len(wal_files) > 0 and file > wal_files[0]:
            print("wal:", wal_files.pop(0))
        print("data:", file)


class IndexAsyncPool:
    def __init__(self):
        self._pool = ProcessPoolExecutor(max_workers=1)

    async def index_async(
        self,
        data: RecordsRepository,
        write_ahead_log: RecordsRepository,
    ):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, index_sync, data, write_ahead_log)

    async def close(self):
        self._pool.shutdown()
