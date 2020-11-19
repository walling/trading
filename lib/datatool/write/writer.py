import asyncio
from typing import List
from concurrent.futures import ThreadPoolExecutor
from pyarrow import Table
from ..model.types import FileId
from ..model.records import RecordsRepository
from .partitioning import partition_records
from .session import Session


class DatasetWriter:
    def __init__(
        self,
        write_ahead_log: RecordsRepository,
        *,
        session: Session = Session(),
    ):
        self._write_ahead_log = write_ahead_log
        self._session = session

    async def write(self, records: Table):
        async with self._session.resource(WriteAsyncPool) as pool:
            await pool.write_async(self._write_ahead_log, records)


class WriteAsyncPool:
    def __init__(self):
        self._pool = ThreadPoolExecutor()

    async def write_async(
        self,
        repository: RecordsRepository,
        records: Table,
    ) -> List[FileId]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, self._write, repository, records)

    async def close(self):
        self._pool.shutdown()

    def _write(self, repository: RecordsRepository, records: Table) -> List[FileId]:
        files = []

        for file, chunk in partition_records(records):
            try:
                repository.writer(file).write(chunk).close()
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                break

            files.append(file)

        return files
