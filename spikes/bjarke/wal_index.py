exit()  # WARNING: Will change local data files

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pathlib import Path

WAL_DIR = Path(__file__).parent.joinpath("../../data_old/wal/trades").resolve()
DATA_DIR = Path(__file__).parent.joinpath("../../data1/trades").resolve()

stats = {}

part = ds.partitioning(field_names=["source", "exchange", "instrument", "year"])
dataset = ds.dataset(WAL_DIR, format="parquet", partitioning=part)
for fragment in dataset.get_fragments():
    path = Path(fragment.path)
    name, subject, file_format = path.name.split(".")
    timestamp, nanoseconds, source, exchange, instrument = name.split("_", 4)
    date = timestamp[0:10]
    symbol = "/".join([source, exchange, instrument])
    if symbol not in stats:
        stats[symbol] = [date, date]
    else:
        stats[symbol][0] = min(stats[symbol][0], date)
        stats[symbol][1] = max(stats[symbol][1], date)

current_bucket = None
writer = None
tables = []
table_size = 0
for fragment in dataset.get_fragments():
    path = Path(fragment.path)
    name, subject, file_format = path.name.split(".")
    timestamp, nanoseconds, source, exchange, instrument = name.split("_", 4)
    date = timestamp[0:10]
    symbol = "/".join([source, exchange, instrument])
    date_min, date_max = stats[symbol]

    if date >= date_max:
        continue

    date_bucket = (
        date[0:4] < date_max[0:4]
        and date[0:4]
        or (date[0:7] < date_max[0:7] and date[0:7] or date)
    )

    bucket = (symbol, date_bucket)
    if bucket != current_bucket:
        if table_size > 0:
            table = pa.concat_tables(tables)
            writer.write_table(table)
            print(current_bucket, len(table), len(tables))
        if writer:
            writer.close()
            writer = None
        current_bucket = bucket
        tables = []
        table_size = 0

    table = fragment.to_table()
    tables.append(table)
    table_size += len(table)

    if not writer:
        filename = f"{current_bucket[1]}_{current_bucket[0].replace('/', '_')}.{subject}.parquet"
        write_path = DATA_DIR / current_bucket[0] / current_bucket[1][0:4] / filename
        write_path.parent.mkdir(parents=True, exist_ok=True)
        writer = pq.ParquetWriter(
            write_path,
            table.schema,
            compression="ZSTD",
            version="2.0",
            data_page_version="2.0",
        )
        print(f"=> {write_path}")

    if table_size >= 1000000:
        table = pa.concat_tables(tables)
        writer.write_table(table[0:1000000])
        print(current_bucket, 1000000, len(tables))
        tables = [table[1000000:]]
        table_size = len(tables[0])


# table = dataset.to_table()
# print(len(table))
# print(table)
