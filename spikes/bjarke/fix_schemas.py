exit()  # WARNING: Will change local data files

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

DATA_DIR = Path(__file__).parent.joinpath("../../data").resolve()

schema = pa.schema(
    [
        pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("price", pa.int64(), nullable=False),
        pa.field("amount", pa.int64(), nullable=False),
        pa.field("side", pa.dictionary(pa.int32(), pa.string()), nullable=False),
        pa.field("order", pa.dictionary(pa.int32(), pa.string()), nullable=False),
        pa.field("extra_json", pa.string(), nullable=False),
    ]
)

for filename in DATA_DIR.rglob("*.parquet"):
    table = pq.read_table(filename)
    table_fixed = table.cast(schema)
    pq.write_table(
        table_fixed,
        filename,
        compression="ZSTD",
        version="2.0",
        data_page_version="2.0",
    )
