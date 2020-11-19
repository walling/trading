exit()  # WARNING: Will change local data files

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.compute as pc
import numpy as np
from decimal import Decimal
from pathlib import Path

DATA_DIR = Path(__file__).parent.joinpath("../../data_old/trades").resolve()
WAL_DIR = Path(__file__).parent.joinpath("../../data_old/wal").resolve()

DECIMAL_VALUE = pa.field("value", pa.uint64(), nullable=False)
DECIMAL_SCALE = pa.field("scale", pa.int32(), nullable=False)
DECIMAL_STRUCT = pa.struct([DECIMAL_VALUE, DECIMAL_SCALE])

EXTERNAL_ID_INTEGER = pa.field("integer", pa.uint64())
EXTERNAL_ID_UUID = pa.field("uuid", pa.binary(16))
EXTERNAL_ID_STRING = pa.field("string", pa.string())
EXTERNAL_ID_STRUCT = pa.struct(
    [EXTERNAL_ID_INTEGER, EXTERNAL_ID_UUID, EXTERNAL_ID_STRING]
)

TRADES_SCHEMA = pa.schema(
    [
        pa.field("external_id", EXTERNAL_ID_STRUCT),
        pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("price", DECIMAL_STRUCT, nullable=False),
        pa.field("amount", DECIMAL_STRUCT, nullable=False),
        pa.field("side", pa.dictionary(pa.int32(), pa.string())),
        pa.field("order", pa.dictionary(pa.int32(), pa.string())),
        pa.field("extra_json", pa.string()),
        pa.field("subject", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("instrument", pa.string(), nullable=False),
        pa.field("year", pa.int32(), nullable=False),
        pa.field("month", pa.int32(), nullable=False),
        pa.field("day", pa.int32(), nullable=False),
    ]
)

PARTITION_COLS = [
    "subject",
    "source",
    "exchange",
    "instrument",
    "year",
    "month",
    "day",
]


def hash(table, partition_cols):
    partitions = [table[name].dictionary_encode() for name in partition_cols]
    dictionaries = [p.chunk(0).dictionary for p in partitions]
    indices = [
        pa.chunked_array([c.indices.cast(pa.int64()) for c in p.iterchunks()])
        for p in partitions
    ]
    sizes = [len(d) for d in dictionaries]

    current = indices[0]
    for i in range(1, len(indices)):
        current = pc.add(pc.multiply(current, sizes[i]), indices[i])

    return current


def partition_table(table, partition_cols):
    h = hash(table, partition_cols).to_numpy()

    # Inspired by https://gist.github.com/nvictus/66627b580c13068589957d6ab0919e66
    where = np.flatnonzero
    starts = np.r_[0, where(np.diff(h)) + 1]
    stops = np.r_[starts, len(table)][1:]

    for start, stop in zip(starts, stops):
        chunk = table[start:stop]
        partition = [pc.unique(chunk[name]) for name in partition_cols]
        if not all((len(p) == 1 for p in partition)):
            print(chunk.to_pandas())
            print(partition)
            raise Exception(f"Partitions columns must be ordered: {start}:{stop}")

        values = [p[0].as_py() for p in partition]
        yield slice(start, stop), tuple(values), chunk.drop(partition_cols)


for filename in DATA_DIR.rglob("*.parquet"):
    source_str = filename.parent.name.split("=")[1].replace("_", "-")
    instrument_str = filename.parent.parent.name.split("=")[1]
    exchange_str = filename.parent.parent.parent.name.split("=")[1]
    year_str = filename.parent.parent.parent.parent.name.split("=")[1]
    subject_str = filename.parent.parent.parent.parent.parent.name

    print(subject_str, year_str, exchange_str, instrument_str, source_str)
    table = pq.read_table(filename)
    external_id = pa.array(
        [None] * len(table),
        type=EXTERNAL_ID_STRUCT,
    )
    time = table["time"]

    if instrument_str == "btc_eur":
        price_scale = 1
    elif instrument_str == "ada_btc":
        price_scale = 8
    elif instrument_str == "ada_eth":
        price_scale = 7
    else:
        price_scale = int(table.schema.field("price").metadata[b"scale"])
    price = pa.StructArray.from_arrays(
        (
            pa.array(table["price"].to_pylist(), type=DECIMAL_VALUE.type),
            pa.array([price_scale] * len(table), type=DECIMAL_SCALE.type),
        ),
        fields=(DECIMAL_VALUE, DECIMAL_SCALE),
    )

    if instrument_str in ["btc_eur", "ada_btc", "ada_eth"]:
        amount_scale = 8
    else:
        amount_scale = int(table.schema.field("amount").metadata[b"scale"])
    amount = pa.StructArray.from_arrays(
        (
            pa.array(table["amount"].to_pylist(), type=DECIMAL_VALUE.type),
            pa.array([amount_scale] * len(table), type=DECIMAL_SCALE.type),
        ),
        fields=(DECIMAL_VALUE, DECIMAL_SCALE),
    )

    side = table["side"]
    order = table["order"]
    extra_json = pa.array(
        ((extra.as_py() or None) for extra in table["extra_json"]),
        type=TRADES_SCHEMA.field("extra_json").type,
        size=len(table),
    )
    subject = pa.array(
        [subject_str] * len(table),
        type=TRADES_SCHEMA.field("subject").type,
    )
    source = pa.array(
        [source_str] * len(table),
        type=TRADES_SCHEMA.field("source").type,
    )
    exchange = pa.array(
        [exchange_str] * len(table),
        type=TRADES_SCHEMA.field("exchange").type,
    )
    instrument = pa.array(
        [instrument_str] * len(table),
        type=TRADES_SCHEMA.field("instrument").type,
    )
    year = pa.array(
        time.to_pandas().dt.year.tolist(),
        type=TRADES_SCHEMA.field("year").type,
    )
    month = pa.array(
        time.to_pandas().dt.month.tolist(),
        type=TRADES_SCHEMA.field("month").type,
    )
    day = pa.array(
        time.to_pandas().dt.day.tolist(),
        type=TRADES_SCHEMA.field("day").type,
    )
    newtable = pa.table(
        [
            external_id,
            time,
            price,
            amount,
            side,
            order,
            extra_json,
            subject,
            source,
            exchange,
            instrument,
            year,
            month,
            day,
        ],
        schema=TRADES_SCHEMA,
    )

    t = newtable["time"].cast(pa.int64())
    if not (
        pc.unique(pc.greater_equal(pc.subtract(t[1:], t[:-1]), 0)) == pa.array([True])
    ):
        raise Exception("time must be monotonic")

    for pos, partition, subtable in partition_table(
        newtable,
        partition_cols=PARTITION_COLS,
    ):
        t = subtable["time"][0].as_py()
        time_str = t.strftime("%Y-%m-%dT%H%M%S_%f") + ("%03d" % t.nanosecond) + "Z"
        filename = f"{time_str}_{partition[1]}_{partition[2]}_{partition[3]}.{partition[0]}.parquet"
        path = WAL_DIR / "/".join(map(str, partition[:-2])) / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            subtable,
            path,
            row_group_size=1000000,
            compression="ZSTD",
            version="2.0",
            data_page_version="2.0",
        )
        print(f"=> wal/{'/'.join(map(str, partition))} ({len(subtable)})")

    # pq.write_to_dataset(
    #     newtable,
    #     WAL_DIR,
    #     partition_cols=[
    #         "subject",
    #         "source",
    #         "exchange",
    #         "instrument",
    #         "year",
    #         "month",
    #         "day",
    #     ],
    #     use_legacy_dataset=False,
    # )
