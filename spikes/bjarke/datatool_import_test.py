"""
Run with:

    env PYTHONPATH=lib python3 spikes/bjarke/datatool_import_test.py
"""

import asyncio
import math
import pyarrow.dataset as ds
import pyarrow as pa
from pathlib import Path

import datatool.write.schema as schema
from datatool.write.writer import DatasetWriter
from datatool._infrastructure.records import RecordsRepository

ROOT_DIR = Path(__file__).parent.joinpath("../..").resolve()


def get_decimal(imported, name):
    decimal = imported[name].flatten()
    return pa.StructArray.from_arrays(
        [
            pa.array(decimal[index].to_pylist(), type=field.type)
            for index, field in enumerate(schema.decimal)
        ],
        fields=list(schema.decimal),
    )


def get_records(imported):
    # external_id = pa.StructArray.from_arrays(
    #     [pa.nulls(len(imported), type=field.type) for field in schema.external_id],
    #     fields=list(schema.external_id),
    # )
    external_id = pa.nulls(len(imported), type=schema.external_id)
    time = imported["time"]
    price = get_decimal(imported, "price")
    amount = get_decimal(imported, "amount")
    side = imported["side"]
    order = imported["order"]
    extra_json = imported["extra_json"]
    subject = pa.array(
        imported["subject"].to_pylist(),
        type=schema.trades_schema.field("subject").type,
    )
    source = pa.array(
        imported["source"].to_pylist(),
        type=schema.trades_schema.field("source").type,
    )
    exchange = pa.array(
        imported["exchange"].to_pylist(),
        type=schema.trades_schema.field("exchange").type,
    )
    instrument = pa.array(
        [i.replace("_", "/") for i in imported["instrument"].to_pylist()],
        type=schema.trades_schema.field("instrument").type,
    )

    return pa.table(
        {
            "external_id": external_id,
            "time": time,
            "price": price,
            "amount": amount,
            "side": side,
            "order": order,
            "extra_json": extra_json,
            "subject": subject,
            "source": source,
            "exchange": exchange,
            "instrument": instrument,
        },
        schema=schema.trades_schema,
    )


def import_records(data_dir, *, filter=None, batch_size=1000):
    field_names = ["subject", "source", "exchange", "instrument", "year"]
    part = ds.partitioning(field_names=field_names)
    dataset = ds.dataset(data_dir, format="parquet", partitioning=part)

    for task in dataset.scan(filter=filter):
        for fragment in task.execute():
            for batch in pa.Table.from_batches([fragment]).to_batches(batch_size):
                yield get_records(pa.Table.from_batches([batch]))


async def main():
    wal = RecordsRepository(ROOT_DIR / "wal")
    writer = DatasetWriter(wal)
    f = None
    # f = ds.field("instrument") == "ada_eth"
    for records in import_records(ROOT_DIR / "data", filter=f, batch_size=10000):
        row = dict((key, column[0]) for key, column in records[0:1].to_pydict().items())
        fileid_parts = [
            row["subject"],
            row["source"],
            row["exchange"],
            row["instrument"].replace("_", "/"),
            row["time"].isoformat(),
        ]
        try:
            await writer.write_records(records)
            print(f"{':'.join(fileid_parts)} ({len(records)})")
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            raise
        except Exception as error:
            print(f"error: {error}")


if __name__ == "__main__":
    asyncio.run(main())
