import pyarrow.dataset as ds
from pathlib import Path

# Trades data location.
trades_dir = Path("data/trades")

# Open trades dataset using hive partitioning scheme (path contains key=value elements).
trades_dataset = ds.dataset(trades_dir, format="parquet", partitioning="hive")

# Prepare to query the dataset. This reads only the files necessary.
query = (
    (ds.field("source") == "kraken_rest")
    & (ds.field("exchange") == "kraken")
    & (ds.field("instrument") == "btc_eur")
    # & (ds.field("year") == 2020)
    # & (ds.field("time") >= "2020-08-01T00:00:00Z")
    # & (ds.field("time") < "2020-09-01T00:00:00Z")
)

# Query the dataset and read the columns we need.
table = trades_dataset.to_table(filter=query, columns=["time", "price"])

# Get price scale from field metadata.
price_scale = int(table.schema.field("price").metadata[b"scale"].decode())

# Extract data as numpy arrays.
time = table["time"].to_numpy()
price = table["price"].to_numpy() / 10 ** price_scale

# Do something interesting with the data.
print(time)
print(price)
print(f"Count: {len(table)}")
