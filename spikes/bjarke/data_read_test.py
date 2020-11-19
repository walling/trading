import pyarrow.dataset as ds
from pathlib import Path

DATA_DIR = Path(__file__).parent.joinpath("../../data/trades").resolve()
part = ds.partitioning(field_names=["source", "exchange", "instrument", "year"])
dataset = ds.dataset(DATA_DIR, format="parquet", partitioning=part)
table = dataset.to_table(filter=ds.field("instrument") == "btc_eur")
price = table["price"]
pricet = price.type
pricef = price.flatten()
value = pricef[pricet.get_field_index("value")].to_numpy()
scale = pricef[pricet.get_field_index("scale")].to_numpy()
print(value / 10.0 ** scale)
print(table[0:1].to_pydict())
