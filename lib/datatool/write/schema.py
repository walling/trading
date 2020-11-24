import pyarrow as pa

# Modeling decimal values as integer representation and scale (number of fractal digits).
decimal_int = pa.field("int", pa.uint64(), nullable=False)
decimal_scale = pa.field("scale", pa.int32(), nullable=False)
decimal = pa.struct([decimal_int, decimal_scale])

# Modeling external id as prefix plus either numeric or uuid part (any optional).
external_id_prefix = pa.field("prefix", pa.string())
external_id_numeric = pa.field("numeric", pa.uint64())
external_id_uuid = pa.field("uuid", pa.binary(16))
external_id = pa.struct([external_id_prefix, external_id_numeric, external_id_uuid])

# Fields used to partition the dataset.
partition_fields = [
    pa.field("subject", pa.dictionary(pa.int32(), pa.string()), nullable=False),
    pa.field("source", pa.dictionary(pa.int32(), pa.string()), nullable=False),
    pa.field("exchange", pa.dictionary(pa.int32(), pa.string()), nullable=False),
    pa.field("instrument", pa.dictionary(pa.int32(), pa.string()), nullable=False),
]

# Fields used by trades records.
trades_fields = [
    pa.field("external_id", external_id),
    pa.field("time", pa.timestamp("ns", tz="UTC"), nullable=False),
    pa.field("price", decimal, nullable=False),
    pa.field("amount", decimal, nullable=False),
    pa.field("side", pa.dictionary(pa.int32(), pa.string())),
    pa.field("order", pa.dictionary(pa.int32(), pa.string())),
    pa.field("extra_json", pa.string()),
] + partition_fields

# Trades schema.
trades_schema = pa.schema(trades_fields)
