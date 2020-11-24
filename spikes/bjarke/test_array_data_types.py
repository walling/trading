import pyarrow as pa
import numpy as np
from decimal import Decimal

# FIELD_X = pa.field("x", pa.uint64(), nullable=False)
# FIELD_Y = pa.field("y", pa.int32(), nullable=False)

# x = pa.array([2] * 10000, type=pa.int64())
# y = pa.array([5] * 10000, type=FIELD_Y.type)

# xy = pa.StructArray.from_arrays((x, y), fields=(FIELD_X, FIELD_Y))
# print(xy)


print(pa.array([Decimal("4.1")] * 10000 + [Decimal("5.005")]).type)
