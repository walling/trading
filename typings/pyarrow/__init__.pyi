from typing import Optional, List, Any, Iterator, overload

class DataType: ...
class Field: ...
class Schema: ...
class TimestampType(DataType): ...
class StructType(DataType): ...

def timestamp(unit, tz: Optional[str] = None) -> TimestampType: ...
def uint8() -> DataType: ...
def uint16() -> DataType: ...
def uint32() -> DataType: ...
def uint64() -> DataType: ...
def int8() -> DataType: ...
def int16() -> DataType: ...
def int32() -> DataType: ...
def int64() -> DataType: ...
def float32() -> DataType: ...
def float64() -> DataType: ...
def dictionary(key: DataType, value: DataType) -> DataType: ...
def string() -> DataType: ...
def binary(length: int = -1) -> DataType: ...
def field(name: str, type: DataType, nullable: bool = False) -> Field: ...
def struct(fields: List[Field]) -> StructType: ...
def schema(
    fields: List[Field], metadata: Optional[Dict[str, str]] = None
) -> Schema: ...

class Scalar: ...

class Array:
    dictionary: Array  # todo: should be only on StructArray
    indices: dictionary  # todo: should be only on StructArray
    def dictionary_encode(self) -> "Array": ...
    def __len__(self) -> int: ...
    def cast(self, type: DataType): ...

_CA = TypeVar("CA")

class ChunkedArray(Generic[_CA]):
    def dictionary_encode(self) -> "ChunkedArray": ...
    def chunk(self, index: int) -> CA: ...
    def iterchunks(self) -> Iterator[CA]: ...

class Table:
    def __len__(self) -> int: ...
    @staticmethod
    def from_arrays(
        arrays: List[Array],
        names: Optional[List[str]] = None,
        metadata: Optional[dict[str, str]] = None,
    ) -> Table: ...
    @staticmethod
    def from_pandas(frame: Any) -> Table: ...
    def to_pandas() -> Any: ...
    @overload
    def __getitem__(self, key: str) -> ChunkedArray: ...
    @overload
    def __getitem__(self, key: slice) -> Table: ...
    @property
    def schema(self) -> Schema: ...
    def drop(self, columns: List[str]) -> Table: ...

def scalar(value: Any, type: Optional[DataType] = None) -> Scalar: ...
def array(values: List[Any], type: Optional[DataType] = None) -> Array: ...
def concat_tables(tables: List[Table]) -> Table: ...
def chunked_array(arrays: List[_CA]) -> ChunkedArray[CA]: ...
def table(values: Dict[str, Any], names: Optional[str] = None) -> Table: ...
