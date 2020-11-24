from typing import Union, TypeVar
from .. import Scalar, Array, ChunkedArray

_Output = TypeVar("Output", Array, ChunkedArray[Any])
_Scalar = Union[int, float, Scalar]
_Input = Union[_Scalar, _Output]

def add(left: _Input, right: _Input) -> _Output: ...
def multiply(left: _Input, right: _Input) -> _Output: ...
def divide(left: _Input, right: _Input) -> _Output: ...
