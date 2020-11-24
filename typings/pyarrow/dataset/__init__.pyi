from typing import Optional, Union, List
from os import PathLike

def dataset(
    path: Union[str, PathLike],
    filesystem=None,
    format: Optional[str] = None,
    partitioning: Optional[str] = None,
): ...
def field(name): ...
def partitioning(field_names: Optional[List[str]] = None): ...
