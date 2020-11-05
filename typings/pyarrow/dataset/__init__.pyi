from typing import Optional, Union
from os import PathLike

def dataset(
    path: Union[str, PathLike],
    filesystem=None,
    format: Optional[str] = None,
    partitioning: Optional[str] = None,
): ...
def field(name): ...
