from typing import Type, cast
import importlib
from ..model.source import Source

known_sources = {
    "kraken_rest": {
        "import": ".kraken_rest.KrakenRESTSource",
        "error": "You must have krakenex installed.",
    },
}


def source_class(source_symbol: str) -> Type[Source]:
    if source_symbol not in known_sources:
        raise ValueError(f"Source not known: {source_symbol}")

    known_source = known_sources[source_symbol]
    if "class" not in known_source:
        mod, name = known_source["import"].rsplit(".", 1)
        try:
            mod = importlib.import_module(mod, __name__)
            known_source["class"] = getattr(mod, name)
        except ImportError as e:
            error = known_source["error"]
            raise ImportError(f"{source_symbol}: {error}") from e

    return cast(Type[Source], known_source["class"])


def source_instance(source_symbol: str) -> Source:
    SourceClass = source_class(source_symbol)
    return SourceClass()
