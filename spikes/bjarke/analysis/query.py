from typing import List, Optional
from dataset import QueryBuilder


def query_trades(
    market: str,
    period: Optional[str] = None,
    columns: List[str] = ["time", "price"],
):
    query = QueryBuilder().markets(market).trades()
    if period:
        query.period(period)
    if columns:
        query.columns(columns)
    return query
