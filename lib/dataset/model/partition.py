from .types import Market, TimeInterval


class Partition:
    """
    Represents a dataset partion, i.e. market over a given period (time interval) downloaded on a given source.

    >>> btc_eur = Market("kraken", "btc/eur")
    >>> august2015 = TimeInterval("2015-08-01", "2015-09-01")
    >>> my_dataset = Partition("kraken_rest", btc_eur, august2015)
    >>> my_dataset
    Partition('kraken_rest', Market('kraken', 'btc/eur'), TimeInterval('2015-08Z', '2015-09Z'))
    >>> str(my_dataset)
    'kraken_rest:kraken:btc/eur:2015-08Z/2015-09Z'
    >>> my_dataset.source
    'kraken_rest'
    >>> my_dataset.market.symbol
    'kraken:btc/eur'
    >>> my_dataset.period.start
    Timestamp('2015-08-01 00:00:00+0000', tz='UTC')
    >>> my_dataset.period.end
    Timestamp('2015-09-01 00:00:00+0000', tz='UTC')
    """

    def __init__(self, source: str, market: Market, period: TimeInterval):
        self._source = source
        self._market = market
        self._period = period

    @property
    def source(self):
        return self._source

    @property
    def market(self):
        return self._market

    @property
    def period(self):
        return self._period

    def with_period(self, new_period: TimeInterval) -> "Partition":
        return Partition(self._source, self._market, new_period)

    def path(self, subject: str, period_short=False):
        period_formatted = self._period.format(short=period_short)
        period = period_formatted.replace("/", ".").replace(":", "")
        year = str(self._period.start.year)
        exchange = self._market.exchange
        instrument = self._market.instrument.replace("/", "_")
        source = self._source
        parts = [
            subject,
            f"year={year}",
            f"exchange={exchange}",
            f"instrument={instrument}",
            f"source={source}",
            f"{period}.{exchange}.{instrument}.{source}.{subject}.parquet",
        ]
        return "/".join(parts)

    def __str__(self):
        return "%s:%s:%s" % (
            self.source,
            self.market,
            self.period,
        )

    def __repr__(self):
        return "Partition(%r, %r, %r)" % (
            self.source,
            self.market,
            self.period,
        )
