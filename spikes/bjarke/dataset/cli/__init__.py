from ..source import KrakenRESTSource


class CLI:
    def __init__(self):
        pass

    def main(self):
        source = KrakenRESTSource()
        print(source.trades("btc/usd").to_pandas())
