from ..source import source_instance


class CLI:
    def __init__(self):
        pass

    def main(self):
        source = source_instance("kraken_rest")
        print(source.trades("btc/usd").to_pandas())
