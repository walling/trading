from ..source import source_instance


class CLI:
    def __init__(self):
        pass

    def main(self):
        source = source_instance("kraken_rest")
        result = source.trades("btc/usd")
        print(result)
        print(result.to_pandas())
