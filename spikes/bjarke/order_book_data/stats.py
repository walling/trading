"""
    To run this script directly:

        python3 -m spikes.bjarke.order_book_data.stats

    Using `frames.py` and `messages.py` to iterate messages and gather some basic
    statistics like number of fetched exchanges, instruments, and markets. It also
    displays the number of messages per exchange.
"""

# Gather some basic statistics from the parsed messages.
if __name__ == "__main__":
    from .frames import iter_frames
    from .messages import iter_messages

    filename = "data/2020-12-13_cryptowatch_messages.dat.zst"
    count = 0
    exchange_counts = {}
    instruments = set()
    markets = set()

    print("     count #exch #inst #mark  exchange counts")
    print("---------- ----- ----- -----  ---------------")

    for message in iter_messages(iter_frames(filename)):
        count += 1

        market = message.marketUpdate.market
        exchange_id = market.exchangeId
        instrument_id = market.currencyPairId
        market_id = market.marketId

        exchange_count = exchange_counts.get(exchange_id, 0)
        exchange_counts[exchange_id] = exchange_count + 1

        instruments.add(instrument_id)
        markets.add(market_id)

        if count % 100000 == 0:
            print(
                "%10d %5d %5d %5d  %r"
                % (
                    count,
                    len(exchange_counts),
                    len(instruments),
                    len(markets),
                    exchange_counts,
                )
            )

    print("========== ===== ===== =====  ===============")
    print(
        "%10d %5d %5d %5d  %r"
        % (
            count,
            len(exchange_counts),
            len(instruments),
            len(markets),
            exchange_counts,
        )
    )
