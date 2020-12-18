"""
    Please install cryptowatch sdk library before:

        pip3 install cryptowatch-sdk

    To run this script directly:

        python3 -m spikes.bjarke.order_book_data.messages

    Using `frames.py` to iterate frames of a given flat file and then deserialize
    those as messages using the built-in protobuf schema from cryptowatch.

    The parsing process is quite slow unfortunately, about 10 times as slow as
    just parsing out the raw frames.
"""

from cryptowatch.stream.proto.public.stream import stream_pb2


def iter_messages(frames):
    """
    Iterate market update messages, deserialized from data frames. Example:

    >>> frames = [b"\n\x02\x08\x01"]
    >>> for message in iter_messages(frames):
    ...     print(message)
    authenticationResult {
      status: AUTHENTICATED
    }
    """

    index = -1
    for frame in frames:
        index += 1

        # Ignore first two messages; see `raw-order-book-data.md`
        if index < 2:
            continue

        # Parse frame as message and emit it
        message = stream_pb2.StreamMessage()
        message.ParseFromString(frame)
        yield message


# Example and test run code.
# It displays the number of messages in total and for each type.
if __name__ == "__main__":
    import time
    from .frames import iter_frames

    filename = "data/2020-12-13_cryptowatch_messages.dat.zst"
    count = 0
    count_trade = 0
    count_snapshot = 0
    count_delta = 0
    count_spread = 0
    t0 = t_start = time.time()

    print("      time      count   #trade  #snpsht   #delta  #spread")
    print("---------- ---------- -------- -------- -------- --------")

    for message in iter_messages(iter_frames(filename)):
        count += 1

        if str(message.marketUpdate.tradesUpdate):
            count_trade += 1
        if str(message.marketUpdate.orderBookUpdate):
            count_snapshot += 1
        if str(message.marketUpdate.orderBookDeltaUpdate):
            count_delta += 1
        if str(message.marketUpdate.orderBookSpreadUpdate):
            count_spread += 1

        if count % 100000 == 0:
            t1 = time.time()
            t = t1 - t0
            t0 = t1
            print(
                "%8.3f s %10d %8d %8d %8d %8d"
                % (t, count, count_trade, count_snapshot, count_delta, count_spread)
            )

    print("========== ========== ======== ======== ======== ========")
    print(
        "%8.3f s %10d %8d %8d %8d %8d"
        % (t, count, count_trade, count_snapshot, count_delta, count_spread)
    )
