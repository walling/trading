"""
    Please install zstd library before:

        pip3 install zstandard

    To run this script directly:

        python3 -m spikes.bjarke.order_book_data.frames

    This decompresses the compressed flat file containing raw market update messages.
    Each message is stored in a frame with the following format:

        | field   | size           |
        | :------ | :------------- |
        | length  | 2 bytes        |
        | message | _length_ bytes |
        | crc     | 4 bytes        |

    Integers (length and crc) are stored in unsigned big-endian format.

    In order to parse the file really fast, it preallocates a fixed buffer of 64 MiB.
    Care has to be given by the caller to use the emitted data frame immediately or
    convert it to an immutable byte object using `bytes(frame)`. The last option will
    in turn allocate memory for the frame.
"""

import binascii
import zstandard as zstd

BUFFER_SIZE = 64 * 1024 * 1024


def iter_frames_for_handle(file):
    """
    Iterate data frames from an uncompressed file. Example:

    >>> with open("uncompressed_file", "rb") as file:
    ...     for frame in iter_frames_for_handle(file)
    ...          print(frame)
    b"hello world uncompressed 1"
    b"hello world uncompressed 2"
    """

    # Allocate fixed buffer that we read data into and parse.
    # This puts a limit to the number of byte objects to allocate. Performance optimization.
    buf = bytearray(BUFFER_SIZE)
    view = memoryview(buf)
    view_size = 0

    while True:
        # Read next chunk into buffer
        read_size = file.readinto(view[view_size:])
        if not read_size:
            break

        # Setup memoryview to parse
        view_size += read_size
        pos = 0

        # Parse everything until 65 KiB before buffer ends.
        # Unless buffer is smaller, then parse everything.
        # This should be safe, since messages can only be 64 KiB (2 byte length field).
        bail_size = max(65 * 1024, view_size - 65 * 1024)

        # Parse as many frames as possible in the current buffer
        while pos < bail_size:
            # Read length: 2 bytes
            pos1 = pos
            pos2 = pos1 + 2
            length = int.from_bytes(view[pos1:pos2], byteorder="big", signed=False)

            # Read message bytes: length bytes
            pos3 = pos2 + length
            frame = view[pos2:pos3]

            # Read crc code: 4 bytes
            pos4 = pos3 + 4
            crc = int.from_bytes(view[pos3:pos4], byteorder="big", signed=False)
            pos = pos4

            # Warn if crc code does not match
            if crc != binascii.crc32(view[pos1:pos3]):
                print(f"invalid crc32: {crc} != {binascii.crc32(view[pos1:pos3])}")
                continue

            # Emit valid frame
            yield frame

        # Move the remaining unparsed bytes to the front of the buffer
        remaining_size = view_size - pos
        buf[0:remaining_size] = view[pos:view_size]
        view_size = remaining_size


def iter_frames(filename):
    """
    Iterate data frames from a zstd compressed file. Example:

    >>> for frame in iter_frames("compressed_file.zst")
    ...     print(frame)
    b"hello world compressed 1"
    b"hello world compressed 2"
    """

    with open(filename, "rb") as compressed_file:
        decompressor = zstd.ZstdDecompressor()
        with decompressor.stream_reader(compressed_file) as file:
            yield from iter_frames_for_handle(file)


# Example and test run code.
# It displays the number of frames as well as their cumulative size.
if __name__ == "__main__":
    import time

    filename = "data/2020-12-13_cryptowatch_messages.dat.zst"
    count = 0
    size = 0
    t0 = t_start = time.time()

    print("      time      count           size")
    print("---------- ---------- --------------")

    for frame in iter_frames(filename):
        count += 1
        size += len(frame)

        if count % 1000000 == 0:
            t1 = time.time()
            t = t1 - t0
            t0 = t1
            print("%8.3f s %10d %12d b" % (t, count, size))

    print("========== ========== ==============")
    print("%8.3f s %10d %12d b" % (time.time() - t_start, count, size))
