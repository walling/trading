# Raw order book data

On 2020-12-13 at around 9:00 UTC, I fetched order book data from five exchanges through the Cryptowatch provider. They receive, clean up, and aggregate trading data from a large number of crypto exchanges.

The five selected exchanges were:

|  Id | Exchange | Year | Country             | Rank \* | Note                             |
| --: | :------- | :--: | :------------------ | :-----: | :------------------------------- |
|  27 | binance  | 2017 | Malta               |    1    | Most popular exchange currently  |
|   1 | bitfinex | 2012 | Hong Kong           |    2    | Long-running popular exchange    |
|  96 | ftx      | 2019 | Antigua and Barbuda |    3    | Recent futures exchange          |
|   4 | kraken   | 2011 | United States       |    4    | My most familiar exchange        |
|  12 | okcoin   | 2013 | China               |   18    | Were better ranked on 2020-12-13 |

\*) Rank by traded volume on Cryptowach on 2020-12-17. <https://cryptowat.ch/exchanges>

## Binary frames

Through Cryptowatch you can fetch data using a HTTP REST API or almost realtime through their websocket API. The websocket packet format is based on either JSON or Protobuf, the last saving a lot of bandwidth. Since you pay a small fee per consumed byte, the lower bandwidth the better.

I created a small script, which fetched the raw data in Protobuf format and saved it directly to a flat records file. Afterwards, the file was compressed using [zstd][] and shared in the data folder. Simply run `./tools/sync.sh` to get the file. The format of the decompressed file is a continous stream of records, each record having the following layout:

[zstd]: http://www.zstd.net

| Field  | Size           |
| :----- | :------------- |
| length | 2 bytes        |
| frame  | _length_ bytes |
| crc    | 4 bytes        |

The next frame is then appended on top of the previous, etc. Reading this format requires one to read the length (next 2 bytes), then read the frame (next _length_ bytes), and finally the crc code (next 4 bytes). The crc code is used to verify the correctness of the frame. It’s calculated as `CRC32(length_bytes + frame_bytes)`, this can be done using the `binascii.crc32` function in Python.

> **Example:** `spikes/bjarke/order_book_data/frames.py`

### Fast file parsing

In order to parse the file as fast as possible in Python, you need to do as few copies as possible. That means using the `memoryview` object and a preallocated `bytearray` as buffer. Still, most time is spent deserialized the frame.

### Parsing messages

Each message is Protobuf encoded using a schema provided by Cryptowatch. Each frame needs to be deserialized. This can be done as follows:

```python
# pip3 install cryptowatch-sdk
from cryptowatch.stream.proto.public.stream import stream_pb2

message = stream_pb2.StreamMessage()
message.ParseFromString(message_bytes)

print(message)
```

> **Example:** `spikes/bjarke/order_book_data/messages.py`

For it to work, you need to install the [cryptowatch-sdk][] package.

[cryptowatch-sdk]: https://pypi.org/project/cryptowatch-sdk/

## Market metadata

Cryptowatch use integer identifiers for various object types. For human readable, these identifiers can be translated to symbols formatted according to [data-models.md][]. The mappings are stored in a JSON file. Here is the overall structure:

[data-models.md]: ./data-models.md

```json
{
    "exchanges": {
        "1": "bitfinex",
        "4": "kraken",
        "12": "okcoin",
        "27": "binance",
        "96": "ftx"
        /* ... */
    },
    "instruments": {
        "1": "eth/php",
        "2": "ric/btc",
        "3": "vtc/btc",
        "4": "eos/usd",
        "5": "etp/btc"
        /* ... */
    },
    "markets": {
        "1": "bitfinex:btc/usd",
        "2": "bitfinex:ltc/usd",
        "3": "bitfinex:ltc/btc",
        "4": "bitfinex:eth/usd",
        "5": "bitfinex:eth/btc"
        /* ... */
    }
}
```

## Messages

Only four types of market update messages were collected. In total 11480532 market updates were collected. Every message only relates to a single market. This is represented through the `marketUpdate.market` property, which contains three sub-properties:

| Market property  | Description                                                   |
| :--------------- | :------------------------------------------------------------ |
| `exchangeId`     | exchange id                                                   |
| `currencyPairId` | instrument id (_pair_ is an [obsolete name for instrument][]) |
| `marketId`       | market id                                                     |

[obsolete name for instrument]: https://docs.cryptowat.ch/rest-api/pairs

The ids can be looked up in the market metadata JSON, read the above section.

The 4 market update message types are:

| Type          | #Messages | Percent | Description                              |
| :------------ | --------: | ------: | :--------------------------------------- |
| trade         |    439059 |   3.8 % | One or more trades in a given market     |
| book snapshot |    129026 |   1.1 % | Full order book, only sent once a minute |
| book delta    |   7891065 |  68.7 % | Delta update for current order book      |
| spread        |   3021382 |  26.3 % | The best bid/ask prices and amounts      |

In order to get the complete order book in realtime, an order book object needs to be maintained over time for every market. Every time a order book snapshot message is sent, it can be fully replaced. Whenever a delta message is sent, the delta is then applied to the most recent order book object for the given market.

### Stats

The gathered market update messages were from 5 exchanges, covered 1883 instruments, and 2172 markets in total. Number of messages per exchange:

| Exchange | #Messages | Percent | Volume rank \* |
| :------- | --------: | ------: | :------------: |
| binance  |   4942003 |  43.0 % |       1        |
| ftx      |   2610729 |  22.7 % |       3        |
| kraken   |   2071002 |  18.0 % |       4        |
| bitfinex |   1704825 |  14.8 % |       2        |
| okcoin   |    151973 |  01.3 % |       18       |

> **Example:** `spikes/bjarke/order_book_data/stats.py`

### Messages to ignore

The first two messages in the flat data file are to be ignored. The first one is simple the authentication result:

```
authenticationResult {
  status: AUTHENTICATED
}
```

The second one is a confirmation of the channels that were subscribed to over the websocket connection. Snippet:

```
subscriptionResult {
  status {
    keys: "exchanges:4:trades"
    keys: "exchanges:4:book:snapshots"
    keys: "exchanges:4:book:deltas"
    keys: "exchanges:4:book:spread"
    // ...
    subscriptions {
      stream_subscription {
        resource: "exchanges:4:book:snapshots"
      }
    }
    // ...
  }
  // ...
}
```

### Trade message

Example trade message:

```
marketUpdate {
  tradesUpdate {
    trades {
      timestamp: 1607848390
      priceStr: "3.2582"
      amountStr: "35"
      timestampNano: 1607848390407000000
      externalId: "OMGUSDT:7704720"
      orderSide: BUYSIDE
    }
    trades {
      timestamp: 1607848390
      priceStr: "3.255"
      amountStr: "252.37"
      timestampNano: 1607848390417000000
      externalId: "OMGUSDT:7704724"
      orderSide: SELLSIDE
    }
  }
  market {
    exchangeId: 27
    currencyPairId: 551
    marketId: 59573
  }
}
```

### Book snapshot message

Example order book snapshot message:

```
marketUpdate {
  orderBookUpdate {
    bids {
      priceStr: "28.718"
      amountStr: "6.968"
    }
    bids {
      priceStr: "28.701"
      amountStr: "13"
    }
    bids {
      priceStr: "28.7"
      amountStr: "8.251"
    }
    // ...
    asks {
      priceStr: "28.761"
      amountStr: "10"
    }
    asks {
      priceStr: "28.762"
      amountStr: "7.759"
    }
    asks {
      priceStr: "28.77"
      amountStr: "1.439"
    }
    // ...
    seqNum: 82312
  }
  market {
    exchangeId: 27
    currencyPairId: 176598
    marketId: 63534
  }
}
```

### Book delta message

Example order book delta message:

```
marketUpdate {
  orderBookDeltaUpdate {
    bids {
      set {
        priceStr: "2.1689"
        amountStr: "2143.8907"
      }
      set {
        priceStr: "2.1678"
        amountStr: "61.80452572"
      }
      // ...
      removeStr: "2.1686"
      removeStr: "2.1617"
      // ...
    }
    asks {
      set {
        priceStr: "2.1731"
        amountStr: "55.53662225"
      }
      set {
        priceStr: "2.1734"
        amountStr: "19.24339092"
      }
      // ...
      removeStr: "2.173"
      removeStr: "2.1739"
      // ...
    }
    seqNum: 1224028
  }
  market {
    exchangeId: 1
    currencyPairId: 1519
    marketId: 5165
  }
}
```

### Spread message

Example spread message:

```
marketUpdate {
  orderBookSpreadUpdate {
    timestamp: 1607848390558
    bid {
      priceStr: "78.63"
      amountStr: "20"
    }
    ask {
      priceStr: "78.65"
      amountStr: "1240"
    }
  }
  market {
    exchangeId: 27
    currencyPairId: 176615
    marketId: 136148
  }
}
```

### Message schemas

The [detailed message definitions][] can be found online in the Github repository (Proto3 format).

[detailed message definitions]: https://github.com/cryptowatch/cw-sdk-python/blob/master/proto/public/markets/market.proto

## Protobuf behaviour

The protobuf library in Python have some strange behaviours, that one needs to get used to. Any property can be referenced, even if it doesn’t exist. In order to test a property, you need to serialize the value as a string. Example:

```python
# Test if we received a order book snapshot message
if str(message.marketUpdate.orderBookUpdate):
    print(message)
```

Repeated values are represented as lists in Python. Example:

```python
# List all bids of the order book message
for bid in message.marketUpdate.orderBookUpdate.bids:
    print(bid.priceStr)
    print(bid.amountStr)
```

The final leaf properties are represented by their equivalent Python value, e.g. `int` or `str`. Example:

```python
# Display market id of message
print(message.marketUpdate.market.marketId)
print(type(message.marketUpdate.market.marketId))  # => int
```

## Order book aggregation

Looking through the data, I observe that the order book deltas are heavily aggregated. This is unfortunate, because it makes it tricky or maybe even impossible to reverse-engineer the origin orders. Maybe it’s possible, if the time resolution on the delta messages is high enough. One problem is: If two trades executed at the same price, their cumulative volume is most probably aggregated into a single price bin in the order book delta message. By watching the changes in the order book and the stream of trade message, one might be able to infer some information. I’m not sure if it’s possible and how much information can be inferred.

A question also arrives, whether Cryptowatch deliberatly aggregates the order book deltas (one potential reason being performance). If this is the case, it might be possible to get better data directly from the exchange websocket APIs. However, accessing the data directly adds to the complexity of data cleaning and normalization. This is what Cryptowatch tries to solve in the first place, a single unified stream of market data with the same conventions applied everywhere.

## Timestamp estimation

Only the trade and spread messages contain timestamps. The order book messages (both snapshot and delta) contain a sequence number `seqNum`, which is increased by 1 for every message in a given market. This is used to synchronize the deltas with the order book snapshot messages. However, no timestamp is given. I speculate, that it would be possible to estimate the time, using the trade and spread messages in a given market. Let’s say five messages are sent:

|   # | Message       |  Timestamp   |
| --: | :------------ | :----------: |
|   1 | spread        | 10:47:12.000 |
|   2 | book snapshot |     n/a      |
|   3 | book delta    |     n/a      |
|   4 | book delta    |     n/a      |
|   5 | spread        | 10:47:13.000 |

It would be reasonable to assume that the messages 2, 3, and 4, were reflected on the exchange between 10:47:12.000 and 10:47:13.000. We can assign estimated timestamps with an error of up to 1 second, in this case.
