# On Exchanges

## Order book

Buyers place 'buy' limit orders on the exchange, at a bid price, and with a volume. Sellers place 'sell' limit orders, with an ask price and a volume. Both kinds are added to the order book. (1)

## Bid price

The bid price is the maximum price the buyer is willing to buy coins for. (2)

## Ask price

The ask price is the minimum price the seller is willing to sell for. (2)

## Bid-ask spread

The bid-ask spread is the difference between the highest bid price and the lowest ask price currently in the order book. (3)

## Execution

A 'buy' order is executed only when the ask price is at or below the bid price of the buy. (4)

## Market orders

Buyers and sellers can also place a market order. It is an offer to buy at the current lowest ask price or sell at the current highest bid price.

Note that a marked order buy will be at the lowest ask price. It is the same or higher than the last transaction price, which is typically reported as the current asset price.

It may be that a 'buy' marked order is only partially filled at the current lowest ask price. The rest of the order will then be filled at second-lowest, third-lowest etc. ask price until the entire 'buy' order volume is filled. (5)

## Questions

In what order are 'buy's executed? Say the lowest ask price drops below the bid price of several 'buy' bid prices, and the ask volume is not large enough to supply all 'buy' bids. Who gets to trade, and who doesn't? Is it first-come first-serve? Does the exact bid price matter?

What happens if a 'buy' bid price matches the ask price, but the 'sell' order volume is insufficient? Is part of the 'buy' order filled? Does the order go to another 'buy' order if one came in later, also with a high enough price, but with a small enough volume? Should we think of an order with volume v as v separate orders of unit volume, or as a single order that can only be processed in one go? Is this true for both sell and buy orders? If they are both single orders, is it then only possible to execute a buy if it matches a sell with the same volume?

At what price does a pair of limit orders execute if the bid price is higher than the ask price?

## References

1. [Crypto-currency exchanges](https://medium.com/luno/how-do-cryptocurrency-exchanges-work-80ab97ba9c02)

2. [Bid and ask](https://finance.zacks.com/bid-ask-work-stock-trading-1156.html)

3. [Bid ask spread](https://www.investopedia.com/terms/b/bid-askspread.asp)

4. [Execution](https://www.investopedia.com/ask/answers/050515/when-buy-limit-order-executed.asp)

5. [Market order](https://www.investopedia.com/terms/m/marketorder.asp)

6. [An OSS exchange](https://github.com/mit-dci/opencx)

7. [Another OSS exchange](https://github.com/udokmeci/opentrade-1)
