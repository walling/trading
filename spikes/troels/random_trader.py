import pyarrow.parquet as pq
from matplotlib import pyplot as pl
import numpy as np

table = pq.read_table("../../data/2020_kraken.parquet")
i = 10
t = table["time"].chunk(i)
p = table["price"].chunk(i)
s = table["price_scale"].chunk(i).to_numpy().astype(np.int8)

price = p.to_numpy()*10.0**(-s)

balance = [1000]  # €
stock = [0]      # btc


def buy(amount_euro, price):
    global balance, stock
    if (balance[-1] >= amount_euro):
        amount_btc = amount_euro/price
        balance.append(balance[-1] - amount_euro)
        stock.append(stock[-1] + amount_btc)


def sell(amount_btc, price):
    global balance, stock
    if (stock[-1] >= amount_btc):
        amount_euro = amount_btc * price
        stock.append(stock[-1] - amount_btc)
        balance.append(balance[-1] + amount_euro)


def noop():
    global balance, stock
    balance.append(balance[-1])
    stock.append(stock[-1])


def trade_random(time, price):
    r = np.random.random(2)
    choice = r[0]
    if (choice < 0.0001):
        amount = r[1]*balance[-1]
        buy(amount, price)
    elif (choice < 0.0002):
        amount = r[1]*stock[-1]
        sell(amount, price)
    else:
        noop()


trade = trade_random

for j in range(len(t)-1):
    T = t[j]
    P = price[j]
    trade(T, P)

balance = np.array(balance)
stock = np.array(stock)

print(balance[-1]+stock[-1]*price[-1])

pl.plot(t, balance)
pl.plot(t, balance + stock*price)
pl.show()
