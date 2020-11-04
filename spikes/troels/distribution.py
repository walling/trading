import pyarrow.parquet as pq
import numpy as np
from matplotlib import pyplot as pl
from decimal import Decimal
from datetime import timedelta
from scipy.stats import laplace
import itertools


def plot_distribution(ax, time, price, scale, interval_letter=None, bins=100):

    if interval_letter != None:
        # Sample data in given interval
        rounded = time.astype("datetime64[" + interval_letter + "]")
        interval = np.timedelta64(1, interval_letter)
        i = rounded[:-1] != rounded[1:]
        time = time[:-1][i]
        p = price[:-1][i]
        s = scale[:-1][i]
    else:
        p = price
        s = scale

    # Apply scale to get price as floating point number
    p = p * 10.0 ** (-s)

    # Find percentual changes in price
    dp = (p[:-1] - p[1:]) / p[:-1] * 100

    # Remove outliers
    i = abs(dp) < 0.25
    dp = dp[i]

    # Fit a laplacian distribution, most likely estimate of distribution parameters
    mle = laplace.fit(dp)

    # Axis for fitted distribution
    x = np.linspace(laplace.ppf(0.001, *mle), laplace.ppf(0.999, *mle), 10000)

    # Plot histogram of price changes
    ax.hist(dp, bins=bins, density=True, log=False, label="btc/eur august")

    # Plot fit
    ax.plot(x, laplace.pdf(x, *mle), "-r", label="laplace fit")

    # Add annotation
    ax.legend()
    interval_text = ", all" if interval_letter == None else " over " + str(interval)
    ax.set_xlabel("Price change (%)" + interval_text)
    ax.set_ylabel("Probability density")


table = pq.read_table("../../data/2020_kraken.parquet")
markets = table.schema.metadata[b"markets"].decode().split(",")
market_id = markets.index("kraken:btc/eur")

time = table["time"].chunk(market_id).to_numpy()
price = table["price"].chunk(market_id).to_numpy()
scale = table["price_scale"].chunk(market_id).to_numpy().astype(np.int8)

fig, axs = pl.subplots(2, 2, figsize=(10, 7))
for interval, ax, bins in itertools.zip_longest(
    [None, "s", "m", "h"], itertools.chain(*axs), [200, 200, 100, 30]
):
    plot_distribution(ax, time, price, scale, interval_letter=interval, bins=bins)
pl.show()
