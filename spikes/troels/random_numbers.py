import numpy as np
from numpy import random as r

N = 10000
#bits = 64
bits = 16
largest = 2**(bits - 1)
rng = np.random.default_rng()
ints = rng.integers(-largest, high=largest, size=N)

# Normalize to 64 bit
high = np.max(np.abs(ints))
scale = 2**63/high
ints = ints*scale

ints.tofile("random/random-numbers")
