# Turbulence and trading

On what we talked about on Skype on 5/11-2020:

[This post](https://towardsdatascience.com/turbulence-and-financial-markets-e24775421835)
compares turbulence in wind with price movements on a financial marked. In brief:

In turbulence, the variance in wind speed between two points at a distance $l$ from each other is given by Kolmogorov's velocity spectrum:

$$ \mathrm{Var}[V](l) = \langle[V(r+l) - V(r)]^2\rangle \propto l^{\frac{2}{3}}, $$

where $$\langle \rangle$$ takes the mean value.

If one observes from a stationary point that the wind passes, it can roughly be transformed from distance to time:

$$
\mathrm{Var}[V](\Delta t) = \langle[V(t+\Delta t) - V(t)]^2\rangle \propto \Delta t^{\frac{2}{3}} \Rightarrow
$$

$$
\sigma[V](\Delta t) \propto \Delta t^{\frac{1}{3}}
$$

where $\sigma = \sqrt{\mathrm{Var}}$ is the standard deviation. In practise it also depends on the Reynolds number - the formula is correct for high Re.

They compare with the price trends on the stock marked (S&P 500), with the price $Y$ analogous to the velocity $V$. They also find a power law, but with a different exponent:

$$
\sigma[Y](\Delta t) \propto \Delta t^{0.53}.
$$

From my Ph.D. I know that different systems (e.g. an ionized fluid) have different exponents, so it is not surprising to find another value for the exponent in another system.

Finally they also find that Y has (very nearly) the same power spectrum as a random walk. A power spectrum is the distribution over frequency of power (energy per time) in a signal. For a stochastic process, it is the distribution of variance instead of power.

I would have liked to look at the references, but unfortunately I can't access articles in Science, and the book he references is only partly available as a Google Books preview. We need to hack a university, use a proxy service or finish me and Siemen's bachelor project - and build a respected, open access, peer-reviewed platform for all scientific litterature ;).
