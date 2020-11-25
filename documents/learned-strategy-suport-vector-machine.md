# Trading strategy learning using support vector machine

We assume that it is possible to decide, given a partial price history, wether it is a good idea to buy, sell or do nothing.

Lets pick a price history some fixed amount back in time. It can possibly be smoothed over some smoothing length. This is an n-dimentional vector, where n is the number of recorded time steps. Knowing the full price history, we can label such vectors as "should have bought", "should have sold" and "should have done nothing".

Using a support vector machine, we can separate this partial price history space. The method is convex, so is never caught in a local extremum, and using a suitable kernel we can always make sure the space is separable. The space I have chosen here is also high dimensional, which decreases the chance of an unseparable problem. We do have to guard against overfitting.

Once the space is separated, this constitutes a trading strategy, ready to be back-tested.
