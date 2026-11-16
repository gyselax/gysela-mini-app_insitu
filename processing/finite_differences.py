"""Fifth-order finite differences in time over the last diagnostic outputs."""

import numpy as np

# With the outputs t_0 < ... < t_5, the derivative is evaluated at t_3 (offsets -3..+2),
# the most centred node of the stencil.
FD_POINTS = 6
FD_TARGET_INDEX = 3


def derivative_weights(times, t):
    """Weights w such that sum_i w[i] * g(times[i]) approximates g'(t).

    These are the derivatives at t of the Lagrange basis polynomials on times.
    The times may be unevenly spaced.
    """
    times = np.asarray(times, dtype=float)
    scale = np.ptp(times) if times.size > 1 else 1.0
    x = (times - t) / scale
    n = x.size
    weights = np.zeros(n)
    for i in range(n):
        for m in range(n):
            if m == i:
                continue
            term = 1.0 / (x[i] - x[m])
            for l in range(n):
                if l not in (i, m):
                    term *= (0.0 - x[l]) / (x[i] - x[l])
            weights[i] += term
    return weights / scale
