"""Fluid moments of a 2D2V distribution function fdistribu[species, x, y, vx, vy]."""

import dask.array as da


def fluid_moments(fdistribu, vx, vy):
    """Density n, mean velocity (ux, uy) and temperature T, dask arrays of shape (species, x, y).

    fdistribu, vx and vy are dask arrays, vx and vy the uniform velocity meshes.
    n = int f dv, u = int v f dv / n and T = int |v - u|^2 f dv / (2 n), the last one
    obtained from the raw moments as (int |v|^2 f dv / n - |u|^2) / 2. Velocities and
    temperature are in the units of the velocity mesh.
    """
    dv = (vx[1] - vx[0]) * (vy[1] - vy[0])
    vx = vx[None, None, None, :, None]
    vy = vy[None, None, None, None, :]
    axes = (3, 4)
    n = da.sum(fdistribu, axis=axes) * dv
    ux = da.sum(fdistribu * vx, axis=axes) * dv / n
    uy = da.sum(fdistribu * vy, axis=axes) * dv / n
    v2 = da.sum(fdistribu * (vx ** 2 + vy ** 2), axis=axes) * dv / n
    return {"n": n, "ux": ux, "uy": uy, "T": 0.5 * (v2 - ux ** 2 - uy ** 2)}
