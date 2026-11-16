"""In-situ analytics (deisa) for gys_deisa_bench, launched next to the simulation.

- Every Output.nbiter_diag iterations:
  - fluid moments of fdistribu (n, ux, uy, T), written to fluid_moments/fluid_moments_iter<iter>.h5;
  - fifth-order time derivative of the fdistribu hyperblock, wherever its stencil is
    available, written to hyperblock_dfdt/dfdistribu_dt_iter<iter>.h5.
- Every Output.nbiter_pca iterations: mergeable PCA of fdistribu.

What the simulation shares and when is set in simulations/deisa_bench/params/pdi_out_diags.yaml.
"""

import threading
import time
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr
from deisa.core import Window
from deisa.dask import Deisa
from distributed.diagnostics import MemorySampler

from processing.finite_differences import FD_POINTS, FD_TARGET_INDEX, derivative_weights
from processing.hyperblock import HYPERBLOCK_SPECIES, hyperblock_slices
from processing.moments import fluid_moments

# Absolute paths, as the Dask workers write into these files too
FLUID_MOMENTS_DIR = Path("fluid_moments").resolve()
HYPERBLOCK_DIR = Path("hyperblock_dfdt").resolve()
for directory in (FLUID_MOMENTS_DIR, HYPERBLOCK_DIR):
    directory.mkdir(parents=True, exist_ok=True)

timers = {"diagnostics": 0.0, "hyperblock": 0.0, "pca": 0.0}
_TIMERS_LOCK = threading.Lock()


def _add_time(name, t0):
    with _TIMERS_LOCK:
        timers[name] += time.time() - t0


# ---------------------------------------------------------------------------
# Deisa analytics callbacks - receive data assembled from all MPI ranks
# ---------------------------------------------------------------------------

deisa = Deisa()


@deisa.register("fdistribu", "deltat", "MeshX", "MeshY", "MeshVx", "MeshVy")
def compute_fluid_moments(fdistribu, deltat, MeshX, MeshY, MeshVx, MeshVy):
    """Fluid moments of fdistribu at this diagnostic step."""
    t0 = time.time()
    iteration = int(fdistribu[-1].t)
    moments = fluid_moments(fdistribu[-1], MeshVx[-1], MeshVy[-1])
    dataset = xr.Dataset(
        {name: (("species", "x", "y"), field) for name, field in moments.items()},
        coords={"species": range(fdistribu[-1].shape[0]), "x": MeshX[-1], "y": MeshY[-1]},
        attrs={"iter": iteration, "time": iteration * float(deltat[-1][0])},
    )
    dataset.to_netcdf(FLUID_MOMENTS_DIR / f"fluid_moments_iter{iteration:05d}.h5", engine="h5netcdf")
    _add_time("diagnostics", t0)


@deisa.register(Window("fdistribu_hyperblock", FD_POINTS), "fdistribu", "deltat",
                "MeshX", "MeshY", "MeshVx", "MeshVy")
def write_hyperblock_derivative(fdistribu_hyperblock, fdistribu, deltat, MeshX, MeshY, MeshVx, MeshVy):
    """Fifth-order time derivative of the hyperblock, wherever its stencil is available.

    The window holds the 4D (x, y, vx, vy) hyperblock of the last FD_POINTS diagnostic
    steps. Once it is full, the derivative is taken at its step FD_TARGET_INDEX, from the
    steps 3 before to 2 after it, and written to hyperblock_dfdt/dfdistribu_dt_iter<iter>.h5
    with the mesh coordinates of the hyperblock. The first 3 and last 2 diagnostic steps
    of the run have no derivative.
    """
    if len(fdistribu_hyperblock) < FD_POINTS:
        return
    t0 = time.time()
    iters = [int(block.t) for block in fdistribu_hyperblock]
    target = iters[FD_TARGET_INDEX]
    dt = float(deltat[-1][0])
    weights = derivative_weights([it * dt for it in iters], target * dt)
    dfdt = sum(w * block for w, block in zip(weights, fdistribu_hyperblock))
    x, y, vx, vy = hyperblock_slices(fdistribu[-1].shape, fdistribu[-1].chunksize)
    dataset = xr.Dataset(
        {"dfdistribu_dt": (("x", "y", "vx", "vy"), dfdt)},
        coords={"x": MeshX[-1][x], "y": MeshY[-1][y], "vx": MeshVx[-1][vx], "vy": MeshVy[-1][vy]},
        attrs={
            "iter": target,
            "time": target * dt,
            "species": HYPERBLOCK_SPECIES,
            "derivative_iters": iters,
            "derivative_order": FD_POINTS - 1,
        },
    )
    dataset.to_netcdf(HYPERBLOCK_DIR / f"dfdistribu_dt_iter{target:05d}.h5", engine="h5netcdf")
    _add_time("hyperblock", t0)


@deisa.register("fdistribu_pca")
def compute_mergeable_pca(fdistribu_pca):
    """Mergeable PCA of fdistribu, every Output.nbiter_pca iterations.

    Placeholder for the mergeable PCA of the upcoming deisa-dask release.
    """
    t0 = time.time()
    iteration = int(fdistribu_pca[-1].t)

    # TODO: mergeable PCA of fdistribu_pca[-1] with the new deisa-dask API.
    print(f"Mergeable PCA placeholder reached for iter {iteration}", flush=True)

    _add_time("pca", t0)


samplers = {
    "all_callbacks": MemorySampler(),
    "spilled_memory": MemorySampler(),
}

with (
    samplers["all_callbacks"].sample("all_callbacks"),
    samplers["spilled_memory"].sample("spilled_memory", measure="spilled"),
):
    t0 = time.time()

    deisa.execute_callbacks()

    print("Time analytics:", time.time() - t0, flush=True)
    print("Time diagnostics:", timers["diagnostics"], flush=True)
    print("Time hyperblock:", timers["hyperblock"], flush=True)
    print("Time pca:", timers["pca"], flush=True)

plots = {
    "all_callbacks": "mem_consumption_analytics.png",
    "spilled_memory": "spilled_mem_analytics.png",
}
for name, filename in plots.items():
    samplers[name].plot()
    plt.savefig(filename)
    plt.clf()

csv_files = {
    "all_callbacks": "memory_samples.csv",
    "spilled_memory": "memory_spilled_samples.csv",
}
for name, filename in csv_files.items():
    df = pd.DataFrame(
        samplers[name].samples[name],
        columns=["time", "memory"],
    )
    df.to_csv(filename, index=False)
