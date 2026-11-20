"""In-situ analytics (deisa) for gys_deisa_bench, launched next to the simulation.

- Every Output.nbiter_diag iterations:
  - fluid moments of fdistribu (n, ux, uy, T), written to fluid_moments/fluid_moments_iter<iter>.h5;
  - fifth-order time derivative of the fdistribu hyperblock, wherever its stencil is
    available, written to hyperblock_dfdt/dfdistribu_dt_iter<iter>.h5.
- Whenever the previous checkpoint has been written: checkpoint of fdistribu, written to
  checkpoint/fdistribu_iter<iter>_<host>.h5 with one file per node.
- Every Output.nbiter_pca iterations: mergeable PCA of fdistribu.

What the simulation shares and when is set in simulations/deisa_bench/params/pdi_out_diags.yaml.

DEISA_VERSION=dask (default) or ray selects the deisa backend; the simulation must use the
same one. deisa-dask runs each callback in its own thread, deisa-ray runs them one after the
other, so with deisa-ray the checkpoint callback only starts the writes and a background
thread acknowledges them once they have finished.
"""

import contextlib
import os
import threading
import time
import traceback
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr

DEISA_VERSION = os.environ.get("DEISA_VERSION", "dask").lower()

if DEISA_VERSION == "ray":
    import ray
    from deisa.ray import Deisa, MergeablePCA
    from deisa.ray.types import Window

    # deisa-ray has no precompute option
    REGISTER_OPTIONS = {}
elif DEISA_VERSION == "dask":
    from deisa.core import Window
    from deisa.dask import Deisa, MergeablePCA
    from distributed.diagnostics import MemorySampler

    REGISTER_OPTIONS = {"precompute": False}
else:
    raise ValueError(f"DEISA_VERSION must be 'dask' or 'ray', got {DEISA_VERSION!r}")

from processing.finite_differences import FD_POINTS, FD_TARGET_INDEX, derivative_weights
from processing.hyperblock import HYPERBLOCK_SPECIES, hyperblock_slices
from processing.moments import fluid_moments
from processing.per_node_hdf5 import submit_per_node_ray, write_per_node

CHECKPOINT_WRITE_DONE = "checkpoint_write_done"

# Absolute paths, as the Dask workers write into these files too
FLUID_MOMENTS_DIR = Path("fluid_moments").resolve()
HYPERBLOCK_DIR = Path("hyperblock_dfdt").resolve()
CHECKPOINT_DIR = Path("checkpoint").resolve()
for directory in (FLUID_MOMENTS_DIR, HYPERBLOCK_DIR, CHECKPOINT_DIR):
    directory.mkdir(parents=True, exist_ok=True)

timers = {"diagnostics": 0.0, "hyperblock": 0.0, "checkpoint": 0.0, "pca": 0.0}
_TIMERS_LOCK = threading.Lock()


def _add_time(name, t0):
    with _TIMERS_LOCK:
        timers[name] += time.time() - t0


def _write_netcdf(dataset, path):
    # Under deisa-ray, xarray cannot write dask arrays lazily: compute them first.
    if DEISA_VERSION == "ray":
        dataset = dataset.compute()
    dataset.to_netcdf(path, engine="h5netcdf")


# Thread acknowledging the last checkpoint started under deisa-ray
checkpoint_thread = None


def _acknowledge_when_written(iteration, writes, t0):
    """Wait for the per-node writes of a deisa-ray checkpoint, then acknowledge it.

    A failed write is reported and acknowledged too, so that the simulation keeps
    launching checkpoints.
    """
    try:
        ray.get(writes)
    except Exception:
        print(f"Checkpoint of iter {iteration} failed:", flush=True)
        traceback.print_exc()
    deisa.set(CHECKPOINT_WRITE_DONE, 1, iteration)
    _add_time("checkpoint", t0)


# ---------------------------------------------------------------------------
# Deisa analytics callbacks - receive data assembled from all MPI ranks
# ---------------------------------------------------------------------------

deisa = Deisa()


@deisa.register("fdistribu", "deltat", "MeshX", "MeshY", "MeshVx", "MeshVy", **REGISTER_OPTIONS)
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
    _write_netcdf(dataset, FLUID_MOMENTS_DIR / f"fluid_moments_iter{iteration:05d}.h5")
    _add_time("diagnostics", t0)


@deisa.register(Window("fdistribu_hyperblock", FD_POINTS), "fdistribu", "deltat",
                "MeshX", "MeshY", "MeshVx", "MeshVy", **REGISTER_OPTIONS)
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
    _write_netcdf(dataset, HYPERBLOCK_DIR / f"dfdistribu_dt_iter{target:05d}.h5")
    _add_time("hyperblock", t0)


@deisa.register("fdistribu", "checkpoint_trigger", "deltat", "MeshX", "MeshY", "MeshVx", "MeshVy",
                **REGISTER_OPTIONS)
def write_checkpoint(fdistribu, checkpoint_trigger, deltat, MeshX, MeshY, MeshVx, MeshVy):
    """Write fdistribu with one file per node, then acknowledge the write.

    Fires at the diagnostic steps where the simulation sends checkpoint_trigger, i.e.
    as soon as the previous checkpoint has been written. The acknowledgement is sent
    even if the write fails, so that the simulation keeps launching checkpoints.
    """
    if DEISA_VERSION == "ray":
        start_checkpoint_ray(fdistribu, deltat, MeshX, MeshY, MeshVx, MeshVy)
        return
    t0 = time.time()
    iteration = int(fdistribu[-1].t)
    try:
        write_per_node(
            deisa.client,
            fdistribu[-1],
            {"x": MeshX[-1], "y": MeshY[-1], "vx": MeshVx[-1], "vy": MeshVy[-1]},
            CHECKPOINT_DIR,
            prefix=f"fdistribu_iter{iteration:05d}",
            attrs={"iter": iteration, "time": iteration * float(deltat[-1][0])},
        )
    except Exception:
        print(f"Checkpoint of iter {iteration} failed:", flush=True)
        traceback.print_exc()
    finally:
        deisa.set(CHECKPOINT_WRITE_DONE, 1, iteration)
        _add_time("checkpoint", t0)


def start_checkpoint_ray(fdistribu, deltat, MeshX, MeshY, MeshVx, MeshVy):
    """Start the per-node writes of the checkpoint, and a thread acknowledging them once
    they have finished, then return without waiting for them."""
    global checkpoint_thread
    t0 = time.time()
    iteration = int(fdistribu[-1].t)
    try:
        writes = submit_per_node_ray(
            fdistribu[-1],
            {"x": MeshX[-1], "y": MeshY[-1], "vx": MeshVx[-1], "vy": MeshVy[-1]},
            CHECKPOINT_DIR,
            prefix=f"fdistribu_iter{iteration:05d}",
            attrs={"iter": iteration, "time": iteration * float(deltat[-1][0])},
        )
    except Exception:
        print(f"Checkpoint of iter {iteration} failed:", flush=True)
        traceback.print_exc()
        deisa.set(CHECKPOINT_WRITE_DONE, 1, iteration)
        _add_time("checkpoint", t0)
        return
    checkpoint_thread = threading.Thread(target=_acknowledge_when_written, args=(iteration, writes, t0), daemon=True)
    checkpoint_thread.start()


@deisa.register("fdistribu_pca", **REGISTER_OPTIONS)
def mergeable_pca(fdistribu_pca):
    """Mergeable PCA of fdistribu, every Output.nbiter_pca iterations."""
    x = fdistribu_pca[-1]

    t_start = time.time()
    pca = MergeablePCA(n_components=3, feature_axes=(3,)).fit(x)
    t_end = time.time()

    print(f"[pca] t={x.timestep} start={t_start:.6f} end={t_end:.6f} duration={t_end - t_start:.3f}s "
          f"explained_ratio={pca.explained_variance_ratio_.round(4).tolist()}", flush=True)
    _add_time("pca", t_start)


# MemorySampler needs a Dask distributed client, which deisa-ray does not have
samplers = {}
if DEISA_VERSION == "dask":
    samplers = {
        "all_callbacks": MemorySampler(),
        "spilled_memory": MemorySampler(),
    }

with contextlib.ExitStack() as sampling:
    if samplers:
        sampling.enter_context(samplers["all_callbacks"].sample("all_callbacks"))
        sampling.enter_context(samplers["spilled_memory"].sample("spilled_memory", measure="spilled"))
    t0 = time.time()

    deisa.execute_callbacks()
    if checkpoint_thread is not None:
        checkpoint_thread.join()

    print("Time analytics:", time.time() - t0, flush=True)
    print("Time diagnostics:", timers["diagnostics"], flush=True)
    print("Time hyperblock:", timers["hyperblock"], flush=True)
    print("Time checkpoint:", timers["checkpoint"], flush=True)
    print("Time pca:", timers["pca"], flush=True)

plots = {
    "all_callbacks": "mem_consumption_analytics.png",
    "spilled_memory": "spilled_mem_analytics.png",
}
for name, filename in plots.items():
    if name not in samplers:
        continue
    samplers[name].plot()
    plt.savefig(filename)
    plt.clf()

csv_files = {
    "all_callbacks": "memory_samples.csv",
    "spilled_memory": "memory_spilled_samples.csv",
}
for name, filename in csv_files.items():
    if name not in samplers:
        continue
    df = pd.DataFrame(
        samplers[name].samples[name],
        columns=["time", "memory"],
    )
    df.to_csv(filename, index=False)
