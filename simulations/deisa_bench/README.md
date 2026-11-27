# GYSELA In-situ Analytics Mini App

2D2V Vlasov–Poisson mini-application (`gys_deisa_bench`) coupled through deisa-dask to in-situ analytics running on a Dask cluster. Default case: Landau damping.

## Files

| File | Role |
| --- | --- |
| `../../src/deisa_bench/gys_deisa_bench.cpp` | C++ mini-app: cold start or restart, exposes `fdistribu` to PDI at every iteration. |
| `params/params_landau_damping.yaml` | Landau-damping input. |
| `params/params_two_stream.yaml` | Two-stream instability input. |
| `params/pdi_out.yaml` | PDI config without analytics. |
| `params/pdi_out_diags.yaml` | PDI config sharing data with the deisa analytics: the pycall plugin sets up the deisa bridge and sends the arrays. |
| `deisa-dask_launch_script.sh` | Starts a Dask scheduler, workers, the analytics and the simulation. |
| `../../processing/diagnostics.py` | Analytics (deisa callbacks), run next to the simulation. |
| `../../processing/moments.py` | Fluid moments of `fdistribu`. |
| `../../processing/finite_differences.py` | Fifth-order finite-difference weights in time. |
| `../../processing/hyperblock.py` | Position of the hyperblock and of each rank's piece of it. |
| `../../processing/per_node_hdf5.py` | Per-node HDF5 output of the checkpoints. |
| `../../benchmark/iops_isav.yaml` | IOPS benchmark campaign. |

## Configuration

In the GYSELA YAML file:

```yaml
Output:
  nbiter_diag: 2   # in-situ diagnostics every N timesteps
  nbiter_pca: 10   # mergeable PCA every N timesteps, 0 disables it
```

## In-situ analytics

With `params/pdi_out_diags.yaml`, the analytics (`processing/diagnostics.py`) write into their working directory:

| Output | When | Content |
| --- | --- | --- |
| `fluid_moments/fluid_moments_iter<iter>.h5` | every `nbiter_diag` | Fluid moments `n`, `ux`, `uy`, `T`, each of shape `(species, x, y)`, with the `x`/`y` mesh coordinates. |
| `hyperblock_dfdt/dfdistribu_dt_iter<iter>.h5` | every `nbiter_diag`, except the first 3 and last 2 diagnostic steps | Time derivative of the hyperblock, `dfdistribu_dt[x, y, vx, vy]`, with its mesh coordinates: one shared file per step. |
| `checkpoint/fdistribu_iter<iter>_<host>.h5` | whenever the previous checkpoint has been written | The whole `fdistribu`, one file per Dask node, one group per MPI rank's chunk with its mesh coordinates and global start indices. |

All are written with xarray and read back with `xr.open_dataset(path)` (`group=...` for the per-node checkpoint files).

### Fluid moments

For each species, with velocities and temperature in the units of the velocity mesh:

- `n = ∫ f dv`
- `u = (ux, uy) = ∫ v f dv / n`
- `T = ∫ |v − u|² f dv / (2 n)`

### Hyperblock

The hyperblock is a 4D `(x, y, vx, vy)` block of `fdistribu`, around the middle of the domain. Its size is a placeholder set in `processing/hyperblock.py`: about `HYPERBLOCK_FRACTION` (10%) of each dimension. It is aligned to the MPI decomposition so that deisa can assemble it: along a dimension where the local chunks are larger than the hyperblock, it lies inside one chunk; otherwise its extent is rounded to whole local chunks. In the `V2DSplit` layout of `fdistribu`, x and y are local to every rank, so only the vx and vy extents can be rounded.

At every diagnostic step, the ranks holding a part of the hyperblock send it (`fdistribu_hyperblock`), and deisa assembles it. The `write_hyperblock_derivative` callback keeps the last 6 hyperblocks in a deisa window (`Window("fdistribu_hyperblock", 6)`). Once the window is full, it computes the time derivative at its 4th step, i.e. 2 diagnostic steps back, as a dask array, and writes it with `xr.Dataset(...).to_netcdf(...)`.

The time derivative is a fifth-order finite difference over the 6 steps from 3 before to 2 after the step (`processing/finite_differences.py`). It is only computed where these steps exist, so the first 3 and the last 2 diagnostic steps of the run have no derivative file. The weights follow the actual step times, so an uneven last step is handled. The file attributes `derivative_iters` and `derivative_order` record the stencil.

### Checkpoint

The whole `fdistribu` is checkpointed asynchronously, as often as the writes allow. At every diagnostic step the pycall checks the PDI variable `checkpoint_write_pending`:

- `1`: the last checkpoint is still being written. The pycall asks deisa for the write acknowledgement (`checkpoint_write_done`) and clears the flag once it has arrived.
- `0`: the pycall sends `checkpoint_trigger` along with that step's `fdistribu`, then sets the flag to `1`. The `write_checkpoint` callback only fires on that trigger: it writes that step's `fdistribu` and acknowledges with `deisa.set`.

Checkpoints therefore run back to back without blocking the simulation, at diagnostic steps, reusing the `fdistribu` already shared for the fluid moments. The first one is the initial state.

### Mergeable PCA

`compute_mergeable_pca` in `processing/diagnostics.py` receives `fdistribu` every `nbiter_pca` iterations. It is a placeholder for the mergeable PCA of the upcoming deisa-dask release.

## Environment

Follow the deisa/Dask installation instructions in the toolchain environment scripts ([`toolchains/persee/xeon/environment.sh`](../../toolchains/persee/xeon/environment.sh) on Persee, [`toolchains/adastra/genoa/environment.sh`](../../toolchains/adastra/genoa/environment.sh) on Adastra). The toolchain environment is sourced automatically by `deisa-dask_launch_script.sh`.

## Running

Without analytics:
```bash
mpirun -n 4 ./build/simulations/deisa_bench/gys_deisa_bench \
  simulations/deisa_bench/params/<PARAMS> simulations/deisa_bench/params/pdi_out.yaml
```

With the in-situ analytics:
```bash
./simulations/deisa_bench/deisa-dask_launch_script.sh [SIMU_NODES] [DASK_WORKERS] [GYSELA_PARAMS]
```

The launch script defaults: `SIMU_NODES=1`, `DASK_WORKERS=1`, `GYSELA_PARAMS=params/params_landau_damping.yaml`, `PDI_CONFIG=params/pdi_out_diags.yaml`.

Benchmark campaigns are described in `benchmark/iops_isav.yaml` (run with `iops run`).

## Restart

`gys_deisa_bench` starts from the analytic initial condition when `Input.nb_restart` is 0. Otherwise it reads `fdistribu` from `Input.fdistribu_filename` (HDF5 dataset `fdistribu[species, x, y, vx, vy]`) and numbers its iterations from `Input.iter_offset`.
