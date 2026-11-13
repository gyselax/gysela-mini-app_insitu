# GYSELA In-situ Analytics Mini App

2D2V Vlasov–Poisson mini-application (`gys_deisa_bench`) coupled through deisa-dask to in-situ analytics running on a Dask cluster. Default case: Landau damping.

## Files

| File | Role |
| --- | --- |
| `../../src/deisa_bench/gys_deisa_bench.cpp` | C++ mini-app: cold start or restart, exposes `fdistribu` to PDI at every iteration. |
| `params/params_landau_damping.yaml` | Landau-damping input. |
| `params/params_two_stream.yaml` | Two-stream instability input. |
| `params/pdi_out.yaml` | PDI config without analytics. |
| `params/pdi_out_diags.yaml` | PDI config sharing `fdistribu` with the deisa analytics through the pycall plugin. |
| `deisa-dask_launch_script.sh` | Starts a Dask scheduler, workers, the analytics and the simulation. |
| `../../processing/diagnostics.py` | Analytics (deisa callbacks), run next to the simulation. |
| `../../benchmark/iops_isav.yaml` | IOPS benchmark campaign. |

## Configuration

In the GYSELA YAML file:

```yaml
Output:
  nbiter_diag: 2  # diagnostics every N timesteps
```

## In-situ analytics

With `params/pdi_out_diags.yaml`, `fdistribu` is shared with the analytics every `nbiter_diag` iterations. `processing/diagnostics.py` computes the conserved quantities (kinetic, electric and total energy, L2 norm, mass, momentum) and appends them to `diagnostics.csv` in its working directory (`species_<i>/diagnostics.csv` with several species).

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
