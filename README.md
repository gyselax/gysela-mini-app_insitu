# Gysela Mini Apps

A minimal application demonstrating GYSELA I/O operations and testing the CPU performance scaling for 5D particle distribution functions.

This repository contains mini apps that allow easy coupling with GyselaX++.

## Installing

```bash
git clone git@github.com:gyselax/gysela-mini-app_io.git
cd gysela-mini-app_io
git submodule update --init --recursive
```

## Quick install

After cloning and initializing submodules:

```bash
sh ./installer.sh <MACHINE>
```

Example on Persee (CPU): `./installer.sh persee/xeon`


`<MACHINE>` is a folder under `toolchains/`.
Available values:

- `a100.leonardo.spack` — Leonardo (A100)
- `a100.raven.spack` — Raven (A100)
- `docker.gyselalibxx_env` — Docker / CI
- `genoa.gcc.adastra.spack` — Adastra (Genoa CPU)
- `h100.jean-zay.spack` — Jean Zay (H100)
- `mi250.hipcc.adastra.spack` — Adastra (MI250)
- `persee/v100` — Persee (V100)
- `persee/xeon` — Persee (CPU); default on Persee hosts if `<MACHINE>` is omitted
- `v100.ruche` — Ruche (V100)

## Manual installation

```bash
source toolchains/<MACHINE>/environment.sh
python -m venv .gys_env    # skip if .gys_env already exists
source .gys_env/bin/activate
pip install -e ".[dev]"
```

For more details see [Gyselalib++ environment toolchains](https://gyselax.github.io/gyselalibxx/toolchains/index.html#environment-setup).

## Building

By default, the deisa_bench app is built:

```bash
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=toolchains/<MACHINE>/toolchain.cmake
cmake --build build -j 4
```

For the docker toolchain, you should use the following in the docker container:
```bash
cmake -S . -B build
cmake --build build -j 4
```

## Python venv

In order to use the python tools, you'll need to execute the following commands from the repo's root:
```bash
source .gys_env/bin/activate
```

The in-situ analytics in `processing/` are run next to the simulation through deisa; see the app README.

## Running

Each app has its own usage instructions. See the README file in the corresponding `simulations/` folder for details.

The mini-app (`simulations/deisa_bench/`, executable `gys_deisa_bench`) takes a GYSELA YAML config and a PDI config on the command line: `params/pdi_out.yaml` without analytics, `params/pdi_out_diags.yaml` with the deisa in-situ analytics. Case-specific templates are `params/params_landau_damping.yaml` and `params/params_two_stream.yaml`.

