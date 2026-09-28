#!/bin/bash
# Launch the compression mini-app together with the deisa analytics (diagnostics.py).
#
# Usage:
#   ./deisa-dask_launch_script.sh [SIMU_NODES] [DASK_WORKERS] [GYSELA_PARAMS] [PDI_CONFIG] [ANALYTICS_FILE]
#
# Defaults:
#   SIMU_NODES     = 1
#   DASK_WORKERS   = 1
#   GYSELA_PARAMS  = params/params_landau_damping.yaml
#   PDI_CONFIG     = params/pdi_out_diags.yaml
#   ANALYTICS_FILE = <repo_root>/processing/diagnostics.py

SIMU_NODES=${1:-1}
DASK_WORKERS=${2:-1}
GYSELA_PARAMS=${3:-params/params_landau_damping.yaml}
PDI_CONFIG=${4:-params/pdi_out_diags.yaml}
ANALYTICS_ARG=${5:-}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd $SCRIPT_DIR/../.. && pwd)"

# Resolve the toolchain environment the same way launch_benchmark.py does
# (replaces the deleted apps/io/activate_deisa_spack_env.sh).
if [ -n "$(command -v scontrol)" ] && scontrol show config 2>/dev/null | grep -qi adastra; then
    SITE="adastra"
    ARCH="genoa"
else
    SITE="persee"
    ARCH="xeon"
fi

TOOLCHAIN_ENV="${BASE_DIR}/toolchains/${SITE}/${ARCH}/environment.sh"
if [ -f "${TOOLCHAIN_ENV}" ]; then
    . "${TOOLCHAIN_ENV}"
fi

# Sourcing the toolchain environment may have reset our markers — restore them.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd $SCRIPT_DIR/../.. && pwd)"

# The spack view that holds the PDI + deisa Python runtime.
export PYTHONPATH=/data/gyselarunner/gysela-io-env-deisa/.spack-env/view/lib/python3.13/site-packages:${PYTHONPATH:-}

# Make the `processing` python package (repo root) importable.
export PYTHONPATH="${BASE_DIR}:${PYTHONPATH}"

if [ -f "${BASE_DIR}/.gys_env/bin/activate" ]; then
    . "${BASE_DIR}/.gys_env/bin/activate"
fi

ANALYTICS_FILE="${ANALYTICS_ARG:-$BASE_DIR/processing/diagnostics.py}"

SCHEFILE="$BASE_DIR/scheduler.json"
rm -f $SCHEFILE

cd $SCRIPT_DIR

echo "Launch scheduler"
dask-scheduler --scheduler-file=$SCHEFILE &
dask_sch_pid=$!

while ! [ -f $SCHEFILE ]; do
	sleep 1
	echo -n .
done

export DEISA_DASK_SCHEDULER_ADDRESS=$(jq -r '.["address"]' $SCHEFILE)

echo "Launch workers"
dask-worker \
	--nworkers ${DASK_WORKERS} \
	--local-directory /tmp \
	--scheduler-file=${SCHEFILE} &
dask_worker_pid=$!

sleep 10

echo "Launch analytics"
python3 $ANALYTICS_FILE &
analytics_pid=$!

echo "Launch simulation"
mpirun -n $SIMU_NODES $BASE_DIR/build/apps/compression/gys_compress \
	$SCRIPT_DIR/$GYSELA_PARAMS \
	$SCRIPT_DIR/$PDI_CONFIG &
simu_pid=$!

wait ${analytics_pid}
echo "Analytics over"
wait ${simu_pid}
echo "Simulation over"

kill -9 ${dask_worker_pid} ${dask_sch_pid}
