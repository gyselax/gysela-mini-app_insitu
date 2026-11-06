#!/bin/bash
#SBATCH --job-name=gys_deisa_ray
#SBATCH --output=slurm-%j.log
#SBATCH --nodes=2
#SBATCH --time=00:07:00
#SBATCH --constraint="MI250"
#SBATCH --account=gen2224
#SBATCH --exclusive
#
# =============================================================================
# gysela-mini-app (compression) + deisa-ray in-situ analytics
# Adastra / MI250 partition
# =============================================================================
#
# Standalone version of the `adastra-mi250` script from iops_ray.yaml, with the
# Jinja variables resolved (diag_mode=1, deisa_version=ray, ranks_per_node=8).
#
# ---------------------------------------------------------------------------
# Resource layout, per MI250 node (64 CPU cores, 8 GCDs)
# ---------------------------------------------------------------------------
#   * 8 CPUs (0,8,16,24,32,40,48,56) + the 8 GCDs -> gysela-mini-app
#        1 MPI rank per GCD, each rank pinned to one CPU + one GCD
#   * the remaining 56 CPUs -> Ray + analytics, split as
#         RAY_CPUS + ANALYTICS_CPUS = 56
#     with RAY_CPUS=48 and ANALYTICS_CPUS=8 by default. The two sets are
#     disjoint: Ray (head + workers) is pinned to its 48 CPUs, analytics to the
#     8 CPUs reserved for it. Ray is told its budget through
#     `ray start --num-cpus=${RAY_CPUS}`, so Ray tasks never use more than
#     RAY_CPUS. Change the split with, e.g., `RAY_CPUS=40 sbatch launch-...sh`.
#
# The GPU <-> CPU affinity mirrors the `mi250_gpu_binding.sh` input of the
# original benchmark; it is written out below so the job is self-contained.
#
# ---------------------------------------------------------------------------
# Ray head node == worker node
# ---------------------------------------------------------------------------
# Node 0 hosts the Ray head. In Ray the head node is *also* a normal worker
# node: `ray start --head` starts a raylet there and Ray will schedule tasks
# and actors on it like on any other node (the head only additionally runs the
# GCS and the dashboard). We therefore do NOT launch a second
# `ray start --address ...` worker on node 0 -- only nodes 1..N-1 join as
# workers. Starting one on the head would create a duplicate raylet/node.
#
# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------
# Everything is written under <submit dir>/job_<job id>_deisa-ray/:
#   job.log         script trace (set -x) + general messages
#   ray-head.log    Ray head   stdout + stderr
#   ray-worker.log  Ray workers stdout + stderr
#   analytics.log   deisa-ray analytics stdout + stderr
#   sim.log         gysela-mini-app stdout + stderr (the timing metrics are here)
#   slurm.log       Slurm-managed bootstrap log (startup + slurmstepd messages)
#
# The working directory is set to the job folder, so the run outputs land there
# too:
#   diagnostics.csv     deisa-ray analytics output
#   GYSELALIBXX_*.h5    gysela-mini-app snapshots
# =============================================================================

set -e
set -o pipefail
set -x

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
# Repo root, resolved from this script's own location (simulations/compression/ -> ../..).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR=${BASE_DIR:-$(cd "${SCRIPT_DIR}/../.." && pwd)}
TOOLCHAIN_ENV=${TOOLCHAIN_ENV:-/lus/scratch/CT5/gen2224/adevora/gysela-mini-app_insitu/toolchains/adastra/mi250/environment.sh}
VENV_ACTIVATE=${VENV_ACTIVATE:-${BASE_DIR}/.gys_env/bin/activate}

GYS_COMPRESS_BIN=${GYS_COMPRESS_BIN:-${BASE_DIR}/build/simulations/compression/gys_compress}
ANALYTICS_SCRIPT=${ANALYTICS_SCRIPT:-${BASE_DIR}/processing/diagnostics_ray.py}
PDI_CONFIG=${PDI_CONFIG:-${BASE_DIR}/simulations/compression/params/pdi_out_diags_ray.yaml}
GYS_CONF=${GYS_CONF:-${BASE_DIR}/simulations/compression/params/params_landau_damping.yaml}

# Base directory for this run (defaults to the submit directory). The job
# folder below is created inside it and becomes the working directory, so the
# relative outputs (diagnostics.csv, GYSELALIBXX_*.h5) land in the job folder.
EXECUTION_DIR=${EXECUTION_DIR:-${SLURM_SUBMIT_DIR:-${PWD}}}
GPU_BINDING=${EXECUTION_DIR}/mi250_gpu_binding.sh

# All logs for this run live in <EXECUTION_DIR>/job_<job id>_deisa-ray/.
# When submitting from the repo, that is
#   /lus/scratch/.../gysela-mini-app_insitu/job_<job id>_deisa-ray/
LOG_DIR=${LOG_DIR:-${EXECUTION_DIR}/job_${SLURM_JOB_ID}_deisa-ray}
mkdir -p "${LOG_DIR}"

# Slurm creates its own output file (from #SBATCH --output=slurm-%j.log) in the
# submit directory *before* the script runs. Move it into the job folder right
# away: Slurm keeps writing through the same file descriptor, so the log stays
# complete in the job folder and no slurm-<id>.log is left in the submit dir.
SLURM_BOOT_LOG="${SLURM_SUBMIT_DIR:-${EXECUTION_DIR}}/slurm-${SLURM_JOB_ID}.log"
mv -f "${SLURM_BOOT_LOG}" "${LOG_DIR}/slurm.log" 2>/dev/null || true

# From here on the script's own stdout/stderr (including the `set -x` trace)
# goes into the job folder.
exec >>"${LOG_DIR}/job.log" 2>&1

# -----------------------------------------------------------------------------
# Hardware budget
# -----------------------------------------------------------------------------
RANKS_PER_NODE=8                        # one MPI rank (and one GCD) per rank
SIM_ANCHOR_CPUS=(0 8 16 24 32 40 48 56) # 1 gysela CPU per GCD
RAY_ANALYTICS_CPUS=56                   # CPUs left to Ray + analytics on each node
RAY_CPUS=${RAY_CPUS:-48}                # CPUs Ray is told it owns
ANALYTICS_CPUS=$((RAY_ANALYTICS_CPUS - RAY_CPUS))
RAY_PORT=6379

if ((RAY_CPUS < 0 || RAY_CPUS > RAY_ANALYTICS_CPUS)); then
  echo "ERROR: RAY_CPUS must be in [0, ${RAY_ANALYTICS_CPUS}] (got ${RAY_CPUS})" >&2
  exit 1
fi

# The 56 CPUs not used by the gysela ranks, i.e. every CPU id that is not one
# of the anchors (multiples of 8). Split into two disjoint sets: Ray takes the
# first RAY_CPUS, analytics the remaining ANALYTICS_CPUS.
ALL_CPUS=()
for ((c = 0; c < 64; c++)); do
  ((c % 8 == 0)) || ALL_CPUS+=("$c")
done

# Compress a sorted CPU id list into a numactl --physcpubind range string.
compress_ranges() {
  local out="" start="" prev="" c
  for c in "$@"; do
    if [[ -z "${start}" ]]; then
      start=$c
      prev=$c
    elif ((c == prev + 1)); then
      prev=$c
    else
      if ((start == prev)); then out+="${out:+,}${start}"; else out+="${out:+,}${start}-${prev}"; fi
      start=$c
      prev=$c
    fi
  done
  if [[ -n "${start}" ]]; then
    if ((start == prev)); then out+="${out:+,}${start}"; else out+="${out:+,}${start}-${prev}"; fi
  fi
  printf '%s' "${out}"
}

RAY_CPU_SET=$(compress_ranges "${ALL_CPUS[@]:0:RAY_CPUS}")
ANALYTICS_CPU_SET=$(compress_ranges "${ALL_CPUS[@]:RAY_CPUS}")

# -----------------------------------------------------------------------------
# Environment
# -----------------------------------------------------------------------------
cd "${LOG_DIR}"

# shellcheck source=/dev/null
source "${TOOLCHAIN_ENV}"
# shellcheck source=/dev/null
source "${VENV_ACTIVATE}"

unset LUA_PATH LUA_CPATH

export GYS_DIAG_MODE=1
export OMP_NUM_THREADS=1
export MPICH_GPU_SUPPORT_ENABLED=1

# Ray (imported by deisa.ray) raises
#   "Please use HIP_VISIBLE_DEVICES instead of ROCR_VISIBLE_DEVICES"
# when ROCR_VISIBLE_DEVICES is set but HIP_VISIBLE_DEVICES is not -- and Slurm
# sets ROCR_VISIBLE_DEVICES for the job. The Ray/analytics side is CPU-only, so
# hide the GPUs from it. The simulation selects its GCD per rank via
# HIP_VISIBLE_DEVICES in the binding script below.
unset ROCR_VISIBLE_DEVICES
export HIP_VISIBLE_DEVICES=""

# -----------------------------------------------------------------------------
# Per-rank GPU / CPU binding script (numactl anchor CPU + HIP device)
# -----------------------------------------------------------------------------
cat >"${GPU_BINDING}" <<'EOF'
#!/bin/bash
set -eu
AFFINITY_NUMACTL=('48' '56' '16' '24' '0' '8' '32' '40')
AFFINITY_GPU=('0' '1' '2' '3' '4' '5' '6' '7')
CPU_SET="${AFFINITY_NUMACTL[$SLURM_LOCALID]}"
unset ROCR_VISIBLE_DEVICES
export HIP_VISIBLE_DEVICES="${AFFINITY_GPU[$SLURM_LOCALID]}"
exec numactl --localalloc --physcpubind="${CPU_SET}" -- "${@}"
EOF
chmod 0755 "${GPU_BINDING}"

# -----------------------------------------------------------------------------
# Node layout
# -----------------------------------------------------------------------------
mapfile -t NODES < <(scontrol show hostnames "${SLURM_JOB_NODELIST}")
NNODES=${#NODES[@]}
HEAD_NODE="${NODES[0]}"
WORKER_NODES=("${NODES[@]:1}")
WORKER_NODELIST=""
if ((NNODES > 1)); then
  WORKER_NODELIST=$(
    IFS=,
    echo "${WORKER_NODES[*]}"
  )
fi

echo "Allocated nodes : ${NODES[*]}"
echo "Ray head node   : ${HEAD_NODE}"
echo "Ray worker nodes: ${WORKER_NODELIST:-<none>}"
echo "CPUs  gysela       : ${SIM_ANCHOR_CPUS[*]} (1 per GCD)"
echo "CPUs  Ray (${RAY_CPUS})     : ${RAY_CPU_SET}"
echo "CPUs  analytics (${ANALYTICS_CPUS}): ${ANALYTICS_CPU_SET}"

# -----------------------------------------------------------------------------
# Cleanup: stop Ray on every node when the job ends
# -----------------------------------------------------------------------------
cleanup() {
  echo "Stopping Ray..."
  ray stop --force >/dev/null 2>&1 || true
  if ((NNODES > 1)); then
    srun -N $((NNODES - 1)) -n $((NNODES - 1)) --ntasks-per-node=1 \
      -w "${WORKER_NODELIST}" --overlap --cpu-bind=none \
      bash -c 'ray stop --force' >/dev/null 2>&1 || true
  fi
  # Safety net: if the bootstrap log was recreated in the submit dir, move it.
  if [[ -f "${SLURM_BOOT_LOG}" ]]; then
    mv -f "${SLURM_BOOT_LOG}" "${LOG_DIR}/slurm.log" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# -----------------------------------------------------------------------------
# Ray head on node 0 (also acts as a worker node)
# -----------------------------------------------------------------------------
HEAD_IP=$(
  srun -N 1 -n 1 -w "${HEAD_NODE}" --overlap --cpu-bind=none \
    bash -c "ip -4 -o addr show hsn0 | awk '{print \$4}' | cut -d/ -f1"
)

echo "Launching Ray head on ${HEAD_NODE} (${HEAD_IP}:${RAY_PORT})"
srun -N 1 -n 1 -w "${HEAD_NODE}" --overlap --cpu-bind=none --export=ALL \
  bash -c "
        set -eu
        unset ROCR_VISIBLE_DEVICES
        export HIP_VISIBLE_DEVICES=''
        echo '[Ray HEAD] hostname:' \$(hostname)
        echo '[Ray HEAD] CPU set : ${RAY_CPU_SET}'
        numactl --physcpubind='${RAY_CPU_SET}' \
            ray start \
                --head \
                --node-ip-address='${HEAD_IP}' \
                --port=${RAY_PORT} \
                --num-cpus=${RAY_CPUS} \
                --disable-usage-stats \
                --block
    " >"${LOG_DIR}/ray-head.log" 2>&1 &

# Wait until the head GCS port accepts connections.
echo -n "Waiting for Ray head "
for _ in $(seq 1 60); do
  if (exec 3<>/dev/tcp/"${HEAD_IP}"/"${RAY_PORT}") 2>/dev/null; then
    exec 3<&- 3>&-
    break
  fi
  echo -n "."
  sleep 2
done
echo " ready"

# deisa-ray 0.1.7 locates the head node through the Ray dashboard state API
# (http://127.0.0.1:8265), so make sure the dashboard is actually reachable.
echo -n "Waiting for Ray dashboard "
dashboard_up=0
for _ in $(seq 1 30); do
  if (exec 3<>/dev/tcp/"${HEAD_IP}"/8265) 2>/dev/null; then
    exec 3<&- 3>&-
    dashboard_up=1
    break
  fi
  echo -n "."
  sleep 2
done
if ((dashboard_up)); then
  echo " up (${HEAD_IP}:8265)"
else
  echo " NOT reachable on ${HEAD_IP}:8265"
  echo "WARNING: deisa-ray needs the Ray dashboard; check ray-head.log and the ray[default] deps." >&2
fi

# -----------------------------------------------------------------------------
# Ray workers on all other nodes (the head node is already a worker)
# -----------------------------------------------------------------------------
if ((NNODES > 1)); then
  echo "Launching Ray workers on: ${WORKER_NODELIST}"
  srun -N $((NNODES - 1)) -n $((NNODES - 1)) --ntasks-per-node=1 \
    -w "${WORKER_NODELIST}" --overlap --cpu-bind=none --export=ALL \
    bash -c "
            set -eu
            unset ROCR_VISIBLE_DEVICES
            export HIP_VISIBLE_DEVICES=''
            node_ip=\$(ip -o -4 addr show hsn0 | awk '{print \$4}' | cut -d/ -f1)
            echo '[Ray WORKER] hostname: '\$(hostname)' ip:' \$node_ip
            numactl --physcpubind='${RAY_CPU_SET}' \
                ray start \
                    --address='${HEAD_IP}:${RAY_PORT}' \
                    --node-ip-address=\$node_ip \
                    --num-cpus=${RAY_CPUS} \
                    --disable-usage-stats \
                    --block
        " >"${LOG_DIR}/ray-worker.log" 2>&1 &

  # Wait (best effort) until all worker nodes have joined the cluster.
  echo -n "Waiting for ${NNODES} Ray nodes "
  for _ in $(seq 1 60); do
    joined=$(ray status --address="${HEAD_IP}:${RAY_PORT}" 2>/dev/null |
      grep -cE '^[[:space:]]*[0-9]+ node_' || true)
    if ((joined >= NNODES)); then
      break
    fi
    echo -n "."
    sleep 2
  done
  echo " (${joined:-?}/${NNODES})"
fi

# -----------------------------------------------------------------------------
# Analytics (on the head node; pinned to its reserved CPUs)
# -----------------------------------------------------------------------------
echo "Launching analytics: ${ANALYTICS_SCRIPT}"
DEISA_DISTRIBUTED_SCHEDULING=1 \
  numactl --physcpubind="${ANALYTICS_CPU_SET}" python3 "${ANALYTICS_SCRIPT}" \
  >"${LOG_DIR}/analytics.log" 2>&1 &
ANALYTICS_PID=$!
# -----------------------------------------------------------------------------
# Simulation on every node: 8 ranks/node, 1 GCD + 1 CPU per rank
# -----------------------------------------------------------------------------
SIMU_NODES=${NNODES}
SIMU_RANKS=$((SIMU_NODES * RANKS_PER_NODE))

echo "Launching simu on ${SIMU_NODES} nodes (${RANKS_PER_NODE} ranks/node, ${SIMU_RANKS} ranks total, 1 GCD/rank)"
srun -N "${SIMU_NODES}" -n "${SIMU_RANKS}" --ntasks-per-node="${RANKS_PER_NODE}" \
  --cpu-bind=none --mem-bind=none --mem=0 --overlap -r 0 \
  "${GPU_BINDING}" \
  "${GYS_COMPRESS_BIN}" "${GYS_CONF}" "${PDI_CONFIG}" \
  >"${LOG_DIR}/sim.log" 2>&1 &
SIM_PID=$!

# -----------------------------------------------------------------------------
# Wait
# -----------------------------------------------------------------------------
ANALYTICS_RC=0
SIM_RC=0
if ((GYS_DIAG_MODE == 1)); then
  wait "${ANALYTICS_PID}" || ANALYTICS_RC=$?
  echo "Analytics over (rc=${ANALYTICS_RC})"
fi
wait "${SIM_PID}" || SIM_RC=$?
echo "Simulation over (rc=${SIM_RC})"

if ((ANALYTICS_RC != 0 || SIM_RC != 0)); then
  echo "ERROR: analytics rc=${ANALYTICS_RC}, simulation rc=${SIM_RC}" >&2
  exit 1
fi
