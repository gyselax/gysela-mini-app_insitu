"""gys_deisa_bench smoke test"""

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GYS_DEISA_BENCH = REPO_ROOT / "build" / "simulations" / "deisa_bench" / "gys_deisa_bench"
DEISA_BENCH_DIR = REPO_ROOT / "simulations" / "deisa_bench"
DEISA_BENCH_PARAMS = DEISA_BENCH_DIR / "params"
REPO_PYTHON = REPO_ROOT


def mpirun_env():
    env = os.environ.copy()
    if env.get("PYTHONPATH"):
        env["PYTHONPATH"] = f"{REPO_PYTHON}{os.pathsep}{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = str(REPO_PYTHON)

    cmd = ["mpirun", "-n", "1"]
    for var in ("PYTHONPATH", "PATH", "LD_LIBRARY_PATH"):
        if var in env:
            cmd.extend(["-x", var])
    return cmd, env


def test_gys_deisa_bench_runs():
    assert GYS_DEISA_BENCH.is_file(), f"Build gys_deisa_bench first (missing {GYS_DEISA_BENCH})"
    assert (DEISA_BENCH_PARAMS / "params_two_stream.yaml").is_file()
    assert (DEISA_BENCH_PARAMS / "pdi_out.yaml").is_file()

    cmd, env = mpirun_env()
    cmd.extend(
        [
            str(GYS_DEISA_BENCH),
            str(DEISA_BENCH_PARAMS / "params_two_stream.yaml"),
            str(DEISA_BENCH_PARAMS / "pdi_out.yaml"),
        ]
    )
    result = subprocess.run(cmd, cwd=DEISA_BENCH_DIR, env=env, check=False)
    assert result.returncode == 0
