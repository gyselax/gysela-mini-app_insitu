"""gys_compress smoke test"""

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GYS_COMPRESS = REPO_ROOT / "build" / "simulations" / "compression" / "gys_compress"
COMPRESSION_DIR = REPO_ROOT / "simulations" / "compression"
COMPRESSION_PARAMS = COMPRESSION_DIR / "params"
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


def test_gys_compress_runs():
    assert GYS_COMPRESS.is_file(), f"Build gys_compress first (missing {GYS_COMPRESS})"
    assert (COMPRESSION_PARAMS / "params_two_stream.yaml").is_file()
    assert (COMPRESSION_PARAMS / "pdi_out.yaml").is_file()

    cmd, env = mpirun_env()
    cmd.extend(
        [
            str(GYS_COMPRESS),
            str(COMPRESSION_PARAMS / "params_two_stream.yaml"),
            str(COMPRESSION_PARAMS / "pdi_out.yaml"),
        ]
    )
    result = subprocess.run(cmd, cwd=COMPRESSION_DIR, env=env, check=False)
    assert result.returncode == 0
