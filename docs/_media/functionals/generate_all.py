#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


SCRIPTS = (
    "geometry/mesh_poisson_disk_sample/generate.py",
    "geometry/mesh_to_voxel_fraction/generate.py",
    "geometry/sdf/generate.py",
    "interpolation/grid_to_point_interpolation/generate.py",
    "interpolation/point_to_grid_interpolation/generate.py",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate all functional visualization assets"
    )
    parser.add_argument(
        "--device",
        default=None,
        choices=("cpu", "cuda"),
        help="Optional device override passed to each generator",
    )
    args = parser.parse_args()

    root = _repo_root()
    scripts_root = root / "docs/_media/functionals"

    for relative_script in SCRIPTS:
        script_path = scripts_root / relative_script
        cmd = [sys.executable, str(script_path)]
        if args.device is not None:
            cmd.extend(["--device", args.device])
        print(f"[visuals] running: {' '.join(cmd)}", flush=True)
        env = dict(os.environ)
        existing_pythonpath = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = (
            f"{root}:{existing_pythonpath}" if existing_pythonpath else str(root)
        )
        subprocess.run(cmd, cwd=root, env=env, check=True)


if __name__ == "__main__":
    main()
