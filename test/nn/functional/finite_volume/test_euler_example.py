# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import torch
from omegaconf import OmegaConf

from physicsnemo.nn.functional import mesh_green_gauss_gradient

EXAMPLE_DIR = (
    Path(__file__).resolve().parents[4]
    / "examples"
    / "functionals"
    / "finite_volume_euler"
)
sys.path.insert(0, str(EXAMPLE_DIR))

from euler_finite_volume import (  # noqa: E402
    euler_cfl_timestep,
    euler_conservative_to_primitive,
    euler_update,
    primitive_to_conservative_torch,
)
from euler_solver import build_case, initial_state  # noqa: E402


def _solver_cfg():
    return OmegaConf.create(
        {
            "device": "cpu",
            "gamma": 1.4,
            "density_floor": 1.0e-6,
            "pressure_floor": 1.0e-6,
            "inflow_density": 1.4,
            "inflow_pressure": 1.0,
            "inflow_mach": 3.0,
            "cfl": 0.25,
        }
    )


@pytest.mark.parametrize(
    "case_cfg",
    [
        OmegaConf.create(
            {
                "name": "tiny2d",
                "dimension": 2,
                "length": 3.0,
                "height": 1.0,
                "step_x": 0.6,
                "step_height": 0.2,
                "nx": 10,
                "ny": 5,
            }
        ),
        OmegaConf.create(
            {
                "name": "tiny3d",
                "dimension": 3,
                "length": 3.0,
                "height": 1.0,
                "depth": 0.4,
                "step_x": 0.6,
                "step_height": 0.2,
                "nx": 5,
                "ny": 5,
                "nz": 2,
            }
        ),
    ],
)
def test_euler_example_warp_matches_torch_one_step(case_cfg):
    pytest.importorskip("warp")
    solver_cfg = _solver_cfg()
    mesh, cell_neighbors, boundary_tags = build_case(case_cfg, torch.device("cpu"))
    gamma = float(solver_cfg.gamma)
    density_floor = float(solver_cfg.density_floor)
    pressure_floor = float(solver_cfg.pressure_floor)
    U = primitive_to_conservative_torch(initial_state(solver_cfg, mesh), gamma)

    outputs = []
    dts = []
    for implementation in ("torch", "warp"):
        W = euler_conservative_to_primitive(
            U, gamma, density_floor, pressure_floor, implementation
        )
        dt = euler_cfl_timestep(
            W,
            mesh.points,
            mesh.cells,
            cell_neighbors,
            boundary_tags,
            W[0],
            gamma,
            float(solver_cfg.cfl),
            density_floor,
            pressure_floor,
            implementation,
        )
        grad_W = mesh_green_gauss_gradient(
            mesh.points,
            mesh.cells,
            cell_neighbors,
            W,
            implementation=implementation,
        )
        outputs.append(
            euler_update(
                U,
                W,
                grad_W,
                mesh.points,
                mesh.cells,
                cell_neighbors,
                boundary_tags,
                W[0],
                dt,
                gamma,
                density_floor,
                pressure_floor,
                implementation,
            )
        )
        dts.append(dt)

    torch.testing.assert_close(dts[1], dts[0], rtol=0.0, atol=1.0e-7)
    torch.testing.assert_close(outputs[1], outputs[0], rtol=1.0e-5, atol=1.0e-5)
