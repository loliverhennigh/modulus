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

import pytest
import torch

from physicsnemo.mesh.calculus.divergence import compute_divergence_points_lsq
from physicsnemo.mesh.calculus.gradient import compute_gradient_points_lsq
from physicsnemo.mesh.calculus.laplacian import (
    compute_laplacian_points_dec,
    compute_laplacian_points_lsq,
)
from physicsnemo.mesh.mesh import Mesh
from test.conftest import requires_module


def _tet_mesh(device: str):
    torch_device = torch.device(device)
    points = torch.tensor(
        [
            [0.0, 0.0, 0.0],
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
            [0.5, 0.5, 0.5],
        ],
        dtype=torch.float32,
        device=torch_device,
    )
    cells = torch.tensor(
        [
            [0, 1, 2, 4],
            [0, 1, 3, 4],
            [0, 2, 3, 4],
            [1, 2, 3, 4],
        ],
        dtype=torch.int64,
        device=torch_device,
    )
    return Mesh(points=points, cells=cells)


@requires_module("warp")
def test_mesh_gradient_exposes_warp_implementation(device: str):
    mesh = _tet_mesh(device)
    values = mesh.points[:, 0] + 2.0 * mesh.points[:, 1] - mesh.points[:, 2]

    output = mesh.gradient(
        values,
        gradient_type="extrinsic",
        implementation="warp",
    )
    expected = compute_gradient_points_lsq(
        mesh,
        values,
        intrinsic=False,
        implementation="warp",
    )

    torch.testing.assert_close(output, expected)


@requires_module("warp")
def test_mesh_compute_point_derivatives_exposes_warp_implementation(device: str):
    mesh = _tet_mesh(device)
    mesh.point_data["f"] = mesh.points[:, 0] + mesh.points[:, 1]

    output = mesh.compute_point_derivatives(
        keys="f",
        gradient_type="extrinsic",
        implementation="warp",
    )
    expected = compute_gradient_points_lsq(
        mesh,
        mesh.point_data["f"],
        intrinsic=False,
        implementation="warp",
    )

    torch.testing.assert_close(output.point_data["f_gradient"], expected)


def test_intrinsic_point_gradient_rejects_warp_implementation(device: str):
    points = torch.tensor(
        [
            [0.0, 0.0, 0.0],
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [1.0, 1.0, 0.0],
        ],
        dtype=torch.float32,
        device=torch.device(device),
    )
    cells = torch.tensor(
        [[0, 1, 2], [1, 3, 2]], dtype=torch.int64, device=points.device
    )
    mesh = Mesh(points=points, cells=cells)
    values = points[:, 0]

    with pytest.raises(NotImplementedError, match="intrinsic tangent-space"):
        mesh.gradient(values, implementation="warp")


def test_mesh_divergence_uses_functional_wiring(device: str):
    mesh = _tet_mesh(device)
    vector_field = mesh.points.clone()

    output = mesh.divergence(vector_field, implementation="torch")
    expected = compute_divergence_points_lsq(
        mesh,
        vector_field,
        implementation="torch",
    )

    torch.testing.assert_close(output, expected)


def test_mesh_laplacian_cotan_uses_functional_wiring(device: str):
    mesh = _tet_mesh(device)
    values = mesh.points.square().sum(dim=-1)

    output = mesh.laplacian(values, implementation="torch")
    expected = compute_laplacian_points_dec(mesh, values, implementation="torch")

    torch.testing.assert_close(output, expected)


def test_mesh_laplacian_lsq_method_uses_functional_wiring(device: str):
    mesh = _tet_mesh(device)
    values = mesh.points.square().sum(dim=-1)

    output = mesh.laplacian(values, method="lsq", implementation="torch")
    expected = compute_laplacian_points_lsq(
        mesh,
        values,
        implementation="torch",
    )

    torch.testing.assert_close(output, expected)
