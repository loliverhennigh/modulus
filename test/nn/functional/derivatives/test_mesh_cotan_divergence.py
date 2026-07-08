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

import torch

from physicsnemo.nn.functional import mesh_cotan_divergence
from physicsnemo.nn.functional.derivatives import MeshCotanDivergence
from test.conftest import requires_module
from test.nn.functional.derivatives._mesh_vector_calculus_test_utils import (
    check_backend_backward_parity,
    check_backend_forward_parity,
    make_simple_cotan_case,
)


def test_mesh_cotan_divergence_simple_chain(device: str):
    points, edges, weights, volumes = make_simple_cotan_case(device)
    vector_field = points.clone()

    output = mesh_cotan_divergence(
        points,
        edges,
        weights,
        volumes,
        vector_field,
        implementation="torch",
    )

    expected = torch.tensor([0.5, 1.0, -1.5], device=points.device)
    torch.testing.assert_close(output, expected)


@requires_module("warp")
def test_mesh_cotan_divergence_clamped_volume_gradient(device: str):
    points, edges, weights, volumes = make_simple_cotan_case(device)
    volumes[0] = 0.0
    vector_field = points.clone()

    torch_volumes = volumes.clone().requires_grad_(True)
    torch_output = mesh_cotan_divergence(
        points,
        edges,
        weights,
        torch_volumes,
        vector_field,
        implementation="torch",
    )
    torch_output.sum().backward()

    warp_volumes = volumes.clone().requires_grad_(True)
    warp_output = mesh_cotan_divergence(
        points,
        edges,
        weights,
        warp_volumes,
        vector_field,
        implementation="warp",
    )
    warp_output.sum().backward()

    assert torch_volumes.grad is not None
    assert warp_volumes.grad is not None
    assert torch_volumes.grad[0] == 0
    torch.testing.assert_close(warp_volumes.grad, torch_volumes.grad)


@requires_module("warp")
def test_mesh_cotan_divergence_mixed_volume_dtype(device: str):
    points, edges, weights, volumes = make_simple_cotan_case(device)
    vector_field = points.to(torch.float32)
    points = points.to(torch.float64)
    weights = weights.to(torch.float64)
    volumes = volumes.to(torch.float64)

    torch_output = mesh_cotan_divergence(
        points,
        edges,
        weights,
        volumes,
        vector_field,
        implementation="torch",
    )
    warp_output = mesh_cotan_divergence(
        points,
        edges,
        weights,
        volumes,
        vector_field,
        implementation="warp",
    )
    assert torch_output.dtype == vector_field.dtype
    assert warp_output.dtype == vector_field.dtype
    torch.testing.assert_close(warp_output, torch_output)


@requires_module("warp")
def test_mesh_cotan_divergence_backend_forward_parity(device: str):
    check_backend_forward_parity(device, MeshCotanDivergence)


@requires_module("warp")
def test_mesh_cotan_divergence_backend_backward_parity(device: str):
    check_backend_backward_parity(device, MeshCotanDivergence, (0, 2, 3, 4))
