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

from physicsnemo.nn.functional import mesh_cotan_laplacian
from physicsnemo.nn.functional.derivatives import MeshCotanLaplacian
from test.conftest import requires_module
from test.nn.functional.derivatives._mesh_vector_calculus_test_utils import (
    check_backend_backward_parity,
    check_backend_forward_parity,
    make_simple_cotan_case,
)


def test_mesh_cotan_laplacian_simple_chain(device: str):
    _points, edges, weights, volumes = make_simple_cotan_case(device)
    values = torch.tensor([0.0, 1.0, 3.0], device=torch.device(device))

    output = mesh_cotan_laplacian(
        edges,
        weights,
        volumes,
        values,
        implementation="torch",
    )

    expected = torch.tensor([1.0, 1.0, -2.0], device=values.device)
    torch.testing.assert_close(output, expected)


def test_mesh_cotan_laplacian_empty(device: str):
    torch_device = torch.device(device)
    edges = torch.empty((0, 2), dtype=torch.int64, device=torch_device)
    weights = torch.empty((0,), dtype=torch.float32, device=torch_device)
    volumes = torch.empty((0,), dtype=torch.float32, device=torch_device)
    values = torch.empty((0,), dtype=torch.float32, device=torch_device)

    output = mesh_cotan_laplacian(
        edges, weights, volumes, values, implementation="torch"
    )
    assert output.shape == values.shape
    assert output.dtype == values.dtype


@requires_module("warp")
def test_mesh_cotan_laplacian_empty_backend_parity(device: str):
    torch_device = torch.device(device)
    edges = torch.empty((0, 2), dtype=torch.int64, device=torch_device)
    weights = torch.empty((0,), dtype=torch.float32, device=torch_device)
    volumes = torch.empty((0,), dtype=torch.float32, device=torch_device)
    values = torch.empty((0, 2), dtype=torch.float32, device=torch_device)

    torch_output = mesh_cotan_laplacian(
        edges, weights, volumes, values, implementation="torch"
    )
    warp_output = mesh_cotan_laplacian(
        edges, weights, volumes, values, implementation="warp"
    )
    torch.testing.assert_close(warp_output, torch_output)


@requires_module("warp")
def test_mesh_cotan_laplacian_clamped_volume_gradient(device: str):
    _points, edges, weights, volumes = make_simple_cotan_case(device)
    volumes[0] = 0.0
    values = torch.tensor([0.0, 1.0, 3.0], device=volumes.device)

    torch_volumes = volumes.clone().requires_grad_(True)
    torch_output = mesh_cotan_laplacian(
        edges, weights, torch_volumes, values, implementation="torch"
    )
    torch_output.sum().backward()

    warp_volumes = volumes.clone().requires_grad_(True)
    warp_output = mesh_cotan_laplacian(
        edges, weights, warp_volumes, values, implementation="warp"
    )
    warp_output.sum().backward()

    assert torch_volumes.grad is not None
    assert warp_volumes.grad is not None
    assert torch_volumes.grad[0] == 0
    torch.testing.assert_close(warp_volumes.grad, torch_volumes.grad)


@requires_module("warp")
def test_mesh_cotan_laplacian_mixed_volume_dtype(device: str):
    _points, edges, weights, volumes = make_simple_cotan_case(device)
    values = torch.tensor([0.0, 1.0, 3.0], device=volumes.device)
    volumes = volumes.to(torch.float64)

    torch_output = mesh_cotan_laplacian(
        edges, weights, volumes, values, implementation="torch"
    )
    warp_output = mesh_cotan_laplacian(
        edges, weights, volumes, values, implementation="warp"
    )
    assert torch_output.dtype == values.dtype
    assert warp_output.dtype == values.dtype
    torch.testing.assert_close(warp_output, torch_output)


@requires_module("warp")
def test_mesh_cotan_laplacian_backend_forward_parity(device: str):
    check_backend_forward_parity(device, MeshCotanLaplacian)


@requires_module("warp")
def test_mesh_cotan_laplacian_backend_backward_parity(device: str):
    check_backend_backward_parity(device, MeshCotanLaplacian, (1, 2, 3))
