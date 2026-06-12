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

from physicsnemo.nn.functional import (
    mesh_cotan_divergence,
    mesh_cotan_laplacian,
    mesh_lsq_curl,
    mesh_lsq_divergence,
    mesh_lsq_laplacian,
)
from physicsnemo.nn.functional.derivatives import (
    MeshCotanDivergence,
    MeshCotanLaplacian,
    MeshLSQCurl,
    MeshLSQDivergence,
    MeshLSQLaplacian,
)
from physicsnemo.nn.functional.derivatives._mesh_lsq_operator_utils import (
    make_knn_csr_case,
)
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


def _lsq_case(device: str, n_dims: int = 3):
    return make_knn_csr_case(
        device=device,
        n_entities=384,
        n_dims=n_dims,
        k_neighbors=14,
        seed=1900 + n_dims,
    )


def _simple_cotan_case(device: str):
    torch_device = torch.device(device)
    points = torch.tensor(
        [[0.0], [1.0], [2.0]],
        dtype=torch.float32,
        device=torch_device,
    )
    edges = torch.tensor([[0, 1], [1, 2]], dtype=torch.int64, device=torch_device)
    weights = torch.ones((2,), dtype=torch.float32, device=torch_device)
    volumes = torch.ones((3,), dtype=torch.float32, device=torch_device)
    return points, edges, weights, volumes


@pytest.mark.parametrize("n_dims", [2, 3])
def test_mesh_lsq_divergence_affine_field(device: str, n_dims: int):
    points, offsets, indices = _lsq_case(device, n_dims=n_dims)
    coeffs = torch.arange(1, n_dims + 1, device=points.device, dtype=points.dtype)
    vector_field = points * coeffs.view(1, -1)

    output = mesh_lsq_divergence(
        points,
        vector_field,
        offsets,
        indices,
        implementation="torch",
    )

    expected = torch.full_like(output, coeffs.sum())
    torch.testing.assert_close(output, expected, atol=4e-3, rtol=4e-3)


@pytest.mark.parametrize("n_dims", [2, 3])
def test_mesh_lsq_curl_affine_rotation(device: str, n_dims: int):
    points, offsets, indices = _lsq_case(device, n_dims=n_dims)
    if n_dims == 2:
        vector_field = torch.stack((-points[:, 1], points[:, 0]), dim=-1)
        expected = torch.full((points.shape[0],), 2.0, device=points.device)
    else:
        vector_field = torch.stack(
            (-points[:, 1], points[:, 0], points[:, 2]),
            dim=-1,
        )
        expected = torch.zeros_like(points)
        expected[:, 2] = 2.0

    output = mesh_lsq_curl(
        points,
        vector_field,
        offsets,
        indices,
        implementation="torch",
    )

    torch.testing.assert_close(output, expected, atol=4e-3, rtol=4e-3)


def test_mesh_lsq_laplacian_constant_is_zero(device: str):
    points, offsets, indices = _lsq_case(device, n_dims=3)
    values = torch.ones((points.shape[0],), dtype=points.dtype, device=points.device)

    output = mesh_lsq_laplacian(
        points,
        values,
        offsets,
        indices,
        implementation="torch",
    )

    torch.testing.assert_close(output, torch.zeros_like(output), atol=1e-6, rtol=1e-6)


def test_mesh_cotan_laplacian_simple_chain(device: str):
    _points, edges, weights, volumes = _simple_cotan_case(device)
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


def test_mesh_cotan_divergence_simple_chain(device: str):
    points, edges, weights, volumes = _simple_cotan_case(device)
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
@pytest.mark.parametrize(
    "spec",
    [
        MeshLSQDivergence,
        MeshLSQCurl,
        MeshLSQLaplacian,
        MeshCotanLaplacian,
        MeshCotanDivergence,
    ],
)
def test_mesh_vector_functional_backend_forward_parity(device: str, spec):
    for _label, args, kwargs in spec.make_inputs_forward(device=device):
        args_torch, kwargs_torch = clone_case(args, kwargs)
        args_warp, kwargs_warp = clone_case(args, kwargs)

        out_torch = spec.dispatch(
            *args_torch,
            implementation="torch",
            **kwargs_torch,
        )
        out_warp = spec.dispatch(
            *args_warp,
            implementation="warp",
            **kwargs_warp,
        )
        spec.compare_forward(out_warp, out_torch)


@requires_module("warp")
@pytest.mark.parametrize(
    "spec,grad_arg_indices",
    [
        (MeshLSQDivergence, (1,)),
        (MeshLSQCurl, (1,)),
        (MeshLSQLaplacian, (1,)),
        (MeshCotanLaplacian, (1, 2, 3)),
        (MeshCotanDivergence, (0, 2, 3, 4)),
    ],
)
def test_mesh_vector_functional_backend_backward_parity(
    device: str,
    spec,
    grad_arg_indices,
):
    _label, args, kwargs = next(iter(spec.make_inputs_backward(device=device)))
    args_torch, kwargs_torch = clone_case(args, kwargs)
    args_warp, kwargs_warp = clone_case(args, kwargs)

    out_torch = spec.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_torch.square().mean().backward()

    out_warp = spec.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    out_warp.square().mean().backward()

    for arg_index in grad_arg_indices:
        grad_torch = args_torch[arg_index].grad
        grad_warp = args_warp[arg_index].grad
        assert grad_torch is not None
        assert grad_warp is not None
        spec.compare_backward(grad_warp, grad_torch)
