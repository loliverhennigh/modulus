# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import torch

from physicsnemo.nn.functional import mesh_lsq_gradient
from physicsnemo.nn.functional.derivatives import MeshLSQGradient
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


# Build deterministic KNN-CSR test data on random points.
def _make_case(device: str, n_entities: int, n_dims: int, k_neighbors: int):
    torch_device = torch.device(device)
    generator = torch.Generator(device=torch_device)
    generator.manual_seed(1234 + n_entities + n_dims)

    points = torch.rand((n_entities, n_dims), generator=generator, device=torch_device)
    dists = torch.cdist(points, points)
    knn = torch.topk(dists, k=k_neighbors + 1, largest=False, dim=1).indices[:, 1:]

    offsets = torch.arange(
        0,
        n_entities * k_neighbors + 1,
        k_neighbors,
        device=torch_device,
        dtype=torch.int64,
    )
    indices = knn.reshape(-1).to(torch.int64)
    return points, offsets, indices


# Validate torch LSQ reconstruction on an affine scalar field.
@pytest.mark.parametrize("n_dims", [1, 2, 3])
def test_mesh_lsq_gradient_torch(device: str, n_dims: int):
    points, offsets, indices = _make_case(device, n_entities=1024, n_dims=n_dims, k_neighbors=16)

    coeff = torch.arange(1, n_dims + 1, device=points.device, dtype=torch.float32)
    values = (points * coeff).sum(dim=-1)

    output = MeshLSQGradient.dispatch(
        points,
        values,
        offsets,
        indices,
        implementation="torch",
    )

    expected = coeff.view(1, -1).expand(points.shape[0], -1)
    torch.testing.assert_close(output, expected, atol=3e-3, rtol=3e-3)


# Validate warp backend parity against torch across benchmark representative inputs.
@requires_module("warp")
def test_mesh_lsq_gradient_warp(device: str):
    for _label, args, kwargs in MeshLSQGradient.make_inputs_forward(device=device):
        args_torch, kwargs_torch = clone_case(args, kwargs)
        args_warp, kwargs_warp = clone_case(args, kwargs)

        out_torch = MeshLSQGradient.dispatch(
            *args_torch,
            implementation="torch",
            **kwargs_torch,
        )
        out_warp = MeshLSQGradient.dispatch(
            *args_warp,
            implementation="warp",
            **kwargs_warp,
        )
        MeshLSQGradient.compare_forward(out_warp, out_torch)


# Validate warp backward parity against torch on differentiable value fields.
@requires_module("warp")
def test_mesh_lsq_gradient_warp_backward(device: str):
    for _label, args, kwargs in MeshLSQGradient.make_inputs_backward(device=device):
        args_torch, kwargs_torch = clone_case(args, kwargs)
        args_warp, kwargs_warp = clone_case(args, kwargs)

        out_torch = MeshLSQGradient.dispatch(
            *args_torch,
            implementation="torch",
            **kwargs_torch,
        )
        out_torch.square().mean().backward()
        grad_torch = args_torch[1].grad
        assert grad_torch is not None

        out_warp = MeshLSQGradient.dispatch(
            *args_warp,
            implementation="warp",
            **kwargs_warp,
        )
        out_warp.square().mean().backward()
        grad_warp = args_warp[1].grad
        assert grad_warp is not None

        MeshLSQGradient.compare_backward(grad_warp, grad_torch)


# Validate warp backend on 1D input parity against torch.
@requires_module("warp")
def test_mesh_lsq_gradient_warp_1d(device: str):
    points, offsets, indices = _make_case(device, n_entities=512, n_dims=1, k_neighbors=16)
    values = torch.sin(2.0 * torch.pi * points[:, 0]).to(torch.float32)

    out_torch = MeshLSQGradient.dispatch(
        points,
        values,
        offsets,
        indices,
        implementation="torch",
    )
    out_warp = MeshLSQGradient.dispatch(
        points,
        values,
        offsets,
        indices,
        implementation="warp",
    )
    MeshLSQGradient.compare_forward(out_warp, out_torch)


# Validate exported API and input validation paths.
def test_mesh_lsq_gradient_error_handling(device: str):
    points, offsets, indices = _make_case(device, n_entities=128, n_dims=3, k_neighbors=8)
    values = torch.sin(points[:, 0])

    output = mesh_lsq_gradient(points, values, offsets, indices)
    assert output.shape == (points.shape[0], points.shape[1])
    assert output.dtype == torch.float32

    with pytest.raises(ValueError, match=r"must have shape \(n_entities \+ 1,\)"):
        MeshLSQGradient.dispatch(
            points,
            values,
            offsets[:-1],
            indices,
            implementation="torch",
        )

    with pytest.raises(ValueError, match=r"must equal len\(neighbor_indices\)"):
        bad_offsets = offsets.clone()
        bad_offsets[-1] = bad_offsets[-1] - 1
        MeshLSQGradient.dispatch(
            points,
            values,
            bad_offsets,
            indices,
            implementation="torch",
        )

    with pytest.raises(ValueError, match="neighbor_offsets must be non-decreasing"):
        bad_offsets = offsets.clone()
        mid = bad_offsets.shape[0] // 2
        bad_offsets[mid] = bad_offsets[mid - 1] - 1
        MeshLSQGradient.dispatch(
            points,
            values,
            bad_offsets,
            indices,
            implementation="torch",
        )

    with pytest.raises(ValueError, match="values leading dimension must match points"):
        MeshLSQGradient.dispatch(
            points,
            values[:-1],
            offsets,
            indices,
            implementation="torch",
        )

    with pytest.raises(TypeError, match="neighbor_offsets must be int32 or int64"):
        MeshLSQGradient.dispatch(
            points,
            values,
            offsets.to(torch.float32),
            indices,
            implementation="torch",
        )

    with pytest.raises(ValueError, match="must satisfy 0 <= index < n_entities"):
        bad_indices = indices.clone()
        bad_indices[0] = points.shape[0]
        MeshLSQGradient.dispatch(
            points,
            values,
            offsets,
            bad_indices,
            implementation="torch",
        )

    if torch.cuda.is_available():
        other_device = torch.device("cuda" if points.device.type == "cpu" else "cpu")
        with pytest.raises(ValueError, match="must be on the same device"):
            MeshLSQGradient.dispatch(
                points,
                values,
                offsets.to(other_device),
                indices,
                implementation="torch",
            )


# Validate warp backend input validation paths mirror torch behavior.
@requires_module("warp")
def test_mesh_lsq_gradient_warp_error_handling(device: str):
    points, offsets, indices = _make_case(device, n_entities=128, n_dims=3, k_neighbors=8)
    values = torch.sin(points[:, 0])

    with pytest.raises(ValueError, match="neighbor_offsets must be non-decreasing"):
        bad_offsets = offsets.clone()
        mid = bad_offsets.shape[0] // 2
        bad_offsets[mid] = bad_offsets[mid - 1] - 1
        MeshLSQGradient.dispatch(
            points,
            values,
            bad_offsets,
            indices,
            implementation="warp",
        )

    if torch.cuda.is_available():
        other_device = torch.device("cuda" if points.device.type == "cpu" else "cpu")
        with pytest.raises(ValueError, match="must be on the same device"):
            MeshLSQGradient.dispatch(
                points,
                values,
                offsets.to(other_device),
                indices,
                implementation="warp",
            )
