# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import torch

from physicsnemo.nn.functional import mesh_green_gauss_gradient
from physicsnemo.nn.functional.derivatives import MeshGreenGaussGradient
from physicsnemo.nn.functional.derivatives.mesh_green_gauss_gradient.utils import (
    build_geometry,
)
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


# Build a deterministic structured triangular mesh.
def _build_case(device: str, nx: int = 36, ny: int = 32):
    torch_device = torch.device(device)
    x = torch.linspace(0.0, 1.0, nx, device=torch_device, dtype=torch.float32)
    y = torch.linspace(0.0, 1.0, ny, device=torch_device, dtype=torch.float32)
    xx, yy = torch.meshgrid(x, y, indexing="ij")
    points = torch.stack((xx.reshape(-1), yy.reshape(-1)), dim=-1)

    cells = []
    for i in range(nx - 1):
        for j in range(ny - 1):
            p00 = i * ny + j
            p10 = (i + 1) * ny + j
            p01 = i * ny + (j + 1)
            p11 = (i + 1) * ny + (j + 1)
            cells.append((p00, p10, p11))
            cells.append((p00, p11, p01))
    cells = torch.tensor(cells, device=torch_device, dtype=torch.int64)
    return points.contiguous(), cells.contiguous()


# Validate torch Green-Gauss reconstruction on a linear field.
def test_mesh_green_gauss_gradient_torch(device: str):
    points, cells = _build_case(device=device, nx=40, ny=34)
    centroids = points[cells].mean(dim=1)
    coeff = torch.tensor([2.0, -3.0], device=points.device, dtype=torch.float32)
    values = (centroids * coeff).sum(dim=-1)

    output = MeshGreenGaussGradient.dispatch(
        points,
        cells,
        values,
        implementation="torch",
    )
    neighbors = build_geometry(points, cells)[1]
    interior = (neighbors >= 0).all(dim=1)
    expected = coeff.view(1, -1).expand(interior.sum(), -1)
    torch.testing.assert_close(output[interior], expected, atol=5e-2, rtol=5e-2)


# Validate warp backend forward parity against torch across benchmark cases.
@requires_module("warp")
def test_mesh_green_gauss_gradient_warp(device: str):
    for _label, args, kwargs in MeshGreenGaussGradient.make_inputs_forward(device=device):
        args_torch, kwargs_torch = clone_case(args, kwargs)
        args_warp, kwargs_warp = clone_case(args, kwargs)

        out_torch = MeshGreenGaussGradient.dispatch(
            *args_torch,
            implementation="torch",
            **kwargs_torch,
        )
        out_warp = MeshGreenGaussGradient.dispatch(
            *args_warp,
            implementation="warp",
            **kwargs_warp,
        )
        MeshGreenGaussGradient.compare_forward(out_warp, out_torch)


# Validate warp backend backward parity against torch on value gradients.
@requires_module("warp")
def test_mesh_green_gauss_gradient_warp_backward(device: str):
    for _label, args, kwargs in MeshGreenGaussGradient.make_inputs_backward(device=device):
        args_torch, kwargs_torch = clone_case(args, kwargs)
        args_warp, kwargs_warp = clone_case(args, kwargs)

        out_torch = MeshGreenGaussGradient.dispatch(
            *args_torch,
            implementation="torch",
            **kwargs_torch,
        )
        out_torch.square().mean().backward()
        grad_torch = args_torch[2].grad
        assert grad_torch is not None

        out_warp = MeshGreenGaussGradient.dispatch(
            *args_warp,
            implementation="warp",
            **kwargs_warp,
        )
        out_warp.square().mean().backward()
        grad_warp = args_warp[2].grad
        assert grad_warp is not None

        MeshGreenGaussGradient.compare_backward(grad_warp, grad_torch)


# Validate exported API and input validation paths.
def test_mesh_green_gauss_gradient_error_handling(device: str):
    points, cells = _build_case(device=device, nx=16, ny=14)
    values = torch.randn(cells.shape[0], device=points.device, dtype=torch.float32)

    output = mesh_green_gauss_gradient(points, cells, values)
    assert output.shape[0] == cells.shape[0]
    assert output.shape[1] == points.shape[1]

    with pytest.raises(ValueError, match="supports dims in"):
        bad_points = torch.randn(points.shape[0], 4, device=points.device, dtype=torch.float32)
        MeshGreenGaussGradient.dispatch(
            bad_points,
            cells,
            values,
            implementation="torch",
        )

    with pytest.raises(ValueError, match="must contain 3 vertices"):
        bad_cells = torch.randint(
            0,
            points.shape[0],
            (cells.shape[0], 4),
            device=points.device,
            dtype=torch.int64,
        )
        MeshGreenGaussGradient.dispatch(
            points,
            bad_cells,
            values,
            implementation="torch",
        )

    with pytest.raises(ValueError, match="leading dimension must match n_cells"):
        MeshGreenGaussGradient.dispatch(
            points,
            cells,
            values[:-1],
            implementation="torch",
        )
