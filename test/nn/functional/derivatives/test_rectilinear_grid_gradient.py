# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import torch

from physicsnemo.nn.functional import rectilinear_grid_gradient
from physicsnemo.nn.functional.derivatives import RectilinearGridGradient
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


# Build analytic periodic fields on nonuniform rectilinear coordinates.
def _make_periodic_case(device: str, dims: int):
    torch_device = torch.device(device)

    if dims == 1:
        n0 = 1024
        s0 = torch.linspace(0.0, 1.0, n0 + 1, device=torch_device)[:-1]
        x0 = s0 + 0.04 * torch.sin(2.0 * torch.pi * s0 + 0.2)
        field = torch.sin(2.0 * torch.pi * x0)
        expected = (2.0 * torch.pi) * torch.cos(2.0 * torch.pi * x0).unsqueeze(0)
        return field, (x0.to(torch.float32),), 1.0, expected

    if dims == 2:
        n0, n1 = 320, 256
        s0 = torch.linspace(0.0, 1.0, n0 + 1, device=torch_device)[:-1]
        s1 = torch.linspace(0.0, 1.0, n1 + 1, device=torch_device)[:-1]
        x0 = s0 + 0.04 * torch.sin(2.0 * torch.pi * s0 + 0.1)
        x1 = s1 + 0.03 * torch.sin(2.0 * torch.pi * s1 - 0.3)
        xx, yy = torch.meshgrid(x0, x1, indexing="ij")
        field = torch.sin(2.0 * torch.pi * xx) + 0.5 * torch.cos(2.0 * torch.pi * yy)
        grad_x = (2.0 * torch.pi) * torch.cos(2.0 * torch.pi * xx)
        grad_y = -1.0 * torch.pi * torch.sin(2.0 * torch.pi * yy)
        expected = torch.stack((grad_x, grad_y), dim=0)
        return field, (x0.to(torch.float32), x1.to(torch.float32)), (1.0, 1.0), expected

    n0, n1, n2 = 120, 96, 80
    s0 = torch.linspace(0.0, 1.0, n0 + 1, device=torch_device)[:-1]
    s1 = torch.linspace(0.0, 1.0, n1 + 1, device=torch_device)[:-1]
    s2 = torch.linspace(0.0, 1.0, n2 + 1, device=torch_device)[:-1]
    x0 = s0 + 0.04 * torch.sin(2.0 * torch.pi * s0 + 0.1)
    x1 = s1 + 0.03 * torch.sin(2.0 * torch.pi * s1 - 0.3)
    x2 = s2 + 0.02 * torch.sin(2.0 * torch.pi * s2 + 0.6)
    xx, yy, zz = torch.meshgrid(x0, x1, x2, indexing="ij")
    field = (
        torch.sin(2.0 * torch.pi * xx)
        + 0.5 * torch.cos(2.0 * torch.pi * yy)
        + 0.25 * torch.sin(2.0 * torch.pi * zz)
    )
    grad_x = (2.0 * torch.pi) * torch.cos(2.0 * torch.pi * xx)
    grad_y = -1.0 * torch.pi * torch.sin(2.0 * torch.pi * yy)
    grad_z = 0.5 * torch.pi * torch.cos(2.0 * torch.pi * zz)
    expected = torch.stack((grad_x, grad_y, grad_z), dim=0)
    return (
        field,
        (x0.to(torch.float32), x1.to(torch.float32), x2.to(torch.float32)),
        (1.0, 1.0, 1.0),
        expected,
    )


# Validate torch backend against analytic periodic derivatives.
@pytest.mark.parametrize("dims", [1, 2, 3])
def test_rectilinear_grid_gradient_torch(device: str, dims: int):
    field, coordinates, periods, expected = _make_periodic_case(device, dims)
    output = RectilinearGridGradient.dispatch(
        field.to(torch.float32),
        coordinates,
        periods=periods,
        implementation="torch",
    )
    torch.testing.assert_close(output, expected, atol=3e-2, rtol=3e-2)


# Validate warp backend forward parity against torch across benchmark cases.
@requires_module("warp")
def test_rectilinear_grid_gradient_warp(device: str):
    for _label, args, kwargs in RectilinearGridGradient.make_inputs_forward(device=device):
        args_torch, kwargs_torch = clone_case(args, kwargs)
        args_warp, kwargs_warp = clone_case(args, kwargs)

        out_torch = RectilinearGridGradient.dispatch(
            *args_torch,
            implementation="torch",
            **kwargs_torch,
        )
        out_warp = RectilinearGridGradient.dispatch(
            *args_warp,
            implementation="warp",
            **kwargs_warp,
        )
        RectilinearGridGradient.compare_forward(out_warp, out_torch)


# Validate warp backend backward parity against torch.
@requires_module("warp")
def test_rectilinear_grid_gradient_warp_backward(device: str):
    for _label, args, kwargs in RectilinearGridGradient.make_inputs_backward(device=device):
        args_torch, kwargs_torch = clone_case(args, kwargs)
        args_warp, kwargs_warp = clone_case(args, kwargs)

        out_torch = RectilinearGridGradient.dispatch(
            *args_torch,
            implementation="torch",
            **kwargs_torch,
        )
        out_torch.square().mean().backward()
        grad_torch = args_torch[0].grad
        assert grad_torch is not None

        out_warp = RectilinearGridGradient.dispatch(
            *args_warp,
            implementation="warp",
            **kwargs_warp,
        )
        out_warp.square().mean().backward()
        grad_warp = args_warp[0].grad
        assert grad_warp is not None

        RectilinearGridGradient.compare_backward(grad_warp, grad_torch)


# Validate exported API and input validation paths.
def test_rectilinear_grid_gradient_error_handling(device: str):
    x = torch.linspace(0.0, 1.0, 17, device=device)[:-1]
    field = torch.sin(2.0 * torch.pi * x).to(torch.float32)

    output = rectilinear_grid_gradient(field, (x.to(torch.float32),), periods=1.0)
    assert output.shape == (1, 16)

    with pytest.raises(ValueError, match="supports 1D-3D fields"):
        RectilinearGridGradient.dispatch(
            torch.randn(2, 2, 2, 2, device=device, dtype=torch.float32),
            (x, x, x, x),
            periods=1.0,
            implementation="torch",
        )

    with pytest.raises(ValueError, match="must contain one axis tensor"):
        RectilinearGridGradient.dispatch(
            torch.randn(32, 32, device=device, dtype=torch.float32),
            (torch.linspace(0.0, 1.0, 32, device=device),),
            periods=1.0,
            implementation="torch",
        )

    with pytest.raises(ValueError, match="strictly increasing"):
        bad_x = torch.tensor([0.0, 0.3, 0.2, 0.8], device=device, dtype=torch.float32)
        bad_f = torch.randn(4, device=device, dtype=torch.float32)
        RectilinearGridGradient.dispatch(
            bad_f,
            (bad_x,),
            periods=1.0,
            implementation="torch",
        )

    with pytest.raises(ValueError, match="must be larger than coordinate span"):
        RectilinearGridGradient.dispatch(
            torch.randn(16, device=device, dtype=torch.float32),
            (torch.linspace(0.0, 1.0, 16, device=device, dtype=torch.float32),),
            periods=0.8,
            implementation="torch",
        )
