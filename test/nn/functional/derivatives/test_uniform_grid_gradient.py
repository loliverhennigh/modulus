# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import torch

from physicsnemo.nn.functional import uniform_grid_gradient
from physicsnemo.nn.functional.derivatives import UniformGridGradient
from physicsnemo.nn.functional.derivatives.uniform_grid_gradient.uniform_grid_gradient import (
    _AUTO_3D_TORCH_COMPILED_MAX_NUMEL,
    _AUTO_3D_TORCH_MAX_NUMEL,
    _auto_select_implementation,
)
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


# Build periodic analytic fields for gradient correctness checks.
def _make_periodic_field(device: str, dims: int):
    torch_device = torch.device(device)

    if dims == 1:
        n0 = 512
        x0 = torch.arange(n0, device=torch_device, dtype=torch.float32) / float(n0)
        field = torch.sin(2.0 * torch.pi * x0)
        spacing = 1.0 / float(n0)
        expected = (2.0 * torch.pi) * torch.cos(2.0 * torch.pi * x0).unsqueeze(0)
        return field, spacing, expected

    if dims == 2:
        n0, n1 = 192, 160
        x0 = torch.arange(n0, device=torch_device, dtype=torch.float32) / float(n0)
        x1 = torch.arange(n1, device=torch_device, dtype=torch.float32) / float(n1)
        xx, yy = torch.meshgrid(x0, x1, indexing="ij")
        field = torch.sin(2.0 * torch.pi * xx) + 0.5 * torch.cos(4.0 * torch.pi * yy)
        spacing = (1.0 / float(n0), 1.0 / float(n1))
        grad_x = (2.0 * torch.pi) * torch.cos(2.0 * torch.pi * xx)
        grad_y = -2.0 * torch.pi * torch.sin(4.0 * torch.pi * yy)
        expected = torch.stack((grad_x, grad_y), dim=0)
        return field, spacing, expected

    n0, n1, n2 = 80, 72, 64
    x0 = torch.arange(n0, device=torch_device, dtype=torch.float32) / float(n0)
    x1 = torch.arange(n1, device=torch_device, dtype=torch.float32) / float(n1)
    x2 = torch.arange(n2, device=torch_device, dtype=torch.float32) / float(n2)
    xx, yy, zz = torch.meshgrid(x0, x1, x2, indexing="ij")
    field = (
        torch.sin(2.0 * torch.pi * xx)
        + 0.5 * torch.cos(2.0 * torch.pi * yy)
        + 0.25 * torch.sin(4.0 * torch.pi * zz)
    )
    spacing = (1.0 / float(n0), 1.0 / float(n1), 1.0 / float(n2))
    grad_x = (2.0 * torch.pi) * torch.cos(2.0 * torch.pi * xx)
    grad_y = -1.0 * torch.pi * torch.sin(2.0 * torch.pi * yy)
    grad_z = 1.0 * torch.pi * torch.cos(4.0 * torch.pi * zz)
    expected = torch.stack((grad_x, grad_y, grad_z), dim=0)
    return field, spacing, expected


# Validate torch backend against analytic periodic derivatives.
@pytest.mark.parametrize("dims", [1, 2, 3])
@pytest.mark.parametrize("order", [2, 4])
def test_uniform_grid_gradient_torch(device: str, dims: int, order: int):
    field, spacing, expected = _make_periodic_field(device, dims)
    output = UniformGridGradient.dispatch(
        field,
        spacing=spacing,
        order=order,
        implementation="torch",
    )
    torch.testing.assert_close(output, expected, atol=2e-2, rtol=2e-2)


# Validate higher-order stencil improves analytic error for smooth fields.
@pytest.mark.parametrize("dims", [1, 2, 3])
def test_uniform_grid_gradient_torch_order4_more_accurate(device: str, dims: int):
    field, spacing, expected = _make_periodic_field(device, dims)
    out_o2 = UniformGridGradient.dispatch(
        field,
        spacing=spacing,
        order=2,
        implementation="torch",
    )
    out_o4 = UniformGridGradient.dispatch(
        field,
        spacing=spacing,
        order=4,
        implementation="torch",
    )

    err_o2 = torch.linalg.vector_norm((out_o2 - expected).reshape(-1)).item()
    err_o4 = torch.linalg.vector_norm((out_o4 - expected).reshape(-1)).item()
    assert err_o4 < err_o2


# Validate warp backend against torch backend for representative cases.
@requires_module("warp")
def test_uniform_grid_gradient_warp(device: str):
    for _label, args, kwargs in UniformGridGradient.make_inputs_forward(device=device):
        args_torch, kwargs_torch = clone_case(args, kwargs)
        args_warp, kwargs_warp = clone_case(args, kwargs)

        out_torch = UniformGridGradient.dispatch(
            *args_torch,
            implementation="torch",
            **kwargs_torch,
        )
        out_warp = UniformGridGradient.dispatch(
            *args_warp,
            implementation="warp",
            **kwargs_warp,
        )
        UniformGridGradient.compare_forward(out_warp, out_torch)


# Validate torch-compiled backend parity against torch backend.
def test_uniform_grid_gradient_torch_compiled(device: str):
    for _label, args, kwargs in UniformGridGradient.make_inputs_forward(device=device):
        args_torch, kwargs_torch = clone_case(args, kwargs)
        args_comp, kwargs_comp = clone_case(args, kwargs)

        out_torch = UniformGridGradient.dispatch(
            *args_torch,
            implementation="torch",
            **kwargs_torch,
        )
        out_compiled = UniformGridGradient.dispatch(
            *args_comp,
            implementation="torch_compiled",
            **kwargs_comp,
        )
        UniformGridGradient.compare_forward(out_compiled, out_torch)


# Validate warp backward parity against torch for representative workloads.
@requires_module("warp")
def test_uniform_grid_gradient_warp_backward(device: str):
    for _label, args, kwargs in UniformGridGradient.make_inputs_backward(device=device):
        args_torch, kwargs_torch = clone_case(args, kwargs)
        args_warp, kwargs_warp = clone_case(args, kwargs)

        out_torch = UniformGridGradient.dispatch(
            *args_torch,
            implementation="torch",
            **kwargs_torch,
        )
        out_torch.square().mean().backward()
        grad_torch = args_torch[0].grad
        assert grad_torch is not None

        out_warp = UniformGridGradient.dispatch(
            *args_warp,
            implementation="warp",
            **kwargs_warp,
        )
        out_warp.square().mean().backward()
        grad_warp = args_warp[0].grad
        assert grad_warp is not None

        UniformGridGradient.compare_backward(grad_warp, grad_torch)


# Validate auto-dispatch default path matches explicit selected implementation.
def test_uniform_grid_gradient_auto_dispatch_matches_selected(device: str):
    field = torch.randn(64, 64, device=device, dtype=torch.float32)
    implementation = _auto_select_implementation(field)

    output_auto = uniform_grid_gradient(field, spacing=(1.0, 1.0))
    output_explicit = UniformGridGradient.dispatch(
        field,
        spacing=(1.0, 1.0),
        implementation=implementation,
    )
    torch.testing.assert_close(output_auto, output_explicit)


# Validate CUDA auto-dispatch heuristic structure across dimensions/sizes.
@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA required")
def test_uniform_grid_gradient_auto_dispatch_heuristic_cuda():
    field_1d = torch.randn(4096, device="cuda", dtype=torch.float32)
    assert _auto_select_implementation(field_1d) == "torch"

    field_2d = torch.randn(512, 512, device="cuda", dtype=torch.float32)
    assert _auto_select_implementation(field_2d) == "torch"

    torch_n = int(round(_AUTO_3D_TORCH_MAX_NUMEL ** (1.0 / 3.0)))
    field_3d_torch = torch.randn(
        torch_n,
        torch_n,
        torch_n,
        device="cuda",
        dtype=torch.float32,
    )
    assert field_3d_torch.numel() <= _AUTO_3D_TORCH_MAX_NUMEL
    assert _auto_select_implementation(field_3d_torch) == "torch"

    small_n = int(round(_AUTO_3D_TORCH_COMPILED_MAX_NUMEL ** (1.0 / 3.0)))
    field_3d_small = torch.randn(
        small_n,
        small_n,
        small_n,
        device="cuda",
        dtype=torch.float32,
    )
    assert field_3d_small.numel() > _AUTO_3D_TORCH_MAX_NUMEL
    assert field_3d_small.numel() <= _AUTO_3D_TORCH_COMPILED_MAX_NUMEL
    assert _auto_select_implementation(field_3d_small) == "torch_compiled"

    large_n = small_n + 1
    field_3d_large = torch.randn(
        large_n,
        large_n,
        large_n,
        device="cuda",
        dtype=torch.float32,
    )
    assert field_3d_large.numel() > _AUTO_3D_TORCH_COMPILED_MAX_NUMEL
    assert _auto_select_implementation(field_3d_large) == "warp"

    field_grad = torch.randn(
        64,
        64,
        64,
        device="cuda",
        dtype=torch.float32,
        requires_grad=True,
    )
    assert _auto_select_implementation(field_grad) == "warp"


# Validate exported functional API and error handling paths.
def test_uniform_grid_gradient_error_handling(device: str):
    field = torch.randn(16, device=device, dtype=torch.float32)

    output = uniform_grid_gradient(field, spacing=1.0)
    assert output.shape == (1, 16)
    assert output.dtype == torch.float32

    with pytest.raises(ValueError, match="supports 1D-3D fields"):
        UniformGridGradient.dispatch(
            torch.randn(4, 4, 4, 4, device=device, dtype=torch.float32),
            implementation="torch",
        )

    with pytest.raises(TypeError, match="floating-point"):
        UniformGridGradient.dispatch(
            torch.ones(8, device=device, dtype=torch.int32),
            implementation="torch",
        )

    with pytest.raises(ValueError, match="spacing must have"):
        UniformGridGradient.dispatch(
            torch.randn(8, 8, device=device, dtype=torch.float32),
            spacing=(1.0,),
            implementation="torch",
        )

    with pytest.raises(ValueError, match="strictly positive"):
        UniformGridGradient.dispatch(
            torch.randn(8, 8, device=device, dtype=torch.float32),
            spacing=(1.0, 0.0),
            implementation="torch",
        )

    with pytest.raises(ValueError, match="supports"):
        UniformGridGradient.dispatch(
            torch.randn(8, 8, device=device, dtype=torch.float32),
            order=6,
            implementation="torch",
        )

    with pytest.raises(TypeError, match="integer"):
        UniformGridGradient.dispatch(
            torch.randn(8, 8, device=device, dtype=torch.float32),
            order=2.0,  # type: ignore[arg-type]
            implementation="torch",
        )
