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

from physicsnemo.nn.functional import magnetic_field_update
from physicsnemo.nn.functional.electromagnetics import MagneticFieldUpdate
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case

_MATERIAL_MODES = ("scalar-scalar", "scalar-field", "field-scalar", "field-field")


# Build deterministic test inputs for explicit material variants.
def _build_case(
    device: str,
    material_mode: str,
    grid_n: int = 10,
    seed: int = 3031,
):
    torch_device = torch.device(device)
    generator = torch.Generator(device=torch_device)
    generator.manual_seed(seed)

    electric_field = torch.randn(
        3,
        grid_n,
        grid_n,
        grid_n,
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    )
    magnetic_field = torch.randn(
        3,
        grid_n,
        grid_n,
        grid_n,
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    )

    mu_field = torch.empty(
        grid_n,
        grid_n,
        grid_n,
        device=torch_device,
        dtype=torch.float32,
    ).uniform_(0.5, 3.0, generator=generator)
    sigma_field = torch.empty(
        grid_n,
        grid_n,
        grid_n,
        device=torch_device,
        dtype=torch.float32,
    ).uniform_(0.0, 0.03, generator=generator)

    if material_mode == "scalar-scalar":
        mu = 1.5
        sigma_m = 0.01
    elif material_mode == "scalar-field":
        mu = 1.5
        sigma_m = sigma_field
    elif material_mode == "field-scalar":
        mu = mu_field
        sigma_m = 0.01
    elif material_mode == "field-field":
        mu = mu_field
        sigma_m = sigma_field
    else:
        raise ValueError(f"unsupported material mode: {material_mode}")

    spacing = torch.tensor(
        [0.010, 0.011, 0.013],
        device=torch_device,
        dtype=torch.float32,
    )
    dt = 0.00125

    args = (
        electric_field,
        magnetic_field,
        mu,
        sigma_m,
        spacing,
        dt,
    )
    kwargs = {"inplace": False}
    return args, kwargs


def test_magnetic_field_update_torch(device: str):
    args, kwargs = _build_case(
        device=device,
        material_mode="field-field",
        grid_n=8,
        seed=61051,
    )
    output = magnetic_field_update(*args, implementation="torch", **kwargs)
    reference = MagneticFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    MagneticFieldUpdate.compare_forward(output, reference)


@requires_module("warp")
def test_magnetic_field_update_warp(device: str):
    args, kwargs = _build_case(
        device=device,
        material_mode="field-field",
        grid_n=8,
        seed=61052,
    )
    output = magnetic_field_update(*args, implementation="warp", **kwargs)
    assert output.shape == args[1].shape


def test_magnetic_field_update_make_inputs_forward(device: str):
    label, args, kwargs = next(iter(MagneticFieldUpdate.make_inputs_forward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    output = MagneticFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    assert output.shape[0] == 3


def test_magnetic_field_update_make_inputs_backward(device: str):
    label, args, kwargs = next(iter(MagneticFieldUpdate.make_inputs_backward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    electric_field, magnetic_field, _, _, _, _ = args
    assert electric_field.requires_grad
    assert magnetic_field.requires_grad

    output = MagneticFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    output.sum().backward()
    assert electric_field.grad is not None


@requires_module("warp")
def test_magnetic_field_update_backend_forward_parity(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        material_mode="field-field",
        grid_n=10,
        seed=3031,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = MagneticFieldUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = MagneticFieldUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    MagneticFieldUpdate.compare_forward(out_warp, out_torch)


@requires_module("warp")
def test_magnetic_field_update_backend_backward_parity(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        material_mode="field-field",
        grid_n=10,
        seed=3031,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    electric_torch = args_torch[0].detach().requires_grad_(True)
    magnetic_torch = args_torch[1].detach().requires_grad_(True)
    electric_warp = args_warp[0].detach().requires_grad_(True)
    magnetic_warp = args_warp[1].detach().requires_grad_(True)

    mu_torch = args_torch[2]
    mu_warp = args_warp[2]
    if isinstance(mu_torch, torch.Tensor):
        mu_torch = mu_torch.detach().requires_grad_(True)
        mu_warp = mu_warp.detach().requires_grad_(True)

    sigma_torch = args_torch[3]
    sigma_warp = args_warp[3]
    if isinstance(sigma_torch, torch.Tensor):
        sigma_torch = sigma_torch.detach().requires_grad_(True)
        sigma_warp = sigma_warp.detach().requires_grad_(True)

    args_torch = (
        electric_torch,
        magnetic_torch,
        mu_torch,
        sigma_torch,
        args_torch[4],
        args_torch[5],
    )
    args_warp = (
        electric_warp,
        magnetic_warp,
        mu_warp,
        sigma_warp,
        args_warp[4],
        args_warp[5],
    )

    out_torch = MagneticFieldUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = MagneticFieldUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    MagneticFieldUpdate.compare_forward(out_warp, out_torch)

    grad_output = torch.randn_like(out_torch)
    out_torch.backward(grad_output)
    out_warp.backward(grad_output)

    assert electric_warp.grad is not None
    assert electric_torch.grad is not None
    MagneticFieldUpdate.compare_backward(electric_warp.grad, electric_torch.grad)

    assert magnetic_warp.grad is not None
    assert magnetic_torch.grad is not None
    MagneticFieldUpdate.compare_backward(magnetic_warp.grad, magnetic_torch.grad)

    if isinstance(mu_torch, torch.Tensor):
        assert isinstance(mu_warp, torch.Tensor)
        assert mu_warp.grad is not None
        assert mu_torch.grad is not None
        MagneticFieldUpdate.compare_backward(mu_warp.grad, mu_torch.grad)

    if isinstance(sigma_torch, torch.Tensor):
        assert isinstance(sigma_warp, torch.Tensor)
        assert sigma_warp.grad is not None
        assert sigma_torch.grad is not None
        MagneticFieldUpdate.compare_backward(sigma_warp.grad, sigma_torch.grad)


@requires_module("warp")
@pytest.mark.parametrize("material_mode", _MATERIAL_MODES)
def test_magnetic_field_update_backend_forward_parity_all_variants(
    device: str,
    material_mode: str,
):
    args_torch, kwargs_torch = _build_case(
        device=device,
        material_mode=material_mode,
        grid_n=8,
        seed=1337,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = MagneticFieldUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = MagneticFieldUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    MagneticFieldUpdate.compare_forward(out_warp, out_torch)


@requires_module("warp")
@pytest.mark.parametrize("material_mode", _MATERIAL_MODES)
def test_magnetic_field_update_backend_backward_parity_all_variants(
    device: str,
    material_mode: str,
):
    args_torch, kwargs_torch = _build_case(
        device=device,
        material_mode=material_mode,
        grid_n=8,
        seed=4242,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    electric_torch = args_torch[0].detach().requires_grad_(True)
    magnetic_torch = args_torch[1].detach().requires_grad_(True)
    electric_warp = args_warp[0].detach().requires_grad_(True)
    magnetic_warp = args_warp[1].detach().requires_grad_(True)

    mu_torch = args_torch[2]
    mu_warp = args_warp[2]
    if isinstance(mu_torch, torch.Tensor):
        mu_torch = mu_torch.detach().requires_grad_(True)
        mu_warp = mu_warp.detach().requires_grad_(True)

    sigma_torch = args_torch[3]
    sigma_warp = args_warp[3]
    if isinstance(sigma_torch, torch.Tensor):
        sigma_torch = sigma_torch.detach().requires_grad_(True)
        sigma_warp = sigma_warp.detach().requires_grad_(True)

    args_torch = (
        electric_torch,
        magnetic_torch,
        mu_torch,
        sigma_torch,
        args_torch[4],
        args_torch[5],
    )
    args_warp = (
        electric_warp,
        magnetic_warp,
        mu_warp,
        sigma_warp,
        args_warp[4],
        args_warp[5],
    )

    out_torch = MagneticFieldUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = MagneticFieldUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    MagneticFieldUpdate.compare_forward(out_warp, out_torch)

    grad_output = torch.randn_like(out_torch)
    out_torch.backward(grad_output)
    out_warp.backward(grad_output)

    assert electric_warp.grad is not None
    assert electric_torch.grad is not None
    MagneticFieldUpdate.compare_backward(electric_warp.grad, electric_torch.grad)

    assert magnetic_warp.grad is not None
    assert magnetic_torch.grad is not None
    MagneticFieldUpdate.compare_backward(magnetic_warp.grad, magnetic_torch.grad)

    if isinstance(mu_torch, torch.Tensor):
        assert isinstance(mu_warp, torch.Tensor)
        assert mu_warp.grad is not None
        assert mu_torch.grad is not None
        MagneticFieldUpdate.compare_backward(mu_warp.grad, mu_torch.grad)
    if isinstance(sigma_torch, torch.Tensor):
        assert isinstance(sigma_warp, torch.Tensor)
        assert sigma_warp.grad is not None
        assert sigma_torch.grad is not None
        MagneticFieldUpdate.compare_backward(sigma_warp.grad, sigma_torch.grad)


@requires_module("warp")
@pytest.mark.parametrize("implementation", ["torch", "warp"])
def test_magnetic_field_update_inplace_requires_grad_guard(
    device: str,
    implementation: str,
):
    args, kwargs = _build_case(
        device=device,
        material_mode="scalar-scalar",
        grid_n=8,
        seed=907,
    )

    magnetic = args[1].detach().requires_grad_(True)
    args = (args[0], magnetic) + args[2:]
    kwargs["inplace"] = True

    with pytest.raises(ValueError, match="requires gradients"):
        MagneticFieldUpdate.dispatch(
            *args,
            implementation=implementation,
            **kwargs,
        )


@requires_module("warp")
@pytest.mark.parametrize("implementation", ["torch", "warp"])
def test_magnetic_field_update_inplace_requires_grad_guard_materials(
    device: str,
    implementation: str,
):
    args, kwargs = _build_case(
        device=device,
        material_mode="scalar-scalar",
        grid_n=8,
        seed=908,
    )

    mu = args[2]
    if not isinstance(mu, torch.Tensor):
        nx, ny, nz = args[1].shape[1:]
        mu = torch.full((nx, ny, nz), 1.5, device=args[1].device, dtype=torch.float32)
    mu = mu.detach().requires_grad_(True)
    args = args[:2] + (mu,) + args[3:]
    kwargs["inplace"] = True

    with pytest.raises(ValueError, match="requires gradients"):
        MagneticFieldUpdate.dispatch(
            *args,
            implementation=implementation,
            **kwargs,
        )


@requires_module("warp")
@pytest.mark.parametrize("implementation", ["torch", "warp"])
def test_magnetic_field_update_spacing_grad_rejected(
    device: str,
    implementation: str,
):
    args, kwargs = _build_case(
        device=device,
        material_mode="field-field",
        grid_n=8,
        seed=909,
    )

    spacing = args[4].detach().requires_grad_(True)
    args = args[:4] + (spacing,) + args[5:]

    with pytest.raises(ValueError, match="spacing gradients are not supported"):
        MagneticFieldUpdate.dispatch(
            *args,
            implementation=implementation,
            **kwargs,
        )


@requires_module("warp")
@pytest.mark.parametrize("implementation", ["torch", "warp"])
def test_magnetic_field_update_inplace_matches_out_of_place(
    device: str,
    implementation: str,
):
    args, kwargs = _build_case(
        device=device,
        material_mode="field-field",
        grid_n=10,
        seed=910,
    )

    kwargs["inplace"] = False
    out_ref = MagneticFieldUpdate.dispatch(
        *args,
        implementation=implementation,
        **kwargs,
    )

    args_inplace, kwargs_inplace = clone_case(args, kwargs)
    kwargs_inplace["inplace"] = True
    magnetic_inplace = args_inplace[1]
    out_inplace = MagneticFieldUpdate.dispatch(
        *args_inplace,
        implementation=implementation,
        **kwargs_inplace,
    )

    assert out_inplace.data_ptr() == magnetic_inplace.data_ptr(), (
        f"in-place path did not return the input tensor for '{implementation}'"
    )
    torch.testing.assert_close(out_inplace, out_ref, atol=5e-5, rtol=1e-4)


@requires_module("warp")
@pytest.mark.parametrize("implementation", ["torch", "warp"])
@pytest.mark.parametrize("material_mode", _MATERIAL_MODES)
def test_magnetic_field_update_inplace_matches_out_of_place_all_variants(
    device: str,
    implementation: str,
    material_mode: str,
):
    args, kwargs = _build_case(
        device=device,
        material_mode=material_mode,
        grid_n=8,
        seed=911,
    )

    kwargs["inplace"] = False
    out_ref = MagneticFieldUpdate.dispatch(
        *args,
        implementation=implementation,
        **kwargs,
    )

    args_inplace, kwargs_inplace = clone_case(args, kwargs)
    kwargs_inplace["inplace"] = True
    out_inplace = MagneticFieldUpdate.dispatch(
        *args_inplace,
        implementation=implementation,
        **kwargs_inplace,
    )

    torch.testing.assert_close(out_inplace, out_ref, atol=5e-5, rtol=1e-4)


@requires_module("warp")
@pytest.mark.parametrize("implementation", ["torch", "warp"])
def test_magnetic_field_update_channel_dim_materials(
    device: str,
    implementation: str,
):
    args, kwargs = _build_case(
        device=device,
        material_mode="field-field",
        grid_n=8,
        seed=912,
    )
    mu = args[2]
    sigma_m = args[3]
    assert isinstance(mu, torch.Tensor)
    assert isinstance(sigma_m, torch.Tensor)

    args_3d = args
    args_4d = (
        args[0],
        args[1],
        mu.unsqueeze(0),
        sigma_m.unsqueeze(0),
        args[4],
        args[5],
    )

    out_3d = MagneticFieldUpdate.dispatch(
        *args_3d,
        implementation=implementation,
        **kwargs,
    )
    out_4d = MagneticFieldUpdate.dispatch(
        *args_4d,
        implementation=implementation,
        **kwargs,
    )
    torch.testing.assert_close(out_4d, out_3d, atol=5e-5, rtol=1e-4)


@requires_module("warp")
@pytest.mark.parametrize("target", ["electric", "magnetic", "mu", "sigma", "spacing"])
def test_magnetic_field_update_warp_requires_contiguous(
    device: str,
    target: str,
):
    args, kwargs = _build_case(
        device=device,
        material_mode="field-field",
        grid_n=8,
        seed=913,
    )
    electric_field, magnetic_field, mu, sigma_m, spacing, dt = args
    assert isinstance(mu, torch.Tensor)
    assert isinstance(sigma_m, torch.Tensor)

    if target == "electric":
        electric_field = electric_field.transpose(1, 2)
    elif target == "magnetic":
        magnetic_field = magnetic_field.transpose(1, 2)
    elif target == "mu":
        mu = mu.transpose(0, 1)
    elif target == "sigma":
        sigma_m = sigma_m.transpose(0, 1)
    elif target == "spacing":
        spaced = torch.empty(6, device=spacing.device, dtype=spacing.dtype)
        spaced[::2] = spacing
        spacing = spaced[::2]

    with pytest.raises(ValueError, match="contiguous"):
        MagneticFieldUpdate.dispatch(
            electric_field,
            magnetic_field,
            mu,
            sigma_m,
            spacing,
            dt,
            implementation="warp",
            **kwargs,
        )


def test_magnetic_field_update_compare_forward_contract(device: str):
    _, args, kwargs = next(iter(MagneticFieldUpdate.make_inputs_forward(device)))
    output = MagneticFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    reference = output.detach().clone()
    MagneticFieldUpdate.compare_forward(output, reference)


def test_magnetic_field_update_compare_backward_contract(device: str):
    _, args, kwargs = next(iter(MagneticFieldUpdate.make_inputs_backward(device)))
    electric_field, magnetic_field, _, _, _, _ = args

    output = MagneticFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    output.sum().backward()

    assert electric_field.grad is not None
    assert magnetic_field.grad is not None
    MagneticFieldUpdate.compare_backward(
        electric_field.grad, electric_field.grad.detach().clone()
    )
    MagneticFieldUpdate.compare_backward(
        magnetic_field.grad, magnetic_field.grad.detach().clone()
    )


def test_magnetic_field_update_error_handling(device: str):
    args, kwargs = _build_case(
        device=device,
        material_mode="scalar-scalar",
        grid_n=6,
        seed=914,
    )
    invalid_magnetic = args[1].to(torch.float64)
    with pytest.raises(TypeError, match="magnetic_field must be float32"):
        MagneticFieldUpdate.dispatch(
            args[0],
            invalid_magnetic,
            *args[2:],
            implementation="torch",
            **kwargs,
        )
