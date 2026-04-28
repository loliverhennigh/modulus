# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import torch

from physicsnemo.nn.functional import pml_magnetic_field_update
from physicsnemo.nn.functional.electromagnetics import PMLMagneticFieldUpdate
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


# Build deterministic PML magnetic-field correction inputs.
def _build_case(
    device: str,
    pml_shape: tuple[int, int, int],
    offset: tuple[int, int, int],
    mu_mode: str = "field",
    seed: int = 12011,
):
    torch_device = torch.device(device)
    generator = torch.Generator(device=torch_device)
    generator.manual_seed(seed)

    magnetic_field = torch.randn(
        3,
        16,
        16,
        16,
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    )
    pml_layer = torch.zeros(
        36,
        pml_shape[0],
        pml_shape[1],
        pml_shape[2],
        device=torch_device,
        dtype=torch.float32,
    )
    pml_layer[3:6] = torch.randn(
        3,
        pml_shape[0],
        pml_shape[1],
        pml_shape[2],
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    )

    if mu_mode == "field":
        mu: float | torch.Tensor = torch.empty(
            16,
            16,
            16,
            device=torch_device,
            dtype=torch.float32,
        ).uniform_(0.5, 3.0, generator=generator)
    elif mu_mode == "scalar":
        mu = 1.8
    else:
        raise ValueError(f"unsupported mu_mode: {mu_mode}")
    spacing = torch.tensor([0.01, 0.012, 0.014], device=torch_device)

    args = (
        magnetic_field,
        pml_layer,
        mu,
        spacing,
        offset,
        0.00125,
    )
    kwargs = {"inplace": False}
    return args, kwargs


def test_pml_magnetic_field_update_torch(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(0, 0, 0),
        seed=12011,
    )
    output = pml_magnetic_field_update(*args, implementation="torch", **kwargs)
    reference = PMLMagneticFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    PMLMagneticFieldUpdate.compare_forward(output, reference)


@requires_module("warp")
def test_pml_magnetic_field_update_warp(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        seed=12012,
    )
    output = pml_magnetic_field_update(*args, implementation="warp", **kwargs)
    assert output.shape == args[0].shape


def test_pml_magnetic_field_update_make_inputs_forward(device: str):
    label, args, kwargs = next(iter(PMLMagneticFieldUpdate.make_inputs_forward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    output = PMLMagneticFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    assert output.shape[0] == 3


def test_pml_magnetic_field_update_make_inputs_backward(device: str):
    label, args, kwargs = next(iter(PMLMagneticFieldUpdate.make_inputs_backward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    magnetic_field, pml_layer, mu, _, _, _ = args
    assert magnetic_field.requires_grad
    assert pml_layer.requires_grad
    assert mu.requires_grad

    output = PMLMagneticFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    output.sum().backward()
    assert magnetic_field.grad is not None
    assert pml_layer.grad is not None
    assert mu.grad is not None


@requires_module("warp")
def test_pml_magnetic_field_update_backend_forward_parity(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        seed=12013,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = PMLMagneticFieldUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLMagneticFieldUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLMagneticFieldUpdate.compare_forward(out_warp, out_torch)


@requires_module("warp")
def test_pml_magnetic_field_update_backend_backward_parity(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        seed=12013,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    magnetic_torch = args_torch[0].detach().requires_grad_(True)
    pml_torch = args_torch[1].detach().requires_grad_(True)
    mu_torch = args_torch[2].detach().requires_grad_(True)

    magnetic_warp = args_warp[0].detach().requires_grad_(True)
    pml_warp = args_warp[1].detach().requires_grad_(True)
    mu_warp = args_warp[2].detach().requires_grad_(True)

    args_torch = (
        magnetic_torch,
        pml_torch,
        mu_torch,
        args_torch[3],
        args_torch[4],
        args_torch[5],
    )
    args_warp = (
        magnetic_warp,
        pml_warp,
        mu_warp,
        args_warp[3],
        args_warp[4],
        args_warp[5],
    )

    out_torch = PMLMagneticFieldUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLMagneticFieldUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLMagneticFieldUpdate.compare_forward(out_warp, out_torch)

    grad_output = torch.randn_like(out_torch)
    out_torch.backward(grad_output)
    out_warp.backward(grad_output)

    assert magnetic_warp.grad is not None
    assert magnetic_torch.grad is not None
    PMLMagneticFieldUpdate.compare_backward(magnetic_warp.grad, magnetic_torch.grad)

    assert pml_warp.grad is not None
    assert pml_torch.grad is not None
    PMLMagneticFieldUpdate.compare_backward(pml_warp.grad, pml_torch.grad)

    assert mu_warp.grad is not None
    assert mu_torch.grad is not None
    PMLMagneticFieldUpdate.compare_backward(mu_warp.grad, mu_torch.grad)


@requires_module("warp")
def test_pml_magnetic_field_update_backend_forward_parity_scalar_mu(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        mu_mode="scalar",
        seed=12016,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = PMLMagneticFieldUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLMagneticFieldUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLMagneticFieldUpdate.compare_forward(out_warp, out_torch)


@requires_module("warp")
def test_pml_magnetic_field_update_backend_backward_parity_scalar_mu(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        mu_mode="scalar",
        seed=12017,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    magnetic_torch = args_torch[0].detach().requires_grad_(True)
    pml_torch = args_torch[1].detach().requires_grad_(True)

    magnetic_warp = args_warp[0].detach().requires_grad_(True)
    pml_warp = args_warp[1].detach().requires_grad_(True)

    args_torch = (
        magnetic_torch,
        pml_torch,
        args_torch[2],
        args_torch[3],
        args_torch[4],
        args_torch[5],
    )
    args_warp = (
        magnetic_warp,
        pml_warp,
        args_warp[2],
        args_warp[3],
        args_warp[4],
        args_warp[5],
    )

    out_torch = PMLMagneticFieldUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLMagneticFieldUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLMagneticFieldUpdate.compare_forward(out_warp, out_torch)

    grad_output = torch.randn_like(out_torch)
    out_torch.backward(grad_output)
    out_warp.backward(grad_output)

    assert magnetic_warp.grad is not None
    assert magnetic_torch.grad is not None
    PMLMagneticFieldUpdate.compare_backward(magnetic_warp.grad, magnetic_torch.grad)

    assert pml_warp.grad is not None
    assert pml_torch.grad is not None
    PMLMagneticFieldUpdate.compare_backward(pml_warp.grad, pml_torch.grad)


def test_pml_magnetic_field_update_compare_forward_contract(device: str):
    _, args, kwargs = next(iter(PMLMagneticFieldUpdate.make_inputs_forward(device)))
    output = PMLMagneticFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    reference = output.detach().clone()
    PMLMagneticFieldUpdate.compare_forward(output, reference)


def test_pml_magnetic_field_update_compare_backward_contract(device: str):
    _, args, kwargs = next(iter(PMLMagneticFieldUpdate.make_inputs_backward(device)))
    magnetic_field, pml_layer, mu, _, _, _ = args

    output = PMLMagneticFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    output.sum().backward()

    assert magnetic_field.grad is not None
    assert pml_layer.grad is not None
    assert mu.grad is not None
    PMLMagneticFieldUpdate.compare_backward(
        magnetic_field.grad, magnetic_field.grad.detach().clone()
    )
    PMLMagneticFieldUpdate.compare_backward(
        pml_layer.grad, pml_layer.grad.detach().clone()
    )
    PMLMagneticFieldUpdate.compare_backward(mu.grad, mu.grad.detach().clone())


def test_pml_magnetic_field_update_error_handling(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(0, 0, 0),
        seed=12014,
    )
    bad_mu = args[2].to(torch.float64)
    with pytest.raises(TypeError, match="mu tensor must be float32"):
        PMLMagneticFieldUpdate.dispatch(
            args[0],
            args[1],
            bad_mu,
            args[3],
            args[4],
            args[5],
            implementation="torch",
            **kwargs,
        )


def test_pml_magnetic_field_update_inplace_contract(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(0, 0, 0),
        seed=12015,
    )

    magnetic_field = args[0]
    magnetic_before = magnetic_field.clone()

    out_of_place = PMLMagneticFieldUpdate.dispatch(
        *args,
        implementation="torch",
        **kwargs,
    )
    assert out_of_place.data_ptr() != magnetic_field.data_ptr()
    torch.testing.assert_close(magnetic_field, magnetic_before)

    inplace_kwargs = dict(kwargs)
    inplace_kwargs["inplace"] = True
    in_place = PMLMagneticFieldUpdate.dispatch(
        *args,
        implementation="torch",
        **inplace_kwargs,
    )
    assert in_place.data_ptr() == magnetic_field.data_ptr()
