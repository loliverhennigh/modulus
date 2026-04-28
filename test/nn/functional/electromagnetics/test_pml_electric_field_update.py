# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import torch

from physicsnemo.nn.functional import pml_electric_field_update
from physicsnemo.nn.functional.electromagnetics import PMLElectricFieldUpdate
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


# Build deterministic PML electric-field correction inputs.
def _build_case(
    device: str,
    pml_shape: tuple[int, int, int],
    offset: tuple[int, int, int],
    eps_mode: str = "field",
    seed: int = 11011,
):
    torch_device = torch.device(device)
    generator = torch.Generator(device=torch_device)
    generator.manual_seed(seed)

    electric_field = torch.randn(
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
    pml_layer[0:3] = torch.randn(
        3,
        pml_shape[0],
        pml_shape[1],
        pml_shape[2],
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    )

    if eps_mode == "field":
        eps: float | torch.Tensor = torch.empty(
            16,
            16,
            16,
            device=torch_device,
            dtype=torch.float32,
        ).uniform_(1.0, 6.0, generator=generator)
    elif eps_mode == "scalar":
        eps = 3.2
    else:
        raise ValueError(f"unsupported eps_mode: {eps_mode}")
    spacing = torch.tensor([0.01, 0.012, 0.014], device=torch_device)

    args = (
        electric_field,
        pml_layer,
        eps,
        spacing,
        offset,
        0.00125,
    )
    kwargs = {"inplace": False}
    return args, kwargs


def test_pml_electric_field_update_torch(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(0, 0, 0),
        seed=11011,
    )
    output = pml_electric_field_update(*args, implementation="torch", **kwargs)
    reference = PMLElectricFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    PMLElectricFieldUpdate.compare_forward(output, reference)


@requires_module("warp")
def test_pml_electric_field_update_warp(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        seed=11012,
    )
    output = pml_electric_field_update(*args, implementation="warp", **kwargs)
    assert output.shape == args[0].shape


def test_pml_electric_field_update_make_inputs_forward(device: str):
    label, args, kwargs = next(iter(PMLElectricFieldUpdate.make_inputs_forward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    output = PMLElectricFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    assert output.shape[0] == 3


def test_pml_electric_field_update_make_inputs_backward(device: str):
    label, args, kwargs = next(iter(PMLElectricFieldUpdate.make_inputs_backward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    electric_field, pml_layer, eps, _, _, _ = args
    assert electric_field.requires_grad
    assert pml_layer.requires_grad
    assert eps.requires_grad

    output = PMLElectricFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    output.sum().backward()
    assert electric_field.grad is not None
    assert pml_layer.grad is not None
    assert eps.grad is not None


@requires_module("warp")
def test_pml_electric_field_update_backend_forward_parity(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        seed=11013,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = PMLElectricFieldUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLElectricFieldUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLElectricFieldUpdate.compare_forward(out_warp, out_torch)


@requires_module("warp")
def test_pml_electric_field_update_backend_backward_parity(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        seed=11013,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    electric_torch = args_torch[0].detach().requires_grad_(True)
    pml_torch = args_torch[1].detach().requires_grad_(True)
    eps_torch = args_torch[2].detach().requires_grad_(True)

    electric_warp = args_warp[0].detach().requires_grad_(True)
    pml_warp = args_warp[1].detach().requires_grad_(True)
    eps_warp = args_warp[2].detach().requires_grad_(True)

    args_torch = (
        electric_torch,
        pml_torch,
        eps_torch,
        args_torch[3],
        args_torch[4],
        args_torch[5],
    )
    args_warp = (
        electric_warp,
        pml_warp,
        eps_warp,
        args_warp[3],
        args_warp[4],
        args_warp[5],
    )

    out_torch = PMLElectricFieldUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLElectricFieldUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLElectricFieldUpdate.compare_forward(out_warp, out_torch)

    grad_output = torch.randn_like(out_torch)
    out_torch.backward(grad_output)
    out_warp.backward(grad_output)

    assert electric_warp.grad is not None
    assert electric_torch.grad is not None
    PMLElectricFieldUpdate.compare_backward(electric_warp.grad, electric_torch.grad)

    assert pml_warp.grad is not None
    assert pml_torch.grad is not None
    PMLElectricFieldUpdate.compare_backward(pml_warp.grad, pml_torch.grad)

    assert eps_warp.grad is not None
    assert eps_torch.grad is not None
    PMLElectricFieldUpdate.compare_backward(eps_warp.grad, eps_torch.grad)


@requires_module("warp")
def test_pml_electric_field_update_backend_forward_parity_scalar_eps(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        eps_mode="scalar",
        seed=11016,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = PMLElectricFieldUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLElectricFieldUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLElectricFieldUpdate.compare_forward(out_warp, out_torch)


@requires_module("warp")
def test_pml_electric_field_update_backend_backward_parity_scalar_eps(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        eps_mode="scalar",
        seed=11017,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    electric_torch = args_torch[0].detach().requires_grad_(True)
    pml_torch = args_torch[1].detach().requires_grad_(True)

    electric_warp = args_warp[0].detach().requires_grad_(True)
    pml_warp = args_warp[1].detach().requires_grad_(True)

    args_torch = (
        electric_torch,
        pml_torch,
        args_torch[2],
        args_torch[3],
        args_torch[4],
        args_torch[5],
    )
    args_warp = (
        electric_warp,
        pml_warp,
        args_warp[2],
        args_warp[3],
        args_warp[4],
        args_warp[5],
    )

    out_torch = PMLElectricFieldUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLElectricFieldUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLElectricFieldUpdate.compare_forward(out_warp, out_torch)

    grad_output = torch.randn_like(out_torch)
    out_torch.backward(grad_output)
    out_warp.backward(grad_output)

    assert electric_warp.grad is not None
    assert electric_torch.grad is not None
    PMLElectricFieldUpdate.compare_backward(electric_warp.grad, electric_torch.grad)

    assert pml_warp.grad is not None
    assert pml_torch.grad is not None
    PMLElectricFieldUpdate.compare_backward(pml_warp.grad, pml_torch.grad)


def test_pml_electric_field_update_compare_forward_contract(device: str):
    _, args, kwargs = next(iter(PMLElectricFieldUpdate.make_inputs_forward(device)))
    output = PMLElectricFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    reference = output.detach().clone()
    PMLElectricFieldUpdate.compare_forward(output, reference)


def test_pml_electric_field_update_compare_backward_contract(device: str):
    _, args, kwargs = next(iter(PMLElectricFieldUpdate.make_inputs_backward(device)))
    electric_field, pml_layer, eps, _, _, _ = args

    output = PMLElectricFieldUpdate.dispatch(*args, implementation="torch", **kwargs)
    output.sum().backward()

    assert electric_field.grad is not None
    assert pml_layer.grad is not None
    assert eps.grad is not None
    PMLElectricFieldUpdate.compare_backward(
        electric_field.grad, electric_field.grad.detach().clone()
    )
    PMLElectricFieldUpdate.compare_backward(
        pml_layer.grad, pml_layer.grad.detach().clone()
    )
    PMLElectricFieldUpdate.compare_backward(eps.grad, eps.grad.detach().clone())


def test_pml_electric_field_update_error_handling(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(0, 0, 0),
        seed=11014,
    )
    bad_eps = args[2].to(torch.float64)
    with pytest.raises(TypeError, match="eps tensor must be float32"):
        PMLElectricFieldUpdate.dispatch(
            args[0],
            args[1],
            bad_eps,
            args[3],
            args[4],
            args[5],
            implementation="torch",
            **kwargs,
        )


def test_pml_electric_field_update_inplace_contract(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(0, 0, 0),
        seed=11015,
    )

    electric_field = args[0]
    electric_before = electric_field.clone()

    out_of_place = PMLElectricFieldUpdate.dispatch(
        *args,
        implementation="torch",
        **kwargs,
    )
    assert out_of_place.data_ptr() != electric_field.data_ptr()
    torch.testing.assert_close(electric_field, electric_before)

    inplace_kwargs = dict(kwargs)
    inplace_kwargs["inplace"] = True
    in_place = PMLElectricFieldUpdate.dispatch(
        *args,
        implementation="torch",
        **inplace_kwargs,
    )
    assert in_place.data_ptr() == electric_field.data_ptr()
