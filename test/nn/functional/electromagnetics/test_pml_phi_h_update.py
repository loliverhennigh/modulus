# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import torch

from physicsnemo.nn.functional import pml_phi_h_update
from physicsnemo.nn.functional.electromagnetics import PMLPhiHUpdate
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


# Build deterministic PML phi_h update inputs.
def _build_case(
    device: str,
    pml_shape: tuple[int, int, int],
    offset: tuple[int, int, int],
    seed: int = 10011,
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
    pml_layer[15:24] = torch.randn(
        9,
        pml_shape[0],
        pml_shape[1],
        pml_shape[2],
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    )
    pml_layer[30:36] = torch.empty(
        6,
        pml_shape[0],
        pml_shape[1],
        pml_shape[2],
        device=torch_device,
        dtype=torch.float32,
    ).uniform_(0.05, 0.95, generator=generator)

    args = (electric_field, pml_layer)
    kwargs = {
        "pml_layer_offset": offset,
        "inplace": False,
    }
    return args, kwargs


def test_pml_phi_h_update_torch(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(0, 0, 0),
        seed=10011,
    )
    output = pml_phi_h_update(*args, implementation="torch", **kwargs)
    reference = PMLPhiHUpdate.dispatch(*args, implementation="torch", **kwargs)
    PMLPhiHUpdate.compare_forward(output, reference)


@requires_module("warp")
def test_pml_phi_h_update_warp(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        seed=10012,
    )
    output = pml_phi_h_update(*args, implementation="warp", **kwargs)
    assert output.shape == args[1].shape


def test_pml_phi_h_update_make_inputs_forward(device: str):
    label, args, kwargs = next(iter(PMLPhiHUpdate.make_inputs_forward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    output = PMLPhiHUpdate.dispatch(*args, implementation="torch", **kwargs)
    assert output.shape[0] == 36


def test_pml_phi_h_update_make_inputs_backward(device: str):
    label, args, kwargs = next(iter(PMLPhiHUpdate.make_inputs_backward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    electric_field, pml_layer = args
    assert electric_field.requires_grad
    assert pml_layer.requires_grad

    output = PMLPhiHUpdate.dispatch(*args, implementation="torch", **kwargs)
    output.sum().backward()
    assert electric_field.grad is not None
    assert pml_layer.grad is not None


@requires_module("warp")
def test_pml_phi_h_update_backend_forward_parity(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        seed=10013,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = PMLPhiHUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLPhiHUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLPhiHUpdate.compare_forward(out_warp, out_torch)


@requires_module("warp")
def test_pml_phi_h_update_backend_backward_parity(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        seed=10013,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    electric_torch = args_torch[0].detach().requires_grad_(True)
    pml_torch = args_torch[1].detach().requires_grad_(True)

    electric_warp = args_warp[0].detach().requires_grad_(True)
    pml_warp = args_warp[1].detach().requires_grad_(True)

    args_torch = (electric_torch, pml_torch)
    args_warp = (electric_warp, pml_warp)

    out_torch = PMLPhiHUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLPhiHUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLPhiHUpdate.compare_forward(out_warp, out_torch)

    grad_output = torch.randn_like(out_torch)
    out_torch.backward(grad_output)
    out_warp.backward(grad_output)

    assert electric_warp.grad is not None
    assert electric_torch.grad is not None
    PMLPhiHUpdate.compare_backward(electric_warp.grad, electric_torch.grad)

    assert pml_warp.grad is not None
    assert pml_torch.grad is not None
    PMLPhiHUpdate.compare_backward(pml_warp.grad, pml_torch.grad)


def test_pml_phi_h_update_compare_forward_contract(device: str):
    _, args, kwargs = next(iter(PMLPhiHUpdate.make_inputs_forward(device)))
    output = PMLPhiHUpdate.dispatch(*args, implementation="torch", **kwargs)
    reference = output.detach().clone()
    PMLPhiHUpdate.compare_forward(output, reference)


def test_pml_phi_h_update_compare_backward_contract(device: str):
    _, args, kwargs = next(iter(PMLPhiHUpdate.make_inputs_backward(device)))
    electric_field, pml_layer = args

    output = PMLPhiHUpdate.dispatch(*args, implementation="torch", **kwargs)
    output.sum().backward()

    assert electric_field.grad is not None
    assert pml_layer.grad is not None
    PMLPhiHUpdate.compare_backward(
        electric_field.grad, electric_field.grad.detach().clone()
    )
    PMLPhiHUpdate.compare_backward(pml_layer.grad, pml_layer.grad.detach().clone())


def test_pml_phi_h_update_error_handling(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(0, 0, 0),
        seed=10014,
    )

    with pytest.raises(ValueError, match="pml_layer_offset must have 3 elements"):
        PMLPhiHUpdate.dispatch(
            args[0],
            args[1],
            pml_layer_offset=(0, 0),
            implementation="torch",
            **{k: v for k, v in kwargs.items() if k != "pml_layer_offset"},
        )


def test_pml_phi_h_update_inplace_contract(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(0, 0, 0),
        seed=10015,
    )

    electric_field, pml_layer = args
    pml_before = pml_layer.clone()

    out_of_place = PMLPhiHUpdate.dispatch(
        electric_field,
        pml_layer,
        implementation="torch",
        **kwargs,
    )
    assert out_of_place.data_ptr() != pml_layer.data_ptr()
    torch.testing.assert_close(pml_layer, pml_before)

    inplace_kwargs = dict(kwargs)
    inplace_kwargs["inplace"] = True
    in_place = PMLPhiHUpdate.dispatch(
        electric_field,
        pml_layer,
        implementation="torch",
        **inplace_kwargs,
    )
    assert in_place.data_ptr() == pml_layer.data_ptr()
