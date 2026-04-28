# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import torch

from physicsnemo.nn.functional import pml_phi_e_update
from physicsnemo.nn.functional.electromagnetics import PMLPhiEUpdate
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


# Build deterministic PML phi_e update inputs.
def _build_case(
    device: str,
    pml_shape: tuple[int, int, int],
    offset: tuple[int, int, int],
    seed: int = 9011,
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
    pml_layer[6:15] = torch.randn(
        9,
        pml_shape[0],
        pml_shape[1],
        pml_shape[2],
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    )
    pml_layer[24:30] = torch.empty(
        6,
        pml_shape[0],
        pml_shape[1],
        pml_shape[2],
        device=torch_device,
        dtype=torch.float32,
    ).uniform_(0.05, 0.95, generator=generator)

    args = (magnetic_field, pml_layer)
    kwargs = {
        "pml_layer_offset": offset,
        "inplace": False,
    }
    return args, kwargs


def test_pml_phi_e_update_torch(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(0, 0, 0),
        seed=9011,
    )
    output = pml_phi_e_update(*args, implementation="torch", **kwargs)
    reference = PMLPhiEUpdate.dispatch(*args, implementation="torch", **kwargs)
    PMLPhiEUpdate.compare_forward(output, reference)


@requires_module("warp")
def test_pml_phi_e_update_warp(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        seed=9012,
    )
    output = pml_phi_e_update(*args, implementation="warp", **kwargs)
    assert output.shape == args[1].shape


def test_pml_phi_e_update_make_inputs_forward(device: str):
    label, args, kwargs = next(iter(PMLPhiEUpdate.make_inputs_forward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    output = PMLPhiEUpdate.dispatch(*args, implementation="torch", **kwargs)
    assert output.shape[0] == 36


def test_pml_phi_e_update_make_inputs_backward(device: str):
    label, args, kwargs = next(iter(PMLPhiEUpdate.make_inputs_backward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    magnetic_field, pml_layer = args
    assert magnetic_field.requires_grad
    assert pml_layer.requires_grad

    output = PMLPhiEUpdate.dispatch(*args, implementation="torch", **kwargs)
    output.sum().backward()
    assert magnetic_field.grad is not None
    assert pml_layer.grad is not None


@requires_module("warp")
def test_pml_phi_e_update_backend_forward_parity(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        seed=9013,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = PMLPhiEUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLPhiEUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLPhiEUpdate.compare_forward(out_warp, out_torch)


@requires_module("warp")
def test_pml_phi_e_update_backend_backward_parity(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(10, 0, 0),
        seed=9013,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    magnetic_torch = args_torch[0].detach().requires_grad_(True)
    pml_torch = args_torch[1].detach().requires_grad_(True)

    magnetic_warp = args_warp[0].detach().requires_grad_(True)
    pml_warp = args_warp[1].detach().requires_grad_(True)

    args_torch = (magnetic_torch, pml_torch)
    args_warp = (magnetic_warp, pml_warp)

    out_torch = PMLPhiEUpdate.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLPhiEUpdate.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLPhiEUpdate.compare_forward(out_warp, out_torch)

    grad_output = torch.randn_like(out_torch)
    out_torch.backward(grad_output)
    out_warp.backward(grad_output)

    assert magnetic_warp.grad is not None
    assert magnetic_torch.grad is not None
    PMLPhiEUpdate.compare_backward(magnetic_warp.grad, magnetic_torch.grad)

    assert pml_warp.grad is not None
    assert pml_torch.grad is not None
    PMLPhiEUpdate.compare_backward(pml_warp.grad, pml_torch.grad)


def test_pml_phi_e_update_compare_forward_contract(device: str):
    _, args, kwargs = next(iter(PMLPhiEUpdate.make_inputs_forward(device)))
    output = PMLPhiEUpdate.dispatch(*args, implementation="torch", **kwargs)
    reference = output.detach().clone()
    PMLPhiEUpdate.compare_forward(output, reference)


def test_pml_phi_e_update_compare_backward_contract(device: str):
    _, args, kwargs = next(iter(PMLPhiEUpdate.make_inputs_backward(device)))
    magnetic_field, pml_layer = args

    output = PMLPhiEUpdate.dispatch(*args, implementation="torch", **kwargs)
    output.sum().backward()

    assert magnetic_field.grad is not None
    assert pml_layer.grad is not None
    PMLPhiEUpdate.compare_backward(
        magnetic_field.grad, magnetic_field.grad.detach().clone()
    )
    PMLPhiEUpdate.compare_backward(pml_layer.grad, pml_layer.grad.detach().clone())


def test_pml_phi_e_update_error_handling(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(0, 0, 0),
        seed=9014,
    )

    with pytest.raises(ValueError, match="pml_layer_offset must have 3 elements"):
        PMLPhiEUpdate.dispatch(
            args[0],
            args[1],
            pml_layer_offset=(0, 0),
            implementation="torch",
            **{k: v for k, v in kwargs.items() if k != "pml_layer_offset"},
        )


def test_pml_phi_e_update_inplace_contract(device: str):
    args, kwargs = _build_case(
        device=device,
        pml_shape=(6, 12, 10),
        offset=(0, 0, 0),
        seed=9015,
    )

    magnetic_field, pml_layer = args
    pml_before = pml_layer.clone()

    out_of_place = PMLPhiEUpdate.dispatch(
        magnetic_field,
        pml_layer,
        implementation="torch",
        **kwargs,
    )
    assert out_of_place.data_ptr() != pml_layer.data_ptr()
    torch.testing.assert_close(pml_layer, pml_before)

    inplace_kwargs = dict(kwargs)
    inplace_kwargs["inplace"] = True
    in_place = PMLPhiEUpdate.dispatch(
        magnetic_field,
        pml_layer,
        implementation="torch",
        **inplace_kwargs,
    )
    assert in_place.data_ptr() == pml_layer.data_ptr()
