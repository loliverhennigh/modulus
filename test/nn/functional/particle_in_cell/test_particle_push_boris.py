# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import torch

from physicsnemo.nn.functional import particle_push_boris
from physicsnemo.nn.functional.particle_in_cell import ParticlePushBoris
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


def _build_case(
    device: str,
    num_particles: int = 4096,
    seed: int = 8011,
) -> tuple[tuple, dict]:
    torch_device = torch.device(device)
    generator = torch.Generator(device=torch_device)
    generator.manual_seed(seed)

    particle_position = torch.randn(
        num_particles,
        3,
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    )
    particle_momentum = torch.randn(
        num_particles,
        3,
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    )
    electric_field = torch.randn(
        num_particles,
        3,
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    )
    magnetic_field = torch.randn(
        num_particles,
        3,
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    )

    args = (
        particle_position,
        particle_momentum,
        electric_field,
        magnetic_field,
        -1.0,
        5.0e-4,
    )
    kwargs = {"inplace": False}
    return args, kwargs


def test_particle_push_boris_torch(device: str):
    args, kwargs = _build_case(device=device, num_particles=2048, seed=8031)
    output = particle_push_boris(*args, implementation="torch", **kwargs)
    reference = ParticlePushBoris.dispatch(*args, implementation="torch", **kwargs)
    ParticlePushBoris.compare_forward(output, reference)


@requires_module("warp")
def test_particle_push_boris_warp(device: str):
    args, kwargs = _build_case(device=device, num_particles=2048, seed=8032)
    position_out, momentum_out = particle_push_boris(
        *args,
        implementation="warp",
        **kwargs,
    )
    assert position_out.shape == args[0].shape
    assert momentum_out.shape == args[1].shape


def test_particle_push_boris_make_inputs_forward(device: str):
    label, args, kwargs = next(iter(ParticlePushBoris.make_inputs_forward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    output = ParticlePushBoris.dispatch(*args, implementation="torch", **kwargs)
    assert isinstance(output, tuple)
    assert output[0].shape[1] == 3
    assert output[1].shape[1] == 3


def test_particle_push_boris_make_inputs_backward(device: str):
    label, args, kwargs = next(iter(ParticlePushBoris.make_inputs_backward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    position, momentum, electric, magnetic, _, _ = args
    assert position.requires_grad
    assert momentum.requires_grad
    assert electric.requires_grad
    assert magnetic.requires_grad

    position_out, momentum_out = ParticlePushBoris.dispatch(
        *args,
        implementation="torch",
        **kwargs,
    )
    (position_out.sum() + momentum_out.sum()).backward()
    assert position.grad is not None
    assert momentum.grad is not None
    assert electric.grad is not None
    assert magnetic.grad is not None


def test_particle_push_boris_compare_forward_contract(device: str):
    args, kwargs = _build_case(device=device, num_particles=256, seed=8046)
    output = ParticlePushBoris.dispatch(*args, implementation="torch", **kwargs)
    reference = (output[0].clone(), output[1].clone())
    ParticlePushBoris.compare_forward(output, reference)


def test_particle_push_boris_compare_backward_contract(device: str):
    grad = torch.randn(32, 3, device=device, dtype=torch.float32)
    ParticlePushBoris.compare_backward(grad, grad.clone())


@requires_module("warp")
def test_particle_push_boris_backend_forward_parity(device: str):
    args_torch, kwargs_torch = _build_case(device=device, num_particles=4096, seed=8041)
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = ParticlePushBoris.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = ParticlePushBoris.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    ParticlePushBoris.compare_forward(out_warp, out_torch)


@requires_module("warp")
def test_particle_push_boris_backend_backward_parity(device: str):
    args_torch, kwargs_torch = _build_case(device=device, num_particles=4096, seed=8043)
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    pos_torch = args_torch[0].detach().requires_grad_(True)
    momentum_torch = args_torch[1].detach().requires_grad_(True)
    e_torch = args_torch[2].detach().requires_grad_(True)
    b_torch = args_torch[3].detach().requires_grad_(True)

    pos_warp = args_warp[0].detach().requires_grad_(True)
    momentum_warp = args_warp[1].detach().requires_grad_(True)
    e_warp = args_warp[2].detach().requires_grad_(True)
    b_warp = args_warp[3].detach().requires_grad_(True)

    args_torch = (pos_torch, momentum_torch, e_torch, b_torch, args_torch[4], args_torch[5])
    args_warp = (pos_warp, momentum_warp, e_warp, b_warp, args_warp[4], args_warp[5])

    pos_out_torch, momentum_out_torch = ParticlePushBoris.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    pos_out_warp, momentum_out_warp = ParticlePushBoris.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    ParticlePushBoris.compare_forward(
        (pos_out_warp, momentum_out_warp),
        (pos_out_torch, momentum_out_torch),
    )

    grad_pos = torch.randn_like(pos_out_torch)
    grad_momentum = torch.randn_like(momentum_out_torch)

    torch.autograd.backward((pos_out_torch, momentum_out_torch), (grad_pos, grad_momentum))
    torch.autograd.backward((pos_out_warp, momentum_out_warp), (grad_pos, grad_momentum))

    assert pos_torch.grad is not None and pos_warp.grad is not None
    assert momentum_torch.grad is not None and momentum_warp.grad is not None
    assert e_torch.grad is not None and e_warp.grad is not None
    assert b_torch.grad is not None and b_warp.grad is not None

    ParticlePushBoris.compare_backward(pos_warp.grad, pos_torch.grad)
    ParticlePushBoris.compare_backward(momentum_warp.grad, momentum_torch.grad)
    ParticlePushBoris.compare_backward(e_warp.grad, e_torch.grad)
    ParticlePushBoris.compare_backward(b_warp.grad, b_torch.grad)


def test_particle_push_boris_error_handling(device: str):
    args, kwargs = _build_case(device=device, num_particles=128, seed=8042)
    bad_electric = args[2][..., :2]

    with pytest.raises(ValueError, match="electric_field must have shape"):
        particle_push_boris(
            args[0],
            args[1],
            bad_electric,
            args[3],
            args[4],
            args[5],
            implementation="torch",
            **kwargs,
        )


def test_particle_push_boris_inplace_with_autograd_raises(device: str):
    args, _ = _build_case(device=device, num_particles=128, seed=8044)
    position = args[0].detach().requires_grad_(True)
    momentum = args[1].detach().requires_grad_(True)
    electric = args[2].detach().requires_grad_(True)
    magnetic = args[3].detach().requires_grad_(True)

    with pytest.raises(ValueError, match="inplace=True is not supported"):
        particle_push_boris(
            position,
            momentum,
            electric,
            magnetic,
            args[4],
            args[5],
            implementation="torch",
            inplace=True,
        )


@requires_module("warp")
@pytest.mark.parametrize("grad_input_index", [0, 1, 2, 3])
def test_particle_push_boris_backend_backward_partial_input_grads(
    device: str, grad_input_index: int
):
    args, kwargs = _build_case(device=device, num_particles=256, seed=8045)
    inputs = [args[0].detach(), args[1].detach(), args[2].detach(), args[3].detach()]
    inputs[grad_input_index] = inputs[grad_input_index].requires_grad_(True)

    position, momentum, electric, magnetic = inputs

    position_out, momentum_out = ParticlePushBoris.dispatch(
        position,
        momentum,
        electric,
        magnetic,
        args[4],
        args[5],
        implementation="warp",
        **kwargs,
    )
    loss = position_out.sum() + momentum_out.sum()
    loss.backward()

    for input_index, input_tensor in enumerate(inputs):
        if input_index == grad_input_index:
            assert input_tensor.grad is not None
        else:
            assert input_tensor.grad is None


@requires_module("warp")
def test_particle_push_boris_backend_backward_single_output_grad(device: str):
    args, kwargs = _build_case(device=device, num_particles=256, seed=8047)
    position = args[0].detach().requires_grad_(True)
    momentum = args[1].detach().requires_grad_(True)
    electric = args[2].detach().requires_grad_(True)
    magnetic = args[3].detach().requires_grad_(True)

    position_out, _ = ParticlePushBoris.dispatch(
        position,
        momentum,
        electric,
        magnetic,
        args[4],
        args[5],
        implementation="warp",
        **kwargs,
    )
    loss = position_out.sum()
    loss.backward()

    assert position.grad is not None
    assert momentum.grad is not None
    assert electric.grad is not None
    assert magnetic.grad is not None
