# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import torch

from physicsnemo.nn.functional import gather_fields_to_particles
from physicsnemo.nn.functional.particle_in_cell import GatherFieldsToParticles
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


def _build_case(
    device: str,
    num_particles: int = 4096,
    grid_n: int = 32,
    seed: int = 8111,
    shape_order: int = 1,
    gather_mode: str = "momentum-conserving",
) -> tuple[tuple, dict]:
    torch_device = torch.device(device)
    generator = torch.Generator(device=torch_device)
    generator.manual_seed(seed)

    origin = (-0.75, 0.20, 1.10)
    spacing = (0.05, 0.07, 0.09)

    extent = torch.tensor(
        [
            spacing[0] * float(grid_n - 1),
            spacing[1] * float(grid_n - 1),
            spacing[2] * float(grid_n - 1),
        ],
        device=torch_device,
        dtype=torch.float32,
    )
    origin_tensor = torch.tensor(origin, device=torch_device, dtype=torch.float32)

    particle_position = origin_tensor.unsqueeze(0) + torch.rand(
        num_particles,
        3,
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    ) * extent.unsqueeze(0)

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

    args = (
        particle_position,
        electric_field,
        magnetic_field,
        origin,
        spacing,
        None,
        None,
        True,
        shape_order,
        gather_mode,
    )
    kwargs = {}
    return args, kwargs


def test_gather_fields_to_particles_torch(device: str):
    args, kwargs = _build_case(device=device, num_particles=2048, grid_n=24, seed=8121)
    output = gather_fields_to_particles(*args, implementation="torch", **kwargs)
    reference = GatherFieldsToParticles.dispatch(*args, implementation="torch", **kwargs)
    GatherFieldsToParticles.compare_forward(output, reference)


@pytest.mark.parametrize(
    ("shape_order", "gather_mode"),
    [(3, "momentum-conserving"), (3, "energy-conserving")],
)
def test_gather_fields_to_particles_torch_order3_modes(
    device: str,
    shape_order: int,
    gather_mode: str,
):
    args, kwargs = _build_case(
        device=device,
        num_particles=1024,
        grid_n=20,
        seed=8120,
        shape_order=shape_order,
        gather_mode=gather_mode,
    )
    electric_particle, magnetic_particle = gather_fields_to_particles(
        *args,
        implementation="torch",
        **kwargs,
    )
    assert electric_particle.shape == args[0].shape
    assert magnetic_particle.shape == args[0].shape


@requires_module("warp")
def test_gather_fields_to_particles_warp(device: str):
    args, kwargs = _build_case(device=device, num_particles=2048, grid_n=24, seed=8122)
    electric_particle, magnetic_particle = gather_fields_to_particles(
        *args,
        implementation="warp",
        **kwargs,
    )
    assert electric_particle.shape == args[0].shape
    assert magnetic_particle.shape == args[0].shape


def test_gather_fields_to_particles_make_inputs_forward(device: str):
    label, args, kwargs = next(iter(GatherFieldsToParticles.make_inputs_forward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    output = GatherFieldsToParticles.dispatch(*args, implementation="torch", **kwargs)
    assert isinstance(output, tuple)
    assert output[0].shape[1] == 3
    assert output[1].shape[1] == 3


def test_gather_fields_to_particles_make_inputs_backward(device: str):
    label, args, kwargs = next(iter(GatherFieldsToParticles.make_inputs_backward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    particle_position, electric_field, magnetic_field, *_ = args
    assert particle_position.requires_grad
    assert electric_field.requires_grad
    assert magnetic_field.requires_grad

    electric_particle, magnetic_particle = GatherFieldsToParticles.dispatch(
        *args,
        implementation="torch",
        **kwargs,
    )
    (electric_particle.sum() + magnetic_particle.sum()).backward()
    assert particle_position.grad is not None
    assert electric_field.grad is not None
    assert magnetic_field.grad is not None


def test_gather_fields_to_particles_compare_forward_contract(device: str):
    args, kwargs = _build_case(device=device, num_particles=256, grid_n=20, seed=8126)
    output = GatherFieldsToParticles.dispatch(*args, implementation="torch", **kwargs)
    reference = (output[0].clone(), output[1].clone())
    GatherFieldsToParticles.compare_forward(output, reference)


def test_gather_fields_to_particles_compare_backward_contract(device: str):
    grad = torch.randn(64, 3, device=device, dtype=torch.float32)
    GatherFieldsToParticles.compare_backward(grad, grad.clone())


@requires_module("warp")
def test_gather_fields_to_particles_backend_forward_parity(device: str):
    args_torch, kwargs_torch = _build_case(device=device, num_particles=4096, grid_n=32, seed=8123)
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = GatherFieldsToParticles.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = GatherFieldsToParticles.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    GatherFieldsToParticles.compare_forward(out_warp, out_torch)


@requires_module("warp")
@pytest.mark.parametrize(
    ("shape_order", "gather_mode"),
    [
        (1, "energy-conserving"),
        (3, "momentum-conserving"),
        (3, "energy-conserving"),
    ],
)
def test_gather_fields_to_particles_backend_forward_parity_modes(
    device: str,
    shape_order: int,
    gather_mode: str,
):
    args_torch, kwargs_torch = _build_case(
        device=device,
        num_particles=2048,
        grid_n=24,
        seed=8131,
        shape_order=shape_order,
        gather_mode=gather_mode,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = GatherFieldsToParticles.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = GatherFieldsToParticles.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    GatherFieldsToParticles.compare_forward(out_warp, out_torch)


@requires_module("warp")
@pytest.mark.parametrize(
    ("shape_order", "gather_mode"),
    [
        (1, "momentum-conserving"),
        (3, "energy-conserving"),
    ],
)
def test_gather_fields_to_particles_backend_forward_parity_far_periodic_positions(
    device: str,
    shape_order: int,
    gather_mode: str,
):
    args_torch, kwargs_torch = _build_case(
        device=device,
        num_particles=2048,
        grid_n=24,
        seed=8132,
        shape_order=shape_order,
        gather_mode=gather_mode,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    particle_position_torch = args_torch[0]
    particle_position_warp = args_warp[0]
    spacing = torch.tensor(args_torch[4], device=particle_position_torch.device, dtype=torch.float32)
    grid_n = int(args_torch[1].shape[1])
    domain_extent = spacing * float(grid_n)
    generator = torch.Generator(device=particle_position_torch.device)
    generator.manual_seed(8133)
    integer_shifts = torch.randint(
        low=-4,
        high=5,
        size=(particle_position_torch.shape[0], 3),
        generator=generator,
        device=particle_position_torch.device,
        dtype=torch.int32,
    ).to(torch.float32)
    offset = integer_shifts * domain_extent.unsqueeze(0)
    particle_position_torch = particle_position_torch + offset
    particle_position_warp = particle_position_warp + offset

    args_torch = (
        particle_position_torch,
        args_torch[1],
        args_torch[2],
        args_torch[3],
        args_torch[4],
        args_torch[5],
        args_torch[6],
        args_torch[7],
        args_torch[8],
        args_torch[9],
    )
    args_warp = (
        particle_position_warp,
        args_warp[1],
        args_warp[2],
        args_warp[3],
        args_warp[4],
        args_warp[5],
        args_warp[6],
        args_warp[7],
        args_warp[8],
        args_warp[9],
    )

    out_torch = GatherFieldsToParticles.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = GatherFieldsToParticles.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    GatherFieldsToParticles.compare_forward(out_warp, out_torch)


def test_gather_fields_to_particles_nonperiodic_rejected(device: str):
    args, kwargs = _build_case(device=device, num_particles=256, grid_n=20, seed=8129)
    args_nonperiodic = (*args[:7], False, *args[8:])

    with pytest.raises(ValueError, match="supports periodic=True only"):
        GatherFieldsToParticles.dispatch(
            *args_nonperiodic,
            implementation="torch",
            **kwargs,
        )

    with pytest.raises(ValueError, match="supports periodic=True only"):
        gather_fields_to_particles(
            *args_nonperiodic,
            implementation="torch",
            **kwargs,
        )


@requires_module("warp")
def test_gather_fields_to_particles_backend_forward_parity_custom_stagger(device: str):
    args_torch, kwargs_torch = _build_case(device=device, num_particles=2048, grid_n=28, seed=8130)
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    custom_electric_stagger = (
        (0.0, 0.0, 0.0),
        (0.5, 0.25, 0.0),
        (0.0, 0.5, 0.25),
    )
    custom_magnetic_stagger = (
        (0.5, 0.0, 0.5),
        (0.25, 0.5, 0.0),
        (0.0, 0.25, 0.5),
    )

    args_torch = (
        args_torch[0],
        args_torch[1],
        args_torch[2],
        args_torch[3],
        args_torch[4],
        custom_electric_stagger,
        custom_magnetic_stagger,
        args_torch[7],
        args_torch[8],
        args_torch[9],
    )
    args_warp = (
        args_warp[0],
        args_warp[1],
        args_warp[2],
        args_warp[3],
        args_warp[4],
        custom_electric_stagger,
        custom_magnetic_stagger,
        args_warp[7],
        args_warp[8],
        args_warp[9],
    )

    out_torch = GatherFieldsToParticles.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = GatherFieldsToParticles.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    GatherFieldsToParticles.compare_forward(out_warp, out_torch)


@requires_module("warp")
@pytest.mark.parametrize(
    ("shape_order", "gather_mode"),
    [
        (1, "momentum-conserving"),
        (3, "momentum-conserving"),
        (3, "energy-conserving"),
    ],
)
def test_gather_fields_to_particles_backend_backward_parity(
    device: str,
    shape_order: int,
    gather_mode: str,
):
    args_torch, kwargs_torch = _build_case(
        device=device,
        num_particles=1024,
        grid_n=24,
        seed=8124,
        shape_order=shape_order,
        gather_mode=gather_mode,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    pos_torch = args_torch[0].detach().requires_grad_(True)
    e_torch = args_torch[1].detach().requires_grad_(True)
    b_torch = args_torch[2].detach().requires_grad_(True)

    pos_warp = args_warp[0].detach().requires_grad_(True)
    e_warp = args_warp[1].detach().requires_grad_(True)
    b_warp = args_warp[2].detach().requires_grad_(True)

    args_torch = (
        pos_torch,
        e_torch,
        b_torch,
        args_torch[3],
        args_torch[4],
        args_torch[5],
        args_torch[6],
        args_torch[7],
        args_torch[8],
        args_torch[9],
    )
    args_warp = (
        pos_warp,
        e_warp,
        b_warp,
        args_warp[3],
        args_warp[4],
        args_warp[5],
        args_warp[6],
        args_warp[7],
        args_warp[8],
        args_warp[9],
    )

    electric_torch, magnetic_torch = GatherFieldsToParticles.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    electric_warp, magnetic_warp = GatherFieldsToParticles.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    GatherFieldsToParticles.compare_forward(
        (electric_warp, magnetic_warp),
        (electric_torch, magnetic_torch),
    )

    grad_electric = torch.randn_like(electric_torch)
    grad_magnetic = torch.randn_like(magnetic_torch)

    torch.autograd.backward((electric_torch, magnetic_torch), (grad_electric, grad_magnetic))
    torch.autograd.backward((electric_warp, magnetic_warp), (grad_electric, grad_magnetic))

    assert pos_torch.grad is not None and pos_warp.grad is not None
    assert e_torch.grad is not None and e_warp.grad is not None
    assert b_torch.grad is not None and b_warp.grad is not None

    GatherFieldsToParticles.compare_backward(pos_warp.grad, pos_torch.grad)
    GatherFieldsToParticles.compare_backward(e_warp.grad, e_torch.grad)
    GatherFieldsToParticles.compare_backward(b_warp.grad, b_torch.grad)


def test_gather_fields_to_particles_error_handling(device: str):
    args, kwargs = _build_case(device=device, num_particles=128, grid_n=16, seed=8125)
    bad_positions = args[0][..., :2]

    with pytest.raises(ValueError, match="particle_position must have shape"):
        gather_fields_to_particles(
            bad_positions,
            args[1],
            args[2],
            args[3],
            args[4],
            args[5],
            args[6],
            args[7],
            args[8],
            args[9],
            implementation="torch",
            **kwargs,
        )

    with pytest.raises(ValueError, match="spacing must be strictly positive"):
        gather_fields_to_particles(
            args[0],
            args[1],
            args[2],
            args[3],
            (-0.1, 0.1, 0.1),
            args[5],
            args[6],
            args[7],
            args[8],
            args[9],
            implementation="torch",
            **kwargs,
        )

    with pytest.raises(ValueError, match="electric_stagger must have shape"):
        gather_fields_to_particles(
            args[0],
            args[1],
            args[2],
            args[3],
            args[4],
            ((0.0, 0.5), (0.5, 0.0)),
            args[6],
            args[7],
            args[8],
            args[9],
            implementation="torch",
            **kwargs,
        )

    with pytest.raises(ValueError, match="shape_order must be one of"):
        gather_fields_to_particles(
            *args[:-2],
            2,
            args[9],
            implementation="torch",
            **kwargs,
        )

    with pytest.raises(TypeError, match="shape_order must be an int"):
        gather_fields_to_particles(
            *args[:-2],
            True,
            args[9],
            implementation="torch",
            **kwargs,
        )

    with pytest.raises(ValueError, match="gather_mode must be one of"):
        gather_fields_to_particles(
            *args[:-1],
            "unknown-mode",
            implementation="torch",
            **kwargs,
        )


@requires_module("warp")
@pytest.mark.parametrize("grad_input_index", [0, 1, 2])
def test_gather_fields_to_particles_backend_backward_partial_input_grads(
    device: str, grad_input_index: int
):
    args, kwargs = _build_case(device=device, num_particles=256, grid_n=20, seed=8127)
    inputs = [args[0].detach(), args[1].detach(), args[2].detach()]
    inputs[grad_input_index] = inputs[grad_input_index].requires_grad_(True)

    electric_particle, magnetic_particle = GatherFieldsToParticles.dispatch(
        inputs[0],
        inputs[1],
        inputs[2],
        args[3],
        args[4],
        args[5],
        args[6],
        args[7],
        args[8],
        args[9],
        implementation="warp",
        **kwargs,
    )
    loss = electric_particle.sum() + magnetic_particle.sum()
    loss.backward()

    for input_index, input_tensor in enumerate(inputs):
        if input_index == grad_input_index:
            assert input_tensor.grad is not None
        else:
            assert input_tensor.grad is None


@requires_module("warp")
def test_gather_fields_to_particles_backend_backward_single_output_grad(device: str):
    args, kwargs = _build_case(device=device, num_particles=256, grid_n=20, seed=8128)
    particle_position = args[0].detach().requires_grad_(True)
    electric_field = args[1].detach().requires_grad_(True)
    magnetic_field = args[2].detach().requires_grad_(True)

    electric_particle, _ = GatherFieldsToParticles.dispatch(
        particle_position,
        electric_field,
        magnetic_field,
        args[3],
        args[4],
        args[5],
        args[6],
        args[7],
        args[8],
        args[9],
        implementation="warp",
        **kwargs,
    )
    loss = electric_particle.sum()
    loss.backward()

    assert particle_position.grad is not None
    assert electric_field.grad is not None
    assert magnetic_field.grad is not None
