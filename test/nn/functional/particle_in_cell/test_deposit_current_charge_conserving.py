# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import torch

from physicsnemo.nn.functional import deposit_current_charge_conserving
from physicsnemo.nn.functional.particle_in_cell import DepositCurrentChargeConserving
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


def _build_case(
    device: str,
    num_particles: int = 4096,
    grid_n: int = 32,
    seed: int = 8211,
    periodic: bool = True,
    include_current_density: bool = False,
    current_stagger: tuple[tuple[float, float, float], ...] | None = None,
    shape_order: int = 1,
) -> tuple[tuple, dict]:
    torch_device = torch.device(device)
    generator = torch.Generator(device=torch_device)
    generator.manual_seed(seed)

    origin = (-0.30, 0.25, 0.40)
    spacing = (0.08, 0.06, 0.07)
    origin_tensor = torch.tensor(origin, device=torch_device, dtype=torch.float32)
    spacing_tensor = torch.tensor(spacing, device=torch_device, dtype=torch.float32)
    extent = spacing_tensor * float(grid_n - 1)

    particle_position_old = origin_tensor.unsqueeze(0) + torch.rand(
        num_particles,
        3,
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    ) * extent.unsqueeze(0)

    displacement = (torch.rand(
        num_particles,
        3,
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    ) - 0.5) * (0.6 * spacing_tensor).unsqueeze(0)
    particle_position_new = particle_position_old + displacement

    if periodic:
        particle_position_new_grid = (
            particle_position_new - origin_tensor.unsqueeze(0)
        ) / spacing_tensor.unsqueeze(0)
        particle_position_new_grid = torch.remainder(
            particle_position_new_grid, float(grid_n)
        )
        particle_position_new = origin_tensor.unsqueeze(0) + (
            particle_position_new_grid * spacing_tensor.unsqueeze(0)
        )
    else:
        domain_min = origin_tensor.unsqueeze(0)
        domain_max = (origin_tensor + extent).unsqueeze(0)
        particle_position_new = particle_position_new.clamp(
            min=domain_min,
            max=domain_max,
        )

    particle_weight = torch.rand(
        num_particles,
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    ).mul_(0.9).add_(0.1)

    current_density = None
    if include_current_density:
        current_density = torch.randn(
            3,
            grid_n,
            grid_n,
            grid_n,
            generator=generator,
            device=torch_device,
            dtype=torch.float32,
        )

    args = (
        particle_position_old,
        particle_position_new,
        particle_weight,
        -1.0,
        1.0e-2,
        (grid_n, grid_n, grid_n),
        origin,
        spacing,
        current_stagger,
        periodic,
        shape_order,
    )
    kwargs = {"current_density": current_density}
    return args, kwargs


def test_deposit_current_charge_conserving_torch(device: str):
    args_ref, kwargs_ref = _build_case(
        device=device,
        num_particles=2048,
        grid_n=24,
        seed=8221,
        periodic=True,
        include_current_density=True,
        shape_order=1,
    )
    args, kwargs = clone_case(args_ref, kwargs_ref)
    args_reference, kwargs_reference = clone_case(args_ref, kwargs_ref)
    output = deposit_current_charge_conserving(
        *args,
        implementation="torch",
        **kwargs,
    )
    reference = DepositCurrentChargeConserving.dispatch(
        *args_reference,
        implementation="torch",
        **kwargs_reference,
    )
    DepositCurrentChargeConserving.compare_forward(output, reference)


@requires_module("warp")
def test_deposit_current_charge_conserving_warp(device: str):
    args, kwargs = _build_case(
        device=device,
        num_particles=2048,
        grid_n=24,
        seed=8222,
        periodic=True,
        include_current_density=True,
        shape_order=1,
    )
    current_density = deposit_current_charge_conserving(
        *args,
        implementation="warp",
        **kwargs,
    )
    assert current_density.shape == (3, 24, 24, 24)


@requires_module("warp")
def test_deposit_current_charge_conserving_warp_current_density_inplace(device: str):
    args, kwargs = _build_case(
        device=device,
        num_particles=1024,
        grid_n=20,
        seed=8231,
        periodic=True,
        include_current_density=True,
        shape_order=1,
    )
    assert kwargs["current_density"] is not None
    current_density = kwargs["current_density"]
    current_density_before = current_density.clone()
    result = deposit_current_charge_conserving(
        *args,
        implementation="warp",
        **kwargs,
    )
    assert result.data_ptr() == current_density.data_ptr()
    assert not torch.equal(result, current_density_before)


def test_deposit_current_charge_conserving_make_inputs_forward(device: str):
    label, args, kwargs = next(
        iter(DepositCurrentChargeConserving.make_inputs_forward(device))
    )
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    output = DepositCurrentChargeConserving.dispatch(
        *args,
        implementation="torch",
        **kwargs,
    )
    assert isinstance(output, torch.Tensor)
    assert output.ndim == 4
    assert output.shape[0] == 3


def test_deposit_current_charge_conserving_compare_forward_contract(device: str):
    args, kwargs = _build_case(
        device=device,
        num_particles=512,
        grid_n=20,
        seed=8223,
        periodic=True,
        include_current_density=True,
        shape_order=1,
    )
    output = DepositCurrentChargeConserving.dispatch(
        *args,
        implementation="torch",
        **kwargs,
    )
    reference = output.clone()
    DepositCurrentChargeConserving.compare_forward(output, reference)


def test_deposit_current_charge_conserving_current_density_inplace(device: str):
    args, kwargs = _build_case(
        device=device,
        num_particles=1024,
        grid_n=20,
        seed=8230,
        periodic=True,
        include_current_density=True,
        shape_order=1,
    )
    assert kwargs["current_density"] is not None
    current_density = kwargs["current_density"]
    current_density_before = current_density.clone()
    result = deposit_current_charge_conserving(
        *args,
        implementation="torch",
        **kwargs,
    )
    assert result.data_ptr() == current_density.data_ptr()
    assert not torch.equal(result, current_density_before)


@requires_module("warp")
def test_deposit_current_charge_conserving_backend_forward_parity(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        num_particles=4096,
        grid_n=32,
        seed=8224,
        periodic=True,
        include_current_density=True,
        shape_order=1,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = DepositCurrentChargeConserving.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = DepositCurrentChargeConserving.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    DepositCurrentChargeConserving.compare_forward(out_warp, out_torch)


def test_deposit_current_charge_conserving_nonperiodic_rejected(device: str):
    args, kwargs = _build_case(
        device=device,
        num_particles=256,
        grid_n=20,
        seed=8225,
        periodic=False,
        include_current_density=False,
        shape_order=1,
    )
    with pytest.raises(ValueError, match="supports periodic=True only"):
        DepositCurrentChargeConserving.dispatch(
            *args,
            implementation="torch",
            **kwargs,
        )

    with pytest.raises(ValueError, match="supports periodic=True only"):
        deposit_current_charge_conserving(
            *args,
            implementation="torch",
            **kwargs,
        )


@requires_module("warp")
def test_deposit_current_charge_conserving_backend_forward_parity_custom_stagger(
    device: str,
):
    custom_stagger = (
        (0.25, 0.0, 0.0),
        (0.0, 0.25, 0.0),
        (0.0, 0.0, 0.25),
    )
    args_torch, kwargs_torch = _build_case(
        device=device,
        num_particles=2048,
        grid_n=24,
        seed=8226,
        periodic=True,
        include_current_density=True,
        current_stagger=custom_stagger,
        shape_order=1,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = DepositCurrentChargeConserving.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = DepositCurrentChargeConserving.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    DepositCurrentChargeConserving.compare_forward(out_warp, out_torch)


def test_deposit_current_charge_conserving_error_handling(device: str):
    args, kwargs = _build_case(
        device=device,
        num_particles=128,
        grid_n=16,
        seed=8227,
    )
    bad_position_old = args[0][..., :2]

    with pytest.raises(ValueError, match="particle_position_old must have shape"):
        deposit_current_charge_conserving(
            bad_position_old,
            args[1],
            args[2],
            args[3],
            args[4],
            args[5],
            args[6],
            args[7],
            args[8],
            args[9],
            args[10],
            implementation="torch",
            **kwargs,
        )

    bad_current_density = torch.randn(
        3,
        8,
        8,
        8,
        device=torch.device(device),
        dtype=torch.float32,
    )
    with pytest.raises(ValueError, match="current_density must have shape"):
        deposit_current_charge_conserving(
            *args,
            implementation="torch",
            current_density=bad_current_density,
        )


@requires_module("warp")
def test_deposit_current_charge_conserving_backend_single_particle_strict_parity(
    device: str,
):
    args_torch, kwargs_torch = _build_case(
        device=device,
        num_particles=1,
        grid_n=20,
        seed=8228,
        periodic=True,
        include_current_density=False,
        shape_order=1,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = DepositCurrentChargeConserving.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = DepositCurrentChargeConserving.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    torch.testing.assert_close(out_warp, out_torch, atol=1e-4, rtol=1e-4)


def test_deposit_current_charge_conserving_zero_displacement(device: str):
    args, _ = _build_case(
        device=device,
        num_particles=256,
        grid_n=20,
        seed=8229,
        periodic=True,
        include_current_density=False,
        shape_order=1,
    )
    stationary_args = (
        args[0],
        args[0].clone(),
        args[2],
        args[3],
        args[4],
        args[5],
        args[6],
        args[7],
        args[8],
        args[9],
        args[10],
    )

    out = DepositCurrentChargeConserving.dispatch(
        *stationary_args,
        implementation="torch",
        current_density=None,
    )
    torch.testing.assert_close(out, torch.zeros_like(out), atol=0.0, rtol=0.0)


def test_deposit_current_charge_conserving_shape_order_validation(device: str):
    args, kwargs = _build_case(
        device=device,
        num_particles=64,
        grid_n=16,
        seed=8240,
        periodic=True,
        include_current_density=False,
        shape_order=1,
    )
    with pytest.raises(ValueError, match="shape_order must be either 1 or 3"):
        deposit_current_charge_conserving(
            *args[:-1],
            2,
            implementation="torch",
            **kwargs,
        )


def test_deposit_current_charge_conserving_shape_order3_torch(device: str):
    args_ref, kwargs_ref = _build_case(
        device=device,
        num_particles=768,
        grid_n=24,
        seed=8241,
        periodic=True,
        include_current_density=False,
        shape_order=3,
    )
    args, kwargs = clone_case(args_ref, kwargs_ref)
    out = deposit_current_charge_conserving(
        *args,
        implementation="torch",
        **kwargs,
    )
    assert out.shape == (3, 24, 24, 24)
    assert torch.isfinite(out).all()
    assert torch.count_nonzero(out) > 0


@requires_module("warp")
def test_deposit_current_charge_conserving_backend_forward_parity_shape_order3(
    device: str,
):
    args_torch, kwargs_torch = _build_case(
        device=device,
        num_particles=2048,
        grid_n=24,
        seed=8242,
        periodic=True,
        include_current_density=True,
        shape_order=3,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = DepositCurrentChargeConserving.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = DepositCurrentChargeConserving.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    DepositCurrentChargeConserving.compare_forward(out_warp, out_torch)
