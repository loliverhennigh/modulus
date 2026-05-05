# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch

from ..utils import _validate_inputs
from .launch_backward import _launch_warp_backward
from .launch_forward import _launch_warp_forward


class _ParticlePushBorisWarpFunction(torch.autograd.Function):
    @staticmethod
    def forward(
        ctx: torch.autograd.function.FunctionCtx,
        particle_position: torch.Tensor,
        particle_momentum: torch.Tensor,
        electric_field: torch.Tensor,
        magnetic_field: torch.Tensor,
        charge_to_mass: float,
        dt: float,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        position_out = particle_position.clone()
        momentum_out = particle_momentum.clone()

        _launch_warp_forward(
            particle_position=particle_position,
            particle_momentum=particle_momentum,
            electric_field=electric_field,
            magnetic_field=magnetic_field,
            charge_to_mass=charge_to_mass,
            dt=dt,
            particle_position_out=position_out,
            particle_momentum_out=momentum_out,
        )

        ctx.save_for_backward(
            particle_position,
            particle_momentum,
            electric_field,
            magnetic_field,
        )
        ctx.charge_to_mass = float(charge_to_mass)
        ctx.dt = float(dt)
        return position_out, momentum_out

    @staticmethod
    def backward(
        ctx: torch.autograd.function.FunctionCtx,
        grad_particle_position_out: torch.Tensor | None,
        grad_particle_momentum_out: torch.Tensor | None,
    ) -> tuple[
        torch.Tensor | None,
        torch.Tensor | None,
        torch.Tensor | None,
        torch.Tensor | None,
        None,
        None,
    ]:
        (
            particle_position,
            particle_momentum,
            electric_field,
            magnetic_field,
        ) = ctx.saved_tensors

        (
            grad_position,
            grad_momentum,
            grad_electric,
            grad_magnetic,
        ) = _launch_warp_backward(
            particle_position=particle_position,
            particle_momentum=particle_momentum,
            electric_field=electric_field,
            magnetic_field=magnetic_field,
            grad_particle_position_out=grad_particle_position_out,
            grad_particle_momentum_out=grad_particle_momentum_out,
            charge_to_mass=ctx.charge_to_mass,
            dt=ctx.dt,
            needs_input_grad=ctx.needs_input_grad,
        )
        return (
            grad_position,
            grad_momentum,
            grad_electric,
            grad_magnetic,
            None,
            None,
        )


def particle_push_boris_warp(
    particle_position: torch.Tensor,
    particle_momentum: torch.Tensor,
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    charge_to_mass: float,
    dt: float,
    inplace: bool = False,
) -> tuple[torch.Tensor, torch.Tensor]:
    _validate_inputs(
        particle_position=particle_position,
        particle_momentum=particle_momentum,
        electric_field=electric_field,
        magnetic_field=magnetic_field,
        charge_to_mass=charge_to_mass,
        dt=dt,
        inplace=inplace,
    )

    for name, tensor in (
        ("particle_position", particle_position),
        ("particle_momentum", particle_momentum),
        ("electric_field", electric_field),
        ("magnetic_field", magnetic_field),
    ):
        if not tensor.is_contiguous():
            raise ValueError(f"{name} must be contiguous for the warp implementation")

    if not inplace:
        return _ParticlePushBorisWarpFunction.apply(
            particle_position,
            particle_momentum,
            electric_field,
            magnetic_field,
            float(charge_to_mass),
            float(dt),
        )

    position_out = particle_position
    momentum_out = particle_momentum

    _launch_warp_forward(
        particle_position=particle_position,
        particle_momentum=particle_momentum,
        electric_field=electric_field,
        magnetic_field=magnetic_field,
        charge_to_mass=charge_to_mass,
        dt=dt,
        particle_position_out=position_out,
        particle_momentum_out=momentum_out,
    )
    return position_out, momentum_out
