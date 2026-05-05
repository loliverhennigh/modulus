# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch

from .utils import _validate_inputs

SPEED_OF_LIGHT = 299_792_458.0


def _gamma_from_momentum(particle_momentum: torch.Tensor) -> torch.Tensor:
    momentum_sq = torch.sum(particle_momentum * particle_momentum, dim=-1, keepdim=True)
    return torch.sqrt(1.0 + momentum_sq / (SPEED_OF_LIGHT * SPEED_OF_LIGHT))


def _particle_push_boris_step(
    particle_position: torch.Tensor,
    particle_momentum: torch.Tensor,
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    charge_to_mass: float,
    dt: float,
) -> tuple[torch.Tensor, torch.Tensor]:
    half_qmdt = 0.5 * float(charge_to_mass) * float(dt)

    momentum_minus = particle_momentum + half_qmdt * electric_field
    gamma_minus = _gamma_from_momentum(momentum_minus)
    t = (half_qmdt / gamma_minus) * magnetic_field
    t2 = torch.sum(t * t, dim=-1, keepdim=True)
    s = (2.0 * t) / (1.0 + t2)

    momentum_prime = momentum_minus + torch.cross(momentum_minus, t, dim=-1)
    momentum_plus = momentum_minus + torch.cross(momentum_prime, s, dim=-1)
    momentum_new = momentum_plus + half_qmdt * electric_field

    gamma_new = _gamma_from_momentum(momentum_new)
    velocity_new = momentum_new / gamma_new
    position_new = particle_position + float(dt) * velocity_new
    return position_new, momentum_new


def particle_push_boris_torch(
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

    position_new, momentum_new = _particle_push_boris_step(
        particle_position=particle_position,
        particle_momentum=particle_momentum,
        electric_field=electric_field,
        magnetic_field=magnetic_field,
        charge_to_mass=charge_to_mass,
        dt=dt,
    )

    if not inplace:
        return position_new, momentum_new

    particle_position.copy_(position_new)
    particle_momentum.copy_(momentum_new)
    position_out = particle_position
    momentum_out = particle_momentum
    return position_out, momentum_out
