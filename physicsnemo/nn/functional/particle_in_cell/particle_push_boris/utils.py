# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import math

import torch


def _validate_inputs(
    particle_position: torch.Tensor,
    particle_momentum: torch.Tensor,
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    charge_to_mass: float,
    dt: float,
    inplace: bool,
) -> None:
    for name, tensor in (
        ("particle_position", particle_position),
        ("particle_momentum", particle_momentum),
        ("electric_field", electric_field),
        ("magnetic_field", magnetic_field),
    ):
        if not isinstance(tensor, torch.Tensor):
            raise TypeError(f"{name} must be a torch.Tensor")
        if tensor.dtype != torch.float32:
            raise TypeError(f"{name} must use torch.float32 dtype")
        if tensor.ndim != 2 or tensor.shape[1] != 3:
            raise ValueError(f"{name} must have shape (num_particles, 3)")

    num_particles = int(particle_position.shape[0])
    for name, tensor in (
        ("particle_momentum", particle_momentum),
        ("electric_field", electric_field),
        ("magnetic_field", magnetic_field),
    ):
        if int(tensor.shape[0]) != num_particles:
            raise ValueError(
                f"{name} must have the same number of particles as particle_position"
            )

    device = particle_position.device
    if particle_momentum.device != device:
        raise ValueError("particle_momentum must be on the same device as particle_position")
    if electric_field.device != device:
        raise ValueError("electric_field must be on the same device as particle_position")
    if magnetic_field.device != device:
        raise ValueError("magnetic_field must be on the same device as particle_position")

    if not isinstance(charge_to_mass, (int, float)) or isinstance(charge_to_mass, bool):
        raise TypeError("charge_to_mass must be a float")
    if not math.isfinite(float(charge_to_mass)):
        raise ValueError("charge_to_mass must be finite")

    if not isinstance(dt, (int, float)) or isinstance(dt, bool):
        raise TypeError("dt must be a float")
    if not math.isfinite(float(dt)) or float(dt) <= 0.0:
        raise ValueError("dt must be a finite positive scalar")

    if not isinstance(inplace, bool):
        raise TypeError("inplace must be a bool")

    if inplace and (
        particle_position.requires_grad
        or particle_momentum.requires_grad
        or electric_field.requires_grad
        or magnetic_field.requires_grad
    ):
        raise ValueError(
            "inplace=True is not supported when autograd is enabled for particle inputs"
        )
