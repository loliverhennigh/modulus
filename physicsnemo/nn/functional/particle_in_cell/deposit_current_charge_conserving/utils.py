# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import math
from typing import Sequence

import torch

_WARPX_YEE_CURRENT_STAGGER = (
    (0.5, 0.0, 0.0),  # Jx: cell-centered in x, nodal in y,z
    (0.0, 0.5, 0.0),  # Jy: nodal in x,z, cell-centered in y
    (0.0, 0.0, 0.5),  # Jz: nodal in x,y, cell-centered in z
)


def _as_vec3_tensor(
    name: str,
    value: torch.Tensor | Sequence[float],
    device: torch.device,
    dtype: torch.dtype,
) -> torch.Tensor:
    if isinstance(value, torch.Tensor):
        tensor = value.detach().to(device=device, dtype=dtype).flatten()
    else:
        tensor = torch.tensor(value, device=device, dtype=dtype).flatten()
    if tensor.numel() != 3:
        raise ValueError(f"{name} must contain exactly three elements")
    if not torch.isfinite(tensor).all():
        raise ValueError(f"{name} must be finite")
    return tensor.contiguous()


def _as_stagger_tensor(
    current_stagger: torch.Tensor | Sequence[Sequence[float]] | None,
    device: torch.device,
    dtype: torch.dtype,
) -> torch.Tensor:
    source = _WARPX_YEE_CURRENT_STAGGER if current_stagger is None else current_stagger
    if isinstance(source, torch.Tensor):
        tensor = source.detach().to(device=device, dtype=dtype)
    else:
        tensor = torch.tensor(source, device=device, dtype=dtype)
    if tensor.shape != (3, 3):
        raise ValueError("current_stagger must have shape (3, 3)")
    if not torch.isfinite(tensor).all():
        raise ValueError("current_stagger must be finite")
    return tensor.contiguous()


def _as_grid_shape(grid_shape: Sequence[int] | torch.Tensor) -> tuple[int, int, int]:
    if isinstance(grid_shape, torch.Tensor):
        values = grid_shape.detach().cpu().flatten().tolist()
    else:
        values = list(grid_shape)
    if len(values) != 3:
        raise ValueError("grid_shape must contain exactly three integers")
    nx, ny, nz = int(values[0]), int(values[1]), int(values[2])
    if nx < 2 or ny < 2 or nz < 2:
        raise ValueError("grid_shape must be >= 2 in each dimension")
    return nx, ny, nz


def _validate_inputs(
    particle_position_old: torch.Tensor,
    particle_position_new: torch.Tensor,
    particle_weight: torch.Tensor,
    particle_charge: float,
    dt: float,
    periodic: bool,
    shape_order: int,
) -> None:
    for name, tensor in (
        ("particle_position_old", particle_position_old),
        ("particle_position_new", particle_position_new),
    ):
        if not isinstance(tensor, torch.Tensor):
            raise TypeError(f"{name} must be a torch.Tensor")
        if tensor.dtype != torch.float32:
            raise TypeError(f"{name} must use torch.float32 dtype")
        if tensor.ndim != 2 or tensor.shape[1] != 3:
            raise ValueError(f"{name} must have shape (num_particles, 3)")

    if not isinstance(particle_weight, torch.Tensor):
        raise TypeError("particle_weight must be a torch.Tensor")
    if particle_weight.dtype != torch.float32:
        raise TypeError("particle_weight must use torch.float32 dtype")
    if particle_weight.ndim != 1:
        raise ValueError("particle_weight must have shape (num_particles,)")

    num_particles = int(particle_position_old.shape[0])
    if int(particle_position_new.shape[0]) != num_particles:
        raise ValueError("particle_position_new must have same number of particles")
    if int(particle_weight.shape[0]) != num_particles:
        raise ValueError("particle_weight must have same number of particles")

    device = particle_position_old.device
    if particle_position_new.device != device:
        raise ValueError(
            "particle_position_new must be on same device as particle_position_old"
        )
    if particle_weight.device != device:
        raise ValueError("particle_weight must be on same device as particle_position_old")

    if particle_position_old.requires_grad or particle_position_new.requires_grad:
        raise ValueError(
            "deposit_current_charge_conserving does not support gradients through particle positions"
        )
    if particle_weight.requires_grad:
        raise ValueError(
            "deposit_current_charge_conserving does not support gradients through particle_weight"
        )

    if not isinstance(particle_charge, (int, float)) or isinstance(particle_charge, bool):
        raise TypeError("particle_charge must be a float")
    if not math.isfinite(float(particle_charge)):
        raise ValueError("particle_charge must be finite")

    if not isinstance(dt, (int, float)) or isinstance(dt, bool):
        raise TypeError("dt must be a float")
    if not math.isfinite(float(dt)) or float(dt) <= 0.0:
        raise ValueError("dt must be a finite positive scalar")

    if not isinstance(periodic, bool):
        raise TypeError("periodic must be a bool")
    if not periodic:
        raise ValueError(
            "deposit_current_charge_conserving currently supports periodic=True only"
        )

    if not isinstance(shape_order, int) or isinstance(shape_order, bool):
        raise TypeError("shape_order must be an int")
    if shape_order not in (1, 3):
        raise ValueError("shape_order must be either 1 or 3")


def _prepare_inputs(
    particle_position_old: torch.Tensor,
    particle_position_new: torch.Tensor,
    particle_weight: torch.Tensor,
    particle_charge: float,
    dt: float,
    grid_shape: Sequence[int] | torch.Tensor,
    origin: torch.Tensor | Sequence[float],
    spacing: torch.Tensor | Sequence[float],
    current_stagger: torch.Tensor | Sequence[Sequence[float]] | None,
    periodic: bool,
    shape_order: int,
) -> tuple[tuple[int, int, int], torch.Tensor, torch.Tensor, torch.Tensor, int]:
    _validate_inputs(
        particle_position_old=particle_position_old,
        particle_position_new=particle_position_new,
        particle_weight=particle_weight,
        particle_charge=particle_charge,
        dt=dt,
        periodic=periodic,
        shape_order=shape_order,
    )

    nx, ny, nz = _as_grid_shape(grid_shape)
    device = particle_position_old.device
    dtype = particle_position_old.dtype

    origin_tensor = _as_vec3_tensor("origin", origin, device=device, dtype=dtype)
    spacing_tensor = _as_vec3_tensor("spacing", spacing, device=device, dtype=dtype)
    if not bool((spacing_tensor > 0.0).all()):
        raise ValueError("spacing must be strictly positive in each dimension")

    current_stagger_tensor = _as_stagger_tensor(
        current_stagger=current_stagger,
        device=device,
        dtype=dtype,
    )

    return (nx, ny, nz), origin_tensor, spacing_tensor, current_stagger_tensor, int(
        shape_order
    )


def _as_float3(value: torch.Tensor | Sequence[float], name: str) -> tuple[float, float, float]:
    if isinstance(value, torch.Tensor):
        vector = value.detach().cpu().flatten().tolist()
    else:
        vector = list(value)
    if len(vector) != 3:
        raise ValueError(f"{name} must contain exactly three elements")
    values = (float(vector[0]), float(vector[1]), float(vector[2]))
    if not all(math.isfinite(v) for v in values):
        raise ValueError(f"{name} must be finite")
    return values


__all__ = [
    "_WARPX_YEE_CURRENT_STAGGER",
    "_as_float3",
    "_prepare_inputs",
]
