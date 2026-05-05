# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import math
from typing import Sequence

import torch

_YEE_ELECTRIC_STAGGER = (
    (0.0, 0.5, 0.5),  # Ex at (i, j+1/2, k+1/2)
    (0.5, 0.0, 0.5),  # Ey at (i+1/2, j, k+1/2)
    (0.5, 0.5, 0.0),  # Ez at (i+1/2, j+1/2, k)
)

_YEE_MAGNETIC_STAGGER = (
    (0.5, 0.0, 0.0),  # Bx at (i+1/2, j, k)
    (0.0, 0.5, 0.0),  # By at (i, j+1/2, k)
    (0.0, 0.0, 0.5),  # Bz at (i, j, k+1/2)
)

_SUPPORTED_SHAPE_ORDERS = (1, 3)
_SUPPORTED_GATHER_MODES = (
    "momentum-conserving",
    "energy-conserving",
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
    name: str,
    value: torch.Tensor | Sequence[Sequence[float]] | None,
    default: Sequence[Sequence[float]],
    device: torch.device,
    dtype: torch.dtype,
) -> torch.Tensor:
    source = default if value is None else value
    if isinstance(source, torch.Tensor):
        tensor = source.detach().to(device=device, dtype=dtype)
    else:
        tensor = torch.tensor(source, device=device, dtype=dtype)
    if tensor.shape != (3, 3):
        raise ValueError(f"{name} must have shape (3, 3)")
    if not torch.isfinite(tensor).all():
        raise ValueError(f"{name} must be finite")
    return tensor.contiguous()


def _validate_grid_field(name: str, field: torch.Tensor) -> None:
    if not isinstance(field, torch.Tensor):
        raise TypeError(f"{name} must be a torch.Tensor")
    if field.dtype != torch.float32:
        raise TypeError(f"{name} must use torch.float32 dtype")
    if field.ndim != 4 or field.shape[0] != 3:
        raise ValueError(f"{name} must have shape (3, nx, ny, nz)")
    if int(field.shape[1]) < 1 or int(field.shape[2]) < 1 or int(field.shape[3]) < 1:
        raise ValueError(f"{name} must have positive spatial dimensions")


def _prepare_inputs(
    particle_position: torch.Tensor,
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    origin: torch.Tensor | Sequence[float],
    spacing: torch.Tensor | Sequence[float],
    electric_stagger: torch.Tensor | Sequence[Sequence[float]] | None,
    magnetic_stagger: torch.Tensor | Sequence[Sequence[float]] | None,
    periodic: bool,
    shape_order: int,
    gather_mode: str,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, int, str]:
    if not isinstance(particle_position, torch.Tensor):
        raise TypeError("particle_position must be a torch.Tensor")
    if particle_position.dtype != torch.float32:
        raise TypeError("particle_position must use torch.float32 dtype")
    if particle_position.ndim != 2 or particle_position.shape[1] != 3:
        raise ValueError("particle_position must have shape (num_particles, 3)")

    _validate_grid_field("electric_field", electric_field)
    _validate_grid_field("magnetic_field", magnetic_field)

    if electric_field.shape != magnetic_field.shape:
        raise ValueError("electric_field and magnetic_field must have identical shapes")

    if particle_position.device != electric_field.device:
        raise ValueError("particle_position must be on the same device as electric_field")
    if magnetic_field.device != electric_field.device:
        raise ValueError("magnetic_field must be on the same device as electric_field")

    if not isinstance(periodic, bool):
        raise TypeError("periodic must be a bool")
    if not periodic:
        raise ValueError(
            "gather_fields_to_particles currently supports periodic=True only"
        )

    if isinstance(shape_order, bool) or not isinstance(shape_order, int):
        raise TypeError("shape_order must be an int")
    if shape_order not in _SUPPORTED_SHAPE_ORDERS:
        supported_orders = ", ".join(str(order) for order in _SUPPORTED_SHAPE_ORDERS)
        raise ValueError(f"shape_order must be one of: {supported_orders}")

    if not isinstance(gather_mode, str):
        raise TypeError("gather_mode must be a str")
    gather_mode_normalized = gather_mode.strip().lower()
    if gather_mode_normalized not in _SUPPORTED_GATHER_MODES:
        supported_modes = ", ".join(_SUPPORTED_GATHER_MODES)
        raise ValueError(f"gather_mode must be one of: {supported_modes}")

    device = particle_position.device
    dtype = particle_position.dtype

    origin_tensor = _as_vec3_tensor("origin", origin, device=device, dtype=dtype)
    spacing_tensor = _as_vec3_tensor("spacing", spacing, device=device, dtype=dtype)
    if not bool((spacing_tensor > 0.0).all()):
        raise ValueError("spacing must be strictly positive in each dimension")

    electric_stagger_tensor = _as_stagger_tensor(
        "electric_stagger",
        electric_stagger,
        _YEE_ELECTRIC_STAGGER,
        device=device,
        dtype=dtype,
    )
    magnetic_stagger_tensor = _as_stagger_tensor(
        "magnetic_stagger",
        magnetic_stagger,
        _YEE_MAGNETIC_STAGGER,
        device=device,
        dtype=dtype,
    )

    return (
        origin_tensor,
        spacing_tensor,
        electric_stagger_tensor,
        magnetic_stagger_tensor,
        shape_order,
        gather_mode_normalized,
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
    "_YEE_ELECTRIC_STAGGER",
    "_YEE_MAGNETIC_STAGGER",
    "_SUPPORTED_GATHER_MODES",
    "_SUPPORTED_SHAPE_ORDERS",
    "_as_float3",
    "_prepare_inputs",
]
