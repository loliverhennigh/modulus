# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from ..utils import _prepare_inputs
from .launch_forward import _launch_warp_forward


def deposit_current_charge_conserving_warp(
    particle_position_old: torch.Tensor,
    particle_position_new: torch.Tensor,
    particle_weight: torch.Tensor,
    particle_charge: float,
    dt: float,
    grid_shape: Sequence[int] | torch.Tensor,
    origin: torch.Tensor | Sequence[float] = (0.0, 0.0, 0.0),
    spacing: torch.Tensor | Sequence[float] = (1.0, 1.0, 1.0),
    current_stagger: torch.Tensor | Sequence[Sequence[float]] | None = None,
    periodic: bool = True,
    shape_order: int = 1,
    current_density: torch.Tensor | None = None,
) -> torch.Tensor:
    (
        grid_shape_tuple,
        origin_tensor,
        spacing_tensor,
        current_stagger_tensor,
        shape_order_value,
    ) = _prepare_inputs(
        particle_position_old=particle_position_old,
        particle_position_new=particle_position_new,
        particle_weight=particle_weight,
        particle_charge=particle_charge,
        dt=dt,
        grid_shape=grid_shape,
        origin=origin,
        spacing=spacing,
        current_stagger=current_stagger,
        periodic=periodic,
        shape_order=shape_order,
    )

    for name, tensor in (
        ("particle_position_old", particle_position_old),
        ("particle_position_new", particle_position_new),
        ("particle_weight", particle_weight),
    ):
        if not tensor.is_contiguous():
            raise ValueError(f"{name} must be contiguous for the warp implementation")

    if current_density is None:
        current_density_out = torch.zeros(
            (3, *grid_shape_tuple),
            device=particle_position_old.device,
            dtype=torch.float32,
        )
    else:
        if not isinstance(current_density, torch.Tensor):
            raise TypeError("current_density must be a torch.Tensor when provided")
        if current_density.dtype != torch.float32:
            raise TypeError("current_density must use torch.float32 dtype")
        if current_density.shape != (3, *grid_shape_tuple):
            raise ValueError("current_density must have shape (3, nx, ny, nz)")
        if current_density.device != particle_position_old.device:
            raise ValueError("current_density must be on the same device as particle inputs")
        if current_density.requires_grad:
            raise ValueError(
                "deposit_current_charge_conserving does not support gradients through current_density"
            )
        if not current_density.is_contiguous():
            raise ValueError("current_density must be contiguous for the warp implementation")
        current_density_out = current_density

    _launch_warp_forward(
        particle_position_old=particle_position_old,
        particle_position_new=particle_position_new,
        particle_weight=particle_weight,
        particle_charge=particle_charge,
        dt=dt,
        origin=origin_tensor,
        spacing=spacing_tensor,
        current_stagger=current_stagger_tensor,
        current_density=current_density_out,
        shape_order=shape_order_value,
    )
    return current_density_out


__all__ = ["deposit_current_charge_conserving_warp"]
