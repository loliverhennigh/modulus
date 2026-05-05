# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from .utils import _prepare_inputs


def _shape_factor_1d(
    coord: torch.Tensor,
    order: int,
) -> tuple[torch.Tensor, tuple[torch.Tensor, ...]]:
    if order == 0:
        base = torch.floor(coord + 0.5).to(dtype=torch.int64)
        return base, (torch.ones_like(coord),)

    if order == 1:
        base = torch.floor(coord).to(dtype=torch.int64)
        frac = coord - base.to(dtype=coord.dtype)
        return base, (1.0 - frac, frac)

    if order == 2:
        base_center = torch.floor(coord + 0.5).to(dtype=torch.int64)
        frac = coord - base_center.to(dtype=coord.dtype)
        weight_0 = 0.5 * (0.5 - frac) * (0.5 - frac)
        weight_1 = 0.75 - frac * frac
        weight_2 = 0.5 * (0.5 + frac) * (0.5 + frac)
        return base_center - 1, (weight_0, weight_1, weight_2)

    if order == 3:
        base_center = torch.floor(coord).to(dtype=torch.int64)
        frac = coord - base_center.to(dtype=coord.dtype)
        one_minus_frac = 1.0 - frac
        weight_0 = (1.0 / 6.0) * one_minus_frac * one_minus_frac * one_minus_frac
        weight_1 = (2.0 / 3.0) - frac * frac * (1.0 - 0.5 * frac)
        weight_2 = (2.0 / 3.0) - one_minus_frac * one_minus_frac * (
            1.0 - 0.5 * one_minus_frac
        )
        weight_3 = (1.0 / 6.0) * frac * frac * frac
        return base_center - 1, (weight_0, weight_1, weight_2, weight_3)

    raise ValueError(f"unsupported interpolation order: {order}")


def _resolve_component_orders(
    shape_order: int,
    gather_mode: str,
    component: int,
    field_kind: str,
) -> tuple[int, int, int]:
    if gather_mode == "momentum-conserving":
        return (shape_order, shape_order, shape_order)

    reduced_order = max(shape_order - 1, 0)
    if field_kind == "electric":
        order_x = reduced_order if component == 0 else shape_order
        order_y = reduced_order if component == 1 else shape_order
        order_z = reduced_order if component == 2 else shape_order
        return (order_x, order_y, order_z)

    order_x = shape_order if component == 0 else reduced_order
    order_y = shape_order if component == 1 else reduced_order
    order_z = shape_order if component == 2 else reduced_order
    return (order_x, order_y, order_z)


def _gather_component_shape(
    field_component: torch.Tensor,
    coord: torch.Tensor,
    order_xyz: tuple[int, int, int],
) -> torch.Tensor:
    nx, ny, nz = field_component.shape

    base_x, weights_x = _shape_factor_1d(coord[:, 0], order_xyz[0])
    base_y, weights_y = _shape_factor_1d(coord[:, 1], order_xyz[1])
    base_z, weights_z = _shape_factor_1d(coord[:, 2], order_xyz[2])

    gathered = torch.zeros_like(coord[:, 0])
    for index_x, weight_x in enumerate(weights_x):
        ix = torch.remainder(base_x + index_x, nx)
        for index_y, weight_y in enumerate(weights_y):
            iy = torch.remainder(base_y + index_y, ny)
            for index_z, weight_z in enumerate(weights_z):
                iz = torch.remainder(base_z + index_z, nz)
                gathered = gathered + (
                    weight_x * weight_y * weight_z * field_component[ix, iy, iz]
                )

    return gathered


def _gather_fields_to_particles_step(
    particle_position: torch.Tensor,
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    origin: torch.Tensor,
    spacing: torch.Tensor,
    electric_stagger: torch.Tensor,
    magnetic_stagger: torch.Tensor,
    shape_order: int,
    gather_mode: str,
) -> tuple[torch.Tensor, torch.Tensor]:
    # Convert particle coordinates from physical units to grid units.
    grid_coord = (particle_position - origin.unsqueeze(0)) / spacing.unsqueeze(0)

    electric_particle = torch.empty_like(particle_position)
    magnetic_particle = torch.empty_like(particle_position)

    for component in range(3):
        coord_e = grid_coord - electric_stagger[component].unsqueeze(0)
        coord_b = grid_coord - magnetic_stagger[component].unsqueeze(0)
        electric_orders = _resolve_component_orders(
            shape_order=shape_order,
            gather_mode=gather_mode,
            component=component,
            field_kind="electric",
        )
        magnetic_orders = _resolve_component_orders(
            shape_order=shape_order,
            gather_mode=gather_mode,
            component=component,
            field_kind="magnetic",
        )
        electric_particle[:, component] = _gather_component_shape(
            electric_field[component],
            coord_e,
            electric_orders,
        )
        magnetic_particle[:, component] = _gather_component_shape(
            magnetic_field[component],
            coord_b,
            magnetic_orders,
        )

    return electric_particle, magnetic_particle


def gather_fields_to_particles_torch(
    particle_position: torch.Tensor,
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    origin: torch.Tensor | Sequence[float] = (0.0, 0.0, 0.0),
    spacing: torch.Tensor | Sequence[float] = (1.0, 1.0, 1.0),
    electric_stagger: torch.Tensor | Sequence[Sequence[float]] | None = None,
    magnetic_stagger: torch.Tensor | Sequence[Sequence[float]] | None = None,
    periodic: bool = True,
    shape_order: int = 1,
    gather_mode: str = "momentum-conserving",
) -> tuple[torch.Tensor, torch.Tensor]:
    (
        origin_tensor,
        spacing_tensor,
        electric_stagger_tensor,
        magnetic_stagger_tensor,
        shape_order_value,
        gather_mode_value,
    ) = _prepare_inputs(
        particle_position=particle_position,
        electric_field=electric_field,
        magnetic_field=magnetic_field,
        origin=origin,
        spacing=spacing,
        electric_stagger=electric_stagger,
        magnetic_stagger=magnetic_stagger,
        periodic=periodic,
        shape_order=shape_order,
        gather_mode=gather_mode,
    )

    return _gather_fields_to_particles_step(
        particle_position=particle_position,
        electric_field=electric_field,
        magnetic_field=magnetic_field,
        origin=origin_tensor,
        spacing=spacing_tensor,
        electric_stagger=electric_stagger_tensor,
        magnetic_stagger=magnetic_stagger_tensor,
        shape_order=shape_order_value,
        gather_mode=gather_mode_value,
    )


__all__ = [
    "_gather_fields_to_particles_step",
    "gather_fields_to_particles_torch",
]
