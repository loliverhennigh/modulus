# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from ..utils import _prepare_inputs
from .launch_backward import _launch_warp_backward
from .launch_forward import _launch_warp_forward


class _GatherFieldsToParticlesWarpFunction(torch.autograd.Function):
    @staticmethod
    def forward(
        ctx: torch.autograd.function.FunctionCtx,
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
        electric_particle = torch.empty_like(particle_position)
        magnetic_particle = torch.empty_like(particle_position)

        _launch_warp_forward(
            particle_position=particle_position,
            electric_field=electric_field,
            magnetic_field=magnetic_field,
            origin=origin,
            spacing=spacing,
            electric_stagger=electric_stagger,
            magnetic_stagger=magnetic_stagger,
            shape_order=shape_order,
            gather_mode=gather_mode,
            electric_particle=electric_particle,
            magnetic_particle=magnetic_particle,
        )

        ctx.save_for_backward(
            particle_position,
            electric_field,
            magnetic_field,
            origin,
            spacing,
            electric_stagger,
            magnetic_stagger,
        )
        ctx.shape_order = int(shape_order)
        ctx.gather_mode = str(gather_mode)
        return electric_particle, magnetic_particle

    @staticmethod
    def backward(
        ctx: torch.autograd.function.FunctionCtx,
        grad_electric_particle: torch.Tensor | None,
        grad_magnetic_particle: torch.Tensor | None,
    ) -> tuple[
        torch.Tensor | None,
        torch.Tensor | None,
        torch.Tensor | None,
        None,
        None,
        None,
        None,
        None,
        None,
    ]:
        (
            particle_position,
            electric_field,
            magnetic_field,
            origin,
            spacing,
            electric_stagger,
            magnetic_stagger,
        ) = ctx.saved_tensors

        (
            grad_particle_position,
            grad_electric_field,
            grad_magnetic_field,
        ) = _launch_warp_backward(
            particle_position=particle_position,
            electric_field=electric_field,
            magnetic_field=magnetic_field,
            origin=origin,
            spacing=spacing,
            electric_stagger=electric_stagger,
            magnetic_stagger=magnetic_stagger,
            grad_electric_particle=grad_electric_particle,
            grad_magnetic_particle=grad_magnetic_particle,
            needs_input_grad=ctx.needs_input_grad,
            shape_order=ctx.shape_order,
            gather_mode=ctx.gather_mode,
        )

        return (
            grad_particle_position,
            grad_electric_field,
            grad_magnetic_field,
            None,
            None,
            None,
            None,
            None,
            None,
        )


def gather_fields_to_particles_warp(
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

    for name, tensor in (
        ("particle_position", particle_position),
        ("electric_field", electric_field),
        ("magnetic_field", magnetic_field),
    ):
        if not tensor.is_contiguous():
            raise ValueError(f"{name} must be contiguous for the warp implementation")

    return _GatherFieldsToParticlesWarpFunction.apply(
        particle_position,
        electric_field,
        magnetic_field,
        origin_tensor,
        spacing_tensor,
        electric_stagger_tensor,
        magnetic_stagger_tensor,
        shape_order_value,
        gather_mode_value,
    )


__all__ = ["gather_fields_to_particles_warp"]
