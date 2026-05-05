# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch

from .._torch_impl import _gather_fields_to_particles_step


def _launch_warp_backward(
    particle_position: torch.Tensor,
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    origin: torch.Tensor,
    spacing: torch.Tensor,
    electric_stagger: torch.Tensor,
    magnetic_stagger: torch.Tensor,
    shape_order: int,
    gather_mode: str,
    grad_electric_particle: torch.Tensor | None,
    grad_magnetic_particle: torch.Tensor | None,
    needs_input_grad: tuple[bool, ...],
) -> tuple[torch.Tensor | None, torch.Tensor | None, torch.Tensor | None]:
    if not any(needs_input_grad[:3]):
        return (None, None, None)

    with torch.enable_grad():
        pos_in = particle_position.detach()
        e_in = electric_field.detach()
        b_in = magnetic_field.detach()

        all_inputs = [pos_in, e_in, b_in]
        active_input_indices: list[int] = []
        active_inputs: list[torch.Tensor] = []
        for input_index in range(3):
            if bool(needs_input_grad[input_index]):
                all_inputs[input_index].requires_grad_(True)
                active_input_indices.append(input_index)
                active_inputs.append(all_inputs[input_index])

        electric_particle, magnetic_particle = _gather_fields_to_particles_step(
            particle_position=pos_in,
            electric_field=e_in,
            magnetic_field=b_in,
            origin=origin.detach(),
            spacing=spacing.detach(),
            electric_stagger=electric_stagger.detach(),
            magnetic_stagger=magnetic_stagger.detach(),
            shape_order=int(shape_order),
            gather_mode=str(gather_mode),
        )

        grad_electric = (
            grad_electric_particle
            if grad_electric_particle is not None
            else torch.zeros_like(electric_particle)
        )
        grad_magnetic = (
            grad_magnetic_particle
            if grad_magnetic_particle is not None
            else torch.zeros_like(magnetic_particle)
        )

        active_outputs: list[torch.Tensor] = []
        active_output_grads: list[torch.Tensor] = []
        if electric_particle.requires_grad:
            active_outputs.append(electric_particle)
            active_output_grads.append(grad_electric)
        if magnetic_particle.requires_grad:
            active_outputs.append(magnetic_particle)
            active_output_grads.append(grad_magnetic)
        if not active_outputs:
            return (None, None, None)

        grad_active_inputs = torch.autograd.grad(
            outputs=tuple(active_outputs),
            inputs=tuple(active_inputs),
            grad_outputs=tuple(active_output_grads),
            allow_unused=True,
            retain_graph=False,
            create_graph=False,
        )

    grads: list[torch.Tensor | None] = [None, None, None]
    for input_index, grad_value in zip(active_input_indices, grad_active_inputs):
        grads[input_index] = grad_value
    return (grads[0], grads[1], grads[2])
