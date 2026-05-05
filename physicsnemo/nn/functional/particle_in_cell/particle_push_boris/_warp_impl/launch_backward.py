# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch

from .._torch_impl import _particle_push_boris_step


def _launch_warp_backward(
    particle_position: torch.Tensor,
    particle_momentum: torch.Tensor,
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    grad_particle_position_out: torch.Tensor | None,
    grad_particle_momentum_out: torch.Tensor | None,
    charge_to_mass: float,
    dt: float,
    needs_input_grad: tuple[bool, ...],
) -> tuple[torch.Tensor | None, torch.Tensor | None, torch.Tensor | None, torch.Tensor | None]:
    if not any(needs_input_grad[:4]):
        return (None, None, None, None)

    with torch.enable_grad():
        pos_in = particle_position.detach()
        momentum_in = particle_momentum.detach()
        e_in = electric_field.detach()
        b_in = magnetic_field.detach()

        all_inputs = [pos_in, momentum_in, e_in, b_in]
        active_input_indices: list[int] = []
        active_inputs: list[torch.Tensor] = []
        for input_index in range(4):
            if bool(needs_input_grad[input_index]):
                all_inputs[input_index].requires_grad_(True)
                active_input_indices.append(input_index)
                active_inputs.append(all_inputs[input_index])

        pos_out, momentum_out = _particle_push_boris_step(
            particle_position=pos_in,
            particle_momentum=momentum_in,
            electric_field=e_in,
            magnetic_field=b_in,
            charge_to_mass=charge_to_mass,
            dt=dt,
        )

        grad_pos = (
            grad_particle_position_out
            if grad_particle_position_out is not None
            else torch.zeros_like(pos_out)
        )
        grad_momentum = (
            grad_particle_momentum_out
            if grad_particle_momentum_out is not None
            else torch.zeros_like(momentum_out)
        )

        active_outputs: list[torch.Tensor] = []
        active_output_grads: list[torch.Tensor] = []
        if pos_out.requires_grad:
            active_outputs.append(pos_out)
            active_output_grads.append(grad_pos)
        if momentum_out.requires_grad:
            active_outputs.append(momentum_out)
            active_output_grads.append(grad_momentum)
        if not active_outputs:
            return (None, None, None, None)

        grad_active_inputs = torch.autograd.grad(
            outputs=tuple(active_outputs),
            inputs=tuple(active_inputs),
            grad_outputs=tuple(active_output_grads),
            allow_unused=True,
            retain_graph=False,
            create_graph=False,
        )

    grads: list[torch.Tensor | None] = [None, None, None, None]
    for input_index, grad_value in zip(active_input_indices, grad_active_inputs):
        grads[input_index] = grad_value
    return (grads[0], grads[1], grads[2], grads[3])
