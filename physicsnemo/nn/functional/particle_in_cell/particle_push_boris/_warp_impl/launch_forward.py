# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ._kernels import _particle_push_boris_kernel_scalar_scalar


def _launch_warp_forward(
    particle_position: torch.Tensor,
    particle_momentum: torch.Tensor,
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    charge_to_mass: float,
    dt: float,
    particle_position_out: torch.Tensor,
    particle_momentum_out: torch.Tensor,
) -> None:
    num_particles = int(particle_position.shape[0])
    if num_particles == 0:
        return

    wp_device, wp_stream = FunctionSpec.warp_launch_context(particle_position)

    wp_position = wp.from_torch(particle_position.contiguous())
    wp_momentum = wp.from_torch(particle_momentum.contiguous())
    wp_electric = wp.from_torch(electric_field.contiguous())
    wp_magnetic = wp.from_torch(magnetic_field.contiguous())
    wp_position_out = wp.from_torch(particle_position_out, return_ctype=True)
    wp_momentum_out = wp.from_torch(particle_momentum_out, return_ctype=True)

    with wp.ScopedStream(wp_stream):
        wp.launch(
            kernel=_particle_push_boris_kernel_scalar_scalar,
            dim=num_particles,
            inputs=[
                wp_position,
                wp_momentum,
                wp_electric,
                wp_magnetic,
                wp_position_out,
                wp_momentum_out,
                float(charge_to_mass),
                float(dt),
            ],
            device=wp_device,
            stream=wp_stream,
        )
