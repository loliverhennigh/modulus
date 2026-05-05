# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ..utils import _as_float3
from ._kernels import (
    _gather_fields_to_particles_kernel_order1_energy_conserving,
    _gather_fields_to_particles_kernel_order1_momentum_conserving,
    _gather_fields_to_particles_kernel_order3_energy_conserving,
    _gather_fields_to_particles_kernel_order3_momentum_conserving,
)


def _launch_warp_forward(
    particle_position: torch.Tensor,
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    origin: torch.Tensor,
    spacing: torch.Tensor,
    electric_stagger: torch.Tensor,
    magnetic_stagger: torch.Tensor,
    shape_order: int,
    gather_mode: str,
    electric_particle: torch.Tensor,
    magnetic_particle: torch.Tensor,
) -> None:
    num_particles = int(particle_position.shape[0])
    if num_particles == 0:
        return

    wp_device, wp_stream = FunctionSpec.warp_launch_context(particle_position)

    origin_values = _as_float3(origin, "origin")
    spacing_values = _as_float3(spacing, "spacing")

    wp_position = wp.from_torch(particle_position.contiguous())
    wp_electric = wp.from_torch(electric_field.contiguous())
    wp_magnetic = wp.from_torch(magnetic_field.contiguous())
    wp_electric_stagger = wp.from_torch(electric_stagger.contiguous())
    wp_magnetic_stagger = wp.from_torch(magnetic_stagger.contiguous())
    wp_electric_particle = wp.from_torch(electric_particle, return_ctype=True)
    wp_magnetic_particle = wp.from_torch(magnetic_particle, return_ctype=True)

    if shape_order == 1:
        if gather_mode == "momentum-conserving":
            kernel = _gather_fields_to_particles_kernel_order1_momentum_conserving
        else:
            kernel = _gather_fields_to_particles_kernel_order1_energy_conserving
    else:
        if gather_mode == "momentum-conserving":
            kernel = _gather_fields_to_particles_kernel_order3_momentum_conserving
        else:
            kernel = _gather_fields_to_particles_kernel_order3_energy_conserving

    with wp.ScopedStream(wp_stream):
        wp.launch(
            kernel=kernel,
            dim=num_particles,
            inputs=[
                wp_position,
                wp_electric,
                wp_magnetic,
                wp_electric_stagger,
                wp_magnetic_stagger,
                wp_electric_particle,
                wp_magnetic_particle,
                wp.vec3f(
                    float(origin_values[0]),
                    float(origin_values[1]),
                    float(origin_values[2]),
                ),
                wp.vec3f(
                    float(spacing_values[0]),
                    float(spacing_values[1]),
                    float(spacing_values[2]),
                ),
            ],
            device=wp_device,
            stream=wp_stream,
        )
