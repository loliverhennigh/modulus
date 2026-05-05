# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ..utils import _as_float3
from ._kernels import (
    _deposit_current_charge_conserving_kernel_jx_scalar_scalar,
    _deposit_current_charge_conserving_kernel_jx_shape3_scalar_scalar,
    _deposit_current_charge_conserving_kernel_jy_scalar_scalar,
    _deposit_current_charge_conserving_kernel_jy_shape3_scalar_scalar,
    _deposit_current_charge_conserving_kernel_jz_scalar_scalar,
    _deposit_current_charge_conserving_kernel_jz_shape3_scalar_scalar,
)


def _launch_warp_forward(
    particle_position_old: torch.Tensor,
    particle_position_new: torch.Tensor,
    particle_weight: torch.Tensor,
    particle_charge: float,
    dt: float,
    origin: torch.Tensor,
    spacing: torch.Tensor,
    current_stagger: torch.Tensor,
    current_density: torch.Tensor,
    shape_order: int = 1,
) -> None:
    if shape_order == 1:
        kernel_jx = _deposit_current_charge_conserving_kernel_jx_scalar_scalar
        kernel_jy = _deposit_current_charge_conserving_kernel_jy_scalar_scalar
        kernel_jz = _deposit_current_charge_conserving_kernel_jz_scalar_scalar
    elif shape_order == 3:
        kernel_jx = _deposit_current_charge_conserving_kernel_jx_shape3_scalar_scalar
        kernel_jy = _deposit_current_charge_conserving_kernel_jy_shape3_scalar_scalar
        kernel_jz = _deposit_current_charge_conserving_kernel_jz_shape3_scalar_scalar
    else:
        raise ValueError("warp launch supports shape_order 1 or 3")

    num_particles = int(particle_position_old.shape[0])
    if num_particles == 0:
        return

    wp_device, wp_stream = FunctionSpec.warp_launch_context(particle_position_old)

    origin_values = _as_float3(origin, "origin")
    spacing_values = _as_float3(spacing, "spacing")

    wp_position_old = wp.from_torch(particle_position_old.contiguous())
    wp_position_new = wp.from_torch(particle_position_new.contiguous())
    wp_particle_weight = wp.from_torch(particle_weight.contiguous())
    wp_current_stagger = wp.from_torch(current_stagger.contiguous())
    wp_current_density = wp.from_torch(current_density, return_ctype=True)
    common_inputs = [
        wp_position_old,
        wp_position_new,
        wp_particle_weight,
        wp_current_stagger,
        wp_current_density,
        float(particle_charge),
        float(dt),
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
    ]

    with wp.ScopedStream(wp_stream):
        wp.launch(
            kernel=kernel_jx,
            dim=num_particles,
            inputs=common_inputs,
            device=wp_device,
            stream=wp_stream,
        )
        wp.launch(
            kernel=kernel_jy,
            dim=num_particles,
            inputs=common_inputs,
            device=wp_device,
            stream=wp_stream,
        )
        wp.launch(
            kernel=kernel_jz,
            dim=num_particles,
            inputs=common_inputs,
            device=wp_device,
            stream=wp_stream,
        )
