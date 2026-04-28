# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ..utils import _normalize_material_field
from ._kernels import (
    _electric_field_update_kernel_eps_field_scalar,
    _electric_field_update_kernel_eps_field_sigma_field,
    _electric_field_update_kernel_scalar_scalar,
    _electric_field_update_kernel_scalar_sigma_field,
)


def _launch_warp_forward(
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    eps: float | torch.Tensor,
    sigma_e: float | torch.Tensor,
    spacing: torch.Tensor,
    dt: float,
    impressed_current: torch.Tensor,
    impressed_current_offset: tuple[int, int, int],
    output: torch.Tensor,
) -> None:
    # Resolve launch geometry from the electric grid.
    nx, ny, nz = electric_field.shape[1:]
    dim = (nx, ny, nz)

    # Build a warp launch context from the input tensor device/stream.
    wp_device, wp_stream = FunctionSpec.warp_launch_context(electric_field)

    # Convert common tensors once; these are reused across all material branches.
    wp_electric = wp.from_torch(electric_field.contiguous())
    wp_magnetic = wp.from_torch(magnetic_field.contiguous())
    wp_current = wp.from_torch(impressed_current.contiguous())
    wp_output = wp.from_torch(output, return_ctype=True)
    wp_spacing = wp.from_torch(spacing.contiguous())

    offset_x, offset_y, offset_z = impressed_current_offset
    current_x = int(impressed_current.shape[1])
    current_y = int(impressed_current.shape[2])
    current_z = int(impressed_current.shape[3])
    dt_value = float(dt)

    # Select the forward kernel variant from scalar/field material modes.
    eps_is_scalar = isinstance(eps, (int, float))
    sigma_is_scalar = isinstance(sigma_e, (int, float))

    with wp.ScopedStream(wp_stream):
        # Fast path: both materials are scalars.
        if eps_is_scalar and sigma_is_scalar:
            wp.launch(
                kernel=_electric_field_update_kernel_scalar_scalar,
                dim=dim,
                inputs=[
                    wp_electric,
                    wp_magnetic,
                    wp_current,
                    wp_output,
                    float(eps),
                    float(sigma_e),
                    dt_value,
                    wp_spacing,
                    offset_x,
                    offset_y,
                    offset_z,
                    current_x,
                    current_y,
                    current_z,
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        # Mixed path: scalar eps + spatial sigma.
        if eps_is_scalar and not sigma_is_scalar:
            sigma_field = _normalize_material_field(
                sigma_e,
                "sigma_e",
                (nx, ny, nz),
                electric_field.device,
            )
            wp_sigma = wp.from_torch(sigma_field.contiguous())
            wp.launch(
                kernel=_electric_field_update_kernel_scalar_sigma_field,
                dim=dim,
                inputs=[
                    wp_electric,
                    wp_magnetic,
                    wp_sigma,
                    wp_current,
                    wp_output,
                    float(eps),
                    dt_value,
                    wp_spacing,
                    offset_x,
                    offset_y,
                    offset_z,
                    current_x,
                    current_y,
                    current_z,
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        # Mixed path: spatial eps + scalar sigma.
        if not eps_is_scalar and sigma_is_scalar:
            eps_field = _normalize_material_field(
                eps,
                "eps",
                (nx, ny, nz),
                electric_field.device,
            )
            wp_eps = wp.from_torch(eps_field.contiguous())
            wp.launch(
                kernel=_electric_field_update_kernel_eps_field_scalar,
                dim=dim,
                inputs=[
                    wp_electric,
                    wp_magnetic,
                    wp_eps,
                    wp_current,
                    wp_output,
                    float(sigma_e),
                    dt_value,
                    wp_spacing,
                    offset_x,
                    offset_y,
                    offset_z,
                    current_x,
                    current_y,
                    current_z,
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        # General path: both materials are spatial fields.
        eps_field = _normalize_material_field(
            eps,
            "eps",
            (nx, ny, nz),
            electric_field.device,
        )
        sigma_field = _normalize_material_field(
            sigma_e,
            "sigma_e",
            (nx, ny, nz),
            electric_field.device,
        )
        wp_eps = wp.from_torch(eps_field.contiguous())
        wp_sigma = wp.from_torch(sigma_field.contiguous())
        wp.launch(
            kernel=_electric_field_update_kernel_eps_field_sigma_field,
            dim=dim,
            inputs=[
                wp_electric,
                wp_magnetic,
                wp_eps,
                wp_sigma,
                wp_current,
                wp_output,
                dt_value,
                wp_spacing,
                offset_x,
                offset_y,
                offset_z,
                current_x,
                current_y,
                current_z,
            ],
            device=wp_device,
            stream=wp_stream,
        )
