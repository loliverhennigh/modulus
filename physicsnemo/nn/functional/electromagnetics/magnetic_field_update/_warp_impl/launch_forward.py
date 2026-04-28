# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ..utils import _normalize_material_field
from ._kernels import (
    _magnetic_field_update_kernel_mu_field_scalar,
    _magnetic_field_update_kernel_mu_field_sigma_field,
    _magnetic_field_update_kernel_scalar_scalar,
    _magnetic_field_update_kernel_scalar_sigma_field,
)


def _launch_warp_forward(
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    mu: float | torch.Tensor,
    sigma_m: float | torch.Tensor,
    spacing: torch.Tensor,
    dt: float,
    output: torch.Tensor,
) -> None:
    nx, ny, nz = magnetic_field.shape[1:]
    dim = (nx, ny, nz)

    wp_device, wp_stream = FunctionSpec.warp_launch_context(magnetic_field)

    wp_electric = wp.from_torch(electric_field.contiguous())
    wp_magnetic = wp.from_torch(magnetic_field.contiguous())
    wp_output = wp.from_torch(output, return_ctype=True)
    wp_spacing = wp.from_torch(spacing.contiguous())

    mu_is_scalar = isinstance(mu, (int, float))
    sigma_is_scalar = isinstance(sigma_m, (int, float))

    with wp.ScopedStream(wp_stream):
        if mu_is_scalar and sigma_is_scalar:
            wp.launch(
                kernel=_magnetic_field_update_kernel_scalar_scalar,
                dim=dim,
                inputs=[
                    wp_electric,
                    wp_magnetic,
                    wp_output,
                    float(mu),
                    float(sigma_m),
                    float(dt),
                    wp_spacing,
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        if mu_is_scalar and not sigma_is_scalar:
            sigma_field = _normalize_material_field(
                sigma_m,
                "sigma_m",
                (nx, ny, nz),
                magnetic_field.device,
            )
            wp_sigma = wp.from_torch(sigma_field.contiguous())
            wp.launch(
                kernel=_magnetic_field_update_kernel_scalar_sigma_field,
                dim=dim,
                inputs=[
                    wp_electric,
                    wp_magnetic,
                    wp_sigma,
                    wp_output,
                    float(mu),
                    float(dt),
                    wp_spacing,
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        if not mu_is_scalar and sigma_is_scalar:
            mu_field = _normalize_material_field(
                mu,
                "mu",
                (nx, ny, nz),
                magnetic_field.device,
            )
            wp_mu = wp.from_torch(mu_field.contiguous())
            wp.launch(
                kernel=_magnetic_field_update_kernel_mu_field_scalar,
                dim=dim,
                inputs=[
                    wp_electric,
                    wp_magnetic,
                    wp_mu,
                    wp_output,
                    float(sigma_m),
                    float(dt),
                    wp_spacing,
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        mu_field = _normalize_material_field(
            mu,
            "mu",
            (nx, ny, nz),
            magnetic_field.device,
        )
        sigma_field = _normalize_material_field(
            sigma_m,
            "sigma_m",
            (nx, ny, nz),
            magnetic_field.device,
        )
        wp_mu = wp.from_torch(mu_field.contiguous())
        wp_sigma = wp.from_torch(sigma_field.contiguous())
        wp.launch(
            kernel=_magnetic_field_update_kernel_mu_field_sigma_field,
            dim=dim,
            inputs=[
                wp_electric,
                wp_magnetic,
                wp_mu,
                wp_sigma,
                wp_output,
                float(dt),
                wp_spacing,
            ],
            device=wp_device,
            stream=wp_stream,
        )
