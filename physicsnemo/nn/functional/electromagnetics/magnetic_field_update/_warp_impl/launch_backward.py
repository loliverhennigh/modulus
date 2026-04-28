# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ._kernels import (
    _magnetic_field_update_backward_kernel_fields,
    _magnetic_field_update_backward_kernel_full,
)


def _launch_warp_backward(
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    mu_field: torch.Tensor,
    sigma_m_field: torch.Tensor,
    spacing: torch.Tensor,
    grad_output: torch.Tensor,
    dt: float,
    mu_scalar: float,
    sigma_m_scalar: float,
    mu_is_scalar: bool,
    sigma_is_scalar: bool,
    needs_input_grad: tuple[bool, ...],
) -> tuple[
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
]:
    # Backward groups cases by requested gradients (fields-only vs full material)
    # instead of mirroring every forward material mode 1:1.
    need_grad_electric = needs_input_grad[0]
    need_grad_magnetic = needs_input_grad[1]
    need_grad_mu = (not mu_is_scalar) and needs_input_grad[2]
    need_grad_sigma = (not sigma_is_scalar) and needs_input_grad[3]

    if not any((need_grad_electric, need_grad_magnetic, need_grad_mu, need_grad_sigma)):
        return None, None, None, None

    device = electric_field.device
    dim = tuple(magnetic_field.shape[1:])

    empty3 = torch.empty((0, 0, 0), device=device, dtype=torch.float32)

    grad_electric = torch.zeros_like(electric_field, dtype=torch.float32)
    grad_magnetic = torch.zeros_like(magnetic_field, dtype=torch.float32)
    grad_mu = torch.zeros_like(mu_field, dtype=torch.float32) if need_grad_mu else empty3
    grad_sigma = (
        torch.zeros_like(sigma_m_field, dtype=torch.float32) if need_grad_sigma else empty3
    )

    wp_device, wp_stream = FunctionSpec.warp_launch_context(electric_field)

    wp_electric = wp.from_torch(electric_field.contiguous())
    wp_magnetic = wp.from_torch(magnetic_field.contiguous())
    wp_mu = wp.from_torch(mu_field.contiguous())
    wp_sigma = wp.from_torch(sigma_m_field.contiguous())
    wp_grad_output = wp.from_torch(grad_output.contiguous())
    wp_grad_electric = wp.from_torch(grad_electric, return_ctype=True)
    wp_grad_magnetic = wp.from_torch(grad_magnetic, return_ctype=True)
    wp_grad_mu = wp.from_torch(grad_mu, return_ctype=True)
    wp_grad_sigma = wp.from_torch(grad_sigma, return_ctype=True)
    wp_spacing = wp.from_torch(spacing.contiguous())

    mu_is_scalar_flag = int(mu_is_scalar)
    sigma_is_scalar_flag = int(sigma_is_scalar)
    dt_value = float(dt)

    if need_grad_mu or need_grad_sigma:
        wp.launch(
            kernel=_magnetic_field_update_backward_kernel_full,
            dim=dim,
            inputs=[
                wp_electric,
                wp_magnetic,
                wp_mu,
                wp_sigma,
                wp_grad_output,
                wp_grad_electric,
                wp_grad_magnetic,
                wp_grad_mu,
                wp_grad_sigma,
                float(mu_scalar),
                float(sigma_m_scalar),
                mu_is_scalar_flag,
                sigma_is_scalar_flag,
                dt_value,
                wp_spacing,
                int(need_grad_mu),
                int(need_grad_sigma),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    else:
        wp.launch(
            kernel=_magnetic_field_update_backward_kernel_fields,
            dim=dim,
            inputs=[
                wp_electric,
                wp_magnetic,
                wp_mu,
                wp_sigma,
                wp_grad_output,
                wp_grad_electric,
                wp_grad_magnetic,
                float(mu_scalar),
                float(sigma_m_scalar),
                mu_is_scalar_flag,
                sigma_is_scalar_flag,
                dt_value,
                wp_spacing,
            ],
            device=wp_device,
            stream=wp_stream,
        )

    return (
        grad_electric if need_grad_electric else None,
        grad_magnetic if need_grad_magnetic else None,
        grad_mu if need_grad_mu else None,
        grad_sigma if need_grad_sigma else None,
    )
