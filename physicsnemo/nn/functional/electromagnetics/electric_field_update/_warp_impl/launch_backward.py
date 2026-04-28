# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ._kernels import (
    _electric_field_update_backward_kernel_fields_no_current,
    _electric_field_update_backward_kernel_fields_with_current,
    _electric_field_update_backward_kernel_full_no_current,
    _electric_field_update_backward_kernel_full_with_current,
)


def _launch_warp_backward(
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    eps_field: torch.Tensor,
    sigma_e_field: torch.Tensor,
    impressed_current: torch.Tensor,
    grad_output: torch.Tensor,
    spacing: torch.Tensor,
    dt: float,
    eps_scalar: float,
    sigma_e_scalar: float,
    eps_is_scalar: bool,
    sigma_is_scalar: bool,
    impressed_current_offset: torch.Tensor,
    needs_input_grad: tuple[bool, ...],
) -> tuple[
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
]:
    # Decode autograd requirements for each differentiable input.
    need_grad_electric = needs_input_grad[0]
    need_grad_magnetic = needs_input_grad[1]
    need_grad_eps = (not eps_is_scalar) and needs_input_grad[2]
    need_grad_sigma = (not sigma_is_scalar) and needs_input_grad[3]
    need_grad_current = needs_input_grad[10] and impressed_current.numel() > 0

    # Exit early when autograd does not request any gradients from this op.
    if not any(
        (
            need_grad_electric,
            need_grad_magnetic,
            need_grad_eps,
            need_grad_sigma,
            need_grad_current,
        )
    ):
        return None, None, None, None, None

    # Allocate output gradient buffers (empty placeholders for disabled grads).
    device = electric_field.device
    spatial_shape = tuple(electric_field.shape[1:])
    dim = spatial_shape

    empty4 = torch.empty((3, 0, 0, 0), device=device, dtype=torch.float32)
    empty3 = torch.empty((0, 0, 0), device=device, dtype=torch.float32)

    # Materialize E/H gradient buffers unconditionally so specialized kernels
    # can write without per-element write guards.
    grad_electric = torch.zeros_like(electric_field, dtype=torch.float32)
    grad_magnetic = torch.zeros_like(magnetic_field, dtype=torch.float32)
    grad_eps = (
        torch.zeros_like(eps_field, dtype=torch.float32) if need_grad_eps else empty3
    )
    grad_sigma = (
        torch.zeros_like(sigma_e_field, dtype=torch.float32)
        if need_grad_sigma
        else empty3
    )
    grad_current = (
        torch.zeros_like(impressed_current, dtype=torch.float32)
        if need_grad_current
        else empty4
    )

    # Build the warp launch context and normalize offset metadata.
    wp_device, wp_stream = FunctionSpec.warp_launch_context(electric_field)

    offset_x, offset_y, offset_z = tuple(
        int(v) for v in impressed_current_offset.detach().cpu().flatten().tolist()
    )
    use_current_input = int(impressed_current.numel() > 0)
    has_material_grads = need_grad_eps or need_grad_sigma

    # Convert all candidate tensors once; specialized kernels reuse these handles.
    wp_electric = wp.from_torch(electric_field.contiguous())
    wp_magnetic = wp.from_torch(magnetic_field.contiguous())
    wp_eps = wp.from_torch(eps_field.contiguous())
    wp_sigma = wp.from_torch(sigma_e_field.contiguous())
    wp_current = wp.from_torch(impressed_current.contiguous())
    wp_grad_output = wp.from_torch(grad_output.contiguous())
    wp_grad_electric = wp.from_torch(grad_electric, return_ctype=True)
    wp_grad_magnetic = wp.from_torch(grad_magnetic, return_ctype=True)
    wp_grad_eps = wp.from_torch(grad_eps, return_ctype=True)
    wp_grad_sigma = wp.from_torch(grad_sigma, return_ctype=True)
    wp_grad_current = wp.from_torch(grad_current, return_ctype=True)
    wp_spacing = wp.from_torch(spacing.contiguous())

    eps_scalar_value = float(eps_scalar)
    sigma_scalar_value = float(sigma_e_scalar)
    eps_is_scalar_flag = int(eps_is_scalar)
    sigma_is_scalar_flag = int(sigma_is_scalar)
    dt_value = float(dt)

    # Dispatch one specialized backward kernel for the requested gradient mode.
    if has_material_grads and use_current_input == 1:
        wp.launch(
            kernel=_electric_field_update_backward_kernel_full_with_current,
            dim=dim,
            inputs=[
                wp_electric,
                wp_magnetic,
                wp_eps,
                wp_sigma,
                wp_current,
                wp_grad_output,
                wp_grad_electric,
                wp_grad_magnetic,
                wp_grad_eps,
                wp_grad_sigma,
                wp_grad_current,
                eps_scalar_value,
                sigma_scalar_value,
                eps_is_scalar_flag,
                sigma_is_scalar_flag,
                dt_value,
                wp_spacing,
                offset_x,
                offset_y,
                offset_z,
                int(impressed_current.shape[1]),
                int(impressed_current.shape[2]),
                int(impressed_current.shape[3]),
                int(need_grad_eps),
                int(need_grad_sigma),
                int(need_grad_current),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    elif has_material_grads and use_current_input == 0:
        wp.launch(
            kernel=_electric_field_update_backward_kernel_full_no_current,
            dim=dim,
            inputs=[
                wp_electric,
                wp_magnetic,
                wp_eps,
                wp_sigma,
                wp_grad_output,
                wp_grad_electric,
                wp_grad_magnetic,
                wp_grad_eps,
                wp_grad_sigma,
                eps_scalar_value,
                sigma_scalar_value,
                eps_is_scalar_flag,
                sigma_is_scalar_flag,
                dt_value,
                wp_spacing,
                int(need_grad_eps),
                int(need_grad_sigma),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    elif not has_material_grads and use_current_input == 1:
        wp.launch(
            kernel=_electric_field_update_backward_kernel_fields_with_current,
            dim=dim,
            inputs=[
                wp_electric,
                wp_magnetic,
                wp_eps,
                wp_sigma,
                wp_current,
                wp_grad_output,
                wp_grad_electric,
                wp_grad_magnetic,
                wp_grad_current,
                eps_scalar_value,
                sigma_scalar_value,
                eps_is_scalar_flag,
                sigma_is_scalar_flag,
                dt_value,
                wp_spacing,
                offset_x,
                offset_y,
                offset_z,
                int(impressed_current.shape[1]),
                int(impressed_current.shape[2]),
                int(impressed_current.shape[3]),
                int(need_grad_current),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    else:
        wp.launch(
            kernel=_electric_field_update_backward_kernel_fields_no_current,
            dim=dim,
            inputs=[
                wp_electric,
                wp_magnetic,
                wp_eps,
                wp_sigma,
                wp_grad_output,
                wp_grad_electric,
                wp_grad_magnetic,
                eps_scalar_value,
                sigma_scalar_value,
                eps_is_scalar_flag,
                sigma_is_scalar_flag,
                dt_value,
                wp_spacing,
            ],
            device=wp_device,
            stream=wp_stream,
        )

    # Return only gradients requested by autograd.
    return (
        grad_electric if need_grad_electric else None,
        grad_magnetic if need_grad_magnetic else None,
        grad_eps if need_grad_eps else None,
        grad_sigma if need_grad_sigma else None,
        grad_current if need_grad_current else None,
    )
