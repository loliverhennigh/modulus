# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ._kernels import (
    _pml_electric_field_update_backward_kernel_eps_field,
    _pml_electric_field_update_backward_kernel_scalar,
)


def _launch_warp_backward(
    pml_layer: torch.Tensor,
    eps_field: torch.Tensor,
    eps_scalar: float,
    eps_is_scalar: bool,
    grad_output: torch.Tensor,
    spacing: torch.Tensor,
    pml_layer_offset: tuple[int, int, int],
    dt: float,
    needs_input_grad: tuple[bool, ...],
) -> tuple[torch.Tensor | None, torch.Tensor | None]:
    need_grad_pml = needs_input_grad[1]
    need_grad_eps = (not eps_is_scalar) and needs_input_grad[2]

    if not (need_grad_pml or need_grad_eps):
        return None, None

    device = pml_layer.device
    empty4 = torch.empty((36, 0, 0, 0), device=device, dtype=torch.float32)
    empty3 = torch.empty((0, 0, 0), device=device, dtype=torch.float32)

    grad_pml = torch.zeros_like(pml_layer, dtype=torch.float32) if need_grad_pml else empty4
    grad_eps = torch.zeros_like(eps_field, dtype=torch.float32) if need_grad_eps else empty3

    wp_device, wp_stream = FunctionSpec.warp_launch_context(pml_layer)

    wp_pml = wp.from_torch(pml_layer.contiguous())
    wp_grad_output = wp.from_torch(grad_output.contiguous())
    wp_grad_pml = wp.from_torch(grad_pml, return_ctype=True)
    wp_spacing = wp.from_torch(spacing.contiguous())
    offset_vec = wp.vec3i(
        int(pml_layer_offset[0]),
        int(pml_layer_offset[1]),
        int(pml_layer_offset[2]),
    )

    if eps_is_scalar:
        wp.launch(
            kernel=_pml_electric_field_update_backward_kernel_scalar,
            dim=tuple(pml_layer.shape[1:]),
            inputs=[
                wp_pml,
                wp_grad_output,
                wp_grad_pml,
                wp_spacing,
                offset_vec,
                float(dt),
                float(eps_scalar),
                int(need_grad_pml),
            ],
            device=wp_device,
            stream=wp_stream,
        )
        return (
            grad_pml if need_grad_pml else None,
            None,
        )

    wp_eps = wp.from_torch(eps_field.contiguous())
    wp_grad_eps = wp.from_torch(grad_eps, return_ctype=True)
    wp.launch(
        kernel=_pml_electric_field_update_backward_kernel_eps_field,
        dim=tuple(pml_layer.shape[1:]),
        inputs=[
            wp_pml,
            wp_eps,
            wp_grad_output,
            wp_grad_pml,
            wp_grad_eps,
            wp_spacing,
            offset_vec,
            float(dt),
            int(need_grad_pml),
            int(need_grad_eps),
        ],
        device=wp_device,
        stream=wp_stream,
    )

    return (
        grad_pml if need_grad_pml else None,
        grad_eps if need_grad_eps else None,
    )
