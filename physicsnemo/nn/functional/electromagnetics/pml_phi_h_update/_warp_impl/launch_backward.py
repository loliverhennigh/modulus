# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ._kernels import _pml_phi_h_update_backward_kernel


def _launch_warp_backward(
    electric_field: torch.Tensor,
    pml_layer: torch.Tensor,
    grad_output: torch.Tensor,
    pml_layer_offset: tuple[int, int, int],
    needs_input_grad: tuple[bool, ...],
) -> tuple[torch.Tensor | None, torch.Tensor | None]:
    need_grad_electric = needs_input_grad[0]
    need_grad_pml = needs_input_grad[1]

    if not (need_grad_electric or need_grad_pml):
        return None, None

    device = pml_layer.device
    empty4 = torch.empty((3, 0, 0, 0), device=device, dtype=torch.float32)
    empty_pml = torch.empty((36, 0, 0, 0), device=device, dtype=torch.float32)

    grad_electric = (
        torch.zeros_like(electric_field, dtype=torch.float32) if need_grad_electric else empty4
    )

    if need_grad_pml:
        grad_pml = grad_output.clone()
        grad_pml[3:6].zero_()
        grad_pml[15:24].zero_()
    else:
        grad_pml = empty_pml

    wp_device, wp_stream = FunctionSpec.warp_launch_context(pml_layer)

    wp_electric = wp.from_torch(electric_field.contiguous())
    wp_pml = wp.from_torch(pml_layer.contiguous())
    wp_grad_output = wp.from_torch(grad_output.contiguous())
    wp_grad_electric = wp.from_torch(grad_electric, return_ctype=True)
    wp_grad_pml = wp.from_torch(grad_pml, return_ctype=True)

    offset_vec = wp.vec3i(
        int(pml_layer_offset[0]),
        int(pml_layer_offset[1]),
        int(pml_layer_offset[2]),
    )

    wp.launch(
        kernel=_pml_phi_h_update_backward_kernel,
        dim=tuple(pml_layer.shape[1:]),
        inputs=[
            wp_electric,
            wp_pml,
            wp_grad_output,
            wp_grad_electric,
            wp_grad_pml,
            offset_vec,
            int(need_grad_electric),
            int(need_grad_pml),
        ],
        device=wp_device,
        stream=wp_stream,
    )

    return (
        grad_electric if need_grad_electric else None,
        grad_pml if need_grad_pml else None,
    )
