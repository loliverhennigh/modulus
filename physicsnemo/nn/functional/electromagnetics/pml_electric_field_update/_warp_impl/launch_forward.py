# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ._kernels import (
    _pml_electric_field_update_kernel_eps_field,
    _pml_electric_field_update_kernel_scalar,
)


def _launch_warp_forward(
    electric_field: torch.Tensor,
    pml_layer: torch.Tensor,
    eps_field: torch.Tensor | None,
    eps_scalar: float,
    eps_is_scalar: bool,
    spacing: torch.Tensor,
    pml_layer_offset: tuple[int, int, int],
    dt: float,
) -> None:
    wp_device, wp_stream = FunctionSpec.warp_launch_context(electric_field)

    wp_electric = wp.from_torch(electric_field, return_ctype=True)
    wp_pml = wp.from_torch(pml_layer.contiguous())
    wp_spacing = wp.from_torch(spacing.contiguous())
    offset_vec = wp.vec3i(
        int(pml_layer_offset[0]),
        int(pml_layer_offset[1]),
        int(pml_layer_offset[2]),
    )

    with wp.ScopedStream(wp_stream):
        if eps_is_scalar:
            wp.launch(
                kernel=_pml_electric_field_update_kernel_scalar,
                dim=tuple(pml_layer.shape[1:]),
                inputs=[
                    wp_electric,
                    wp_pml,
                    float(eps_scalar),
                    wp_spacing,
                    offset_vec,
                    float(dt),
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        if eps_field is None:
            raise ValueError("eps_field must be provided when eps_is_scalar is False")
        wp_eps = wp.from_torch(eps_field.contiguous())
        wp.launch(
            kernel=_pml_electric_field_update_kernel_eps_field,
            dim=tuple(pml_layer.shape[1:]),
            inputs=[
                wp_electric,
                wp_pml,
                wp_eps,
                wp_spacing,
                offset_vec,
                float(dt),
            ],
            device=wp_device,
            stream=wp_stream,
        )
