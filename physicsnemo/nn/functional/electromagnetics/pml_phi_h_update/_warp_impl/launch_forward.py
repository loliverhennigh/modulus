# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ._kernels import _pml_phi_h_update_kernel


def _launch_warp_forward(
    electric_field: torch.Tensor,
    pml_layer_in: torch.Tensor,
    pml_layer_out: torch.Tensor,
    pml_layer_offset: tuple[int, int, int],
) -> None:
    wp_device, wp_stream = FunctionSpec.warp_launch_context(pml_layer_out)

    wp_electric = wp.from_torch(electric_field.contiguous())
    wp_pml_in = wp.from_torch(pml_layer_in.contiguous())
    wp_pml_out = wp.from_torch(pml_layer_out, return_ctype=True)

    offset_vec = wp.vec3i(
        int(pml_layer_offset[0]),
        int(pml_layer_offset[1]),
        int(pml_layer_offset[2]),
    )

    with wp.ScopedStream(wp_stream):
        wp.launch(
            kernel=_pml_phi_h_update_kernel,
            dim=tuple(pml_layer_out.shape[1:]),
            inputs=[wp_electric, wp_pml_in, wp_pml_out, offset_vec],
            device=wp_device,
            stream=wp_stream,
        )
