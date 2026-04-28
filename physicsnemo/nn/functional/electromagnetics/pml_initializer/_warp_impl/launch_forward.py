# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ._kernels import _pml_initializer_kernel


def _launch_warp_forward(
    pml_layer: torch.Tensor,
    direction: torch.Tensor,
    thickness: int,
    courant_number: float,
    kappa: float,
    a: float,
) -> None:
    wp_device, wp_stream = FunctionSpec.warp_launch_context(pml_layer)
    wp_pml_layer = wp.from_torch(pml_layer, return_ctype=True)

    direction_values = direction.detach().cpu().flatten().tolist()
    direction_vec = wp.vec3f(
        float(direction_values[0]),
        float(direction_values[1]),
        float(direction_values[2]),
    )

    with wp.ScopedStream(wp_stream):
        wp.launch(
            kernel=_pml_initializer_kernel,
            dim=tuple(pml_layer.shape[1:]),
            inputs=[
                wp_pml_layer,
                direction_vec,
                int(thickness),
                float(courant_number),
                float(kappa),
                float(a),
            ],
            device=wp_device,
            stream=wp_stream,
        )
