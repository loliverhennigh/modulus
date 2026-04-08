# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import torch
import warp as wp

from ._kernels import (
    _rectilinear_derivatives_1d_fused_no_mixed_kernel,
    _rectilinear_derivatives_2d_fused_no_mixed_kernel,
    _rectilinear_derivatives_3d_fused_no_mixed_kernel,
    _rectilinear_gradient_1d_kernel,
    _rectilinear_gradient_2d_kernel,
    _rectilinear_gradient_3d_kernel,
    _rectilinear_second_derivative_1d_kernel,
    _rectilinear_second_derivative_2d_kernel,
    _rectilinear_second_derivative_3d_kernel,
)


def _launch_forward(
    *,
    field_fp32: torch.Tensor,
    coords_tuple: tuple[torch.Tensor, ...],
    period_tuple: tuple[float, ...],
    derivative_order: int,
    grad_components: list[torch.Tensor],
    wp_device,
    wp_stream,
) -> None:
    ### Launch dimensionality-specific forward kernels.
    with wp.ScopedStream(wp_stream):
        if field_fp32.ndim == 1:
            wp.launch(
                kernel=(
                    _rectilinear_gradient_1d_kernel
                    if derivative_order == 1
                    else _rectilinear_second_derivative_1d_kernel
                ),
                dim=field_fp32.shape[0],
                inputs=[
                    wp.from_torch(field_fp32, dtype=wp.float32),
                    wp.from_torch(coords_tuple[0], dtype=wp.float32),
                    float(period_tuple[0]),
                    wp.from_torch(grad_components[0], dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        if field_fp32.ndim == 2:
            wp.launch(
                kernel=(
                    _rectilinear_gradient_2d_kernel
                    if derivative_order == 1
                    else _rectilinear_second_derivative_2d_kernel
                ),
                dim=field_fp32.shape,
                inputs=[
                    wp.from_torch(field_fp32, dtype=wp.float32),
                    wp.from_torch(coords_tuple[0], dtype=wp.float32),
                    wp.from_torch(coords_tuple[1], dtype=wp.float32),
                    float(period_tuple[0]),
                    float(period_tuple[1]),
                    wp.from_torch(grad_components[0], dtype=wp.float32),
                    wp.from_torch(grad_components[1], dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        wp.launch(
            kernel=(
                _rectilinear_gradient_3d_kernel
                if derivative_order == 1
                else _rectilinear_second_derivative_3d_kernel
            ),
            dim=field_fp32.shape,
            inputs=[
                wp.from_torch(field_fp32, dtype=wp.float32),
                wp.from_torch(coords_tuple[0], dtype=wp.float32),
                wp.from_torch(coords_tuple[1], dtype=wp.float32),
                wp.from_torch(coords_tuple[2], dtype=wp.float32),
                float(period_tuple[0]),
                float(period_tuple[1]),
                float(period_tuple[2]),
                wp.from_torch(grad_components[0], dtype=wp.float32),
                wp.from_torch(grad_components[1], dtype=wp.float32),
                wp.from_torch(grad_components[2], dtype=wp.float32),
            ],
            device=wp_device,
            stream=wp_stream,
        )


def _launch_forward_fused_no_mixed(
    *,
    field_fp32: torch.Tensor,
    coords_tuple: tuple[torch.Tensor, ...],
    period_tuple: tuple[float, ...],
    first_components: list[torch.Tensor],
    second_components: list[torch.Tensor],
    wp_device,
    wp_stream,
) -> None:
    """Launch dimensionality-specific fused first+second derivative kernels."""
    with wp.ScopedStream(wp_stream):
        if field_fp32.ndim == 1:
            wp.launch(
                kernel=_rectilinear_derivatives_1d_fused_no_mixed_kernel,
                dim=field_fp32.shape[0],
                inputs=[
                    wp.from_torch(field_fp32, dtype=wp.float32),
                    wp.from_torch(coords_tuple[0], dtype=wp.float32),
                    float(period_tuple[0]),
                    wp.from_torch(first_components[0], dtype=wp.float32),
                    wp.from_torch(second_components[0], dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        if field_fp32.ndim == 2:
            wp.launch(
                kernel=_rectilinear_derivatives_2d_fused_no_mixed_kernel,
                dim=field_fp32.shape,
                inputs=[
                    wp.from_torch(field_fp32, dtype=wp.float32),
                    wp.from_torch(coords_tuple[0], dtype=wp.float32),
                    wp.from_torch(coords_tuple[1], dtype=wp.float32),
                    float(period_tuple[0]),
                    float(period_tuple[1]),
                    wp.from_torch(first_components[0], dtype=wp.float32),
                    wp.from_torch(first_components[1], dtype=wp.float32),
                    wp.from_torch(second_components[0], dtype=wp.float32),
                    wp.from_torch(second_components[1], dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        wp.launch(
            kernel=_rectilinear_derivatives_3d_fused_no_mixed_kernel,
            dim=field_fp32.shape,
            inputs=[
                wp.from_torch(field_fp32, dtype=wp.float32),
                wp.from_torch(coords_tuple[0], dtype=wp.float32),
                wp.from_torch(coords_tuple[1], dtype=wp.float32),
                wp.from_torch(coords_tuple[2], dtype=wp.float32),
                float(period_tuple[0]),
                float(period_tuple[1]),
                float(period_tuple[2]),
                wp.from_torch(first_components[0], dtype=wp.float32),
                wp.from_torch(first_components[1], dtype=wp.float32),
                wp.from_torch(first_components[2], dtype=wp.float32),
                wp.from_torch(second_components[0], dtype=wp.float32),
                wp.from_torch(second_components[1], dtype=wp.float32),
                wp.from_torch(second_components[2], dtype=wp.float32),
            ],
            device=wp_device,
            stream=wp_stream,
        )
