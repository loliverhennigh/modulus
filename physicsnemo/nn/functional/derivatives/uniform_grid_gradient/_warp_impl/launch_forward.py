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
    _uniform_grid_derivatives_1d_order2_fused_kernel,
    _uniform_grid_derivatives_2d_order2_fused_kernel,
    _uniform_grid_derivatives_2d_order2_fused_no_mixed_kernel,
    _uniform_grid_derivatives_3d_order2_fused_kernel,
    _uniform_grid_derivatives_3d_order2_fused_no_mixed_kernel,
    _uniform_grid_gradient_1d_kernel,
    _uniform_grid_gradient_1d_order4_kernel,
    _uniform_grid_gradient_2d_kernel,
    _uniform_grid_gradient_2d_order4_kernel,
    _uniform_grid_gradient_3d_kernel,
    _uniform_grid_gradient_3d_order4_kernel,
    _uniform_grid_second_derivative_1d_kernel,
    _uniform_grid_second_derivative_1d_order4_kernel,
    _uniform_grid_second_derivative_2d_kernel,
    _uniform_grid_second_derivative_2d_order4_kernel,
    _uniform_grid_second_derivative_3d_kernel,
    _uniform_grid_second_derivative_3d_order4_kernel,
)
from .utils import _wp_launch


def _launch_forward(
    *,
    field_fp32: torch.Tensor,
    spacing_tuple: tuple[float, ...],
    order: int,
    derivative_order: int,
    grad_components: list[torch.Tensor],
    wp_device,
    wp_stream,
) -> None:
    ### Launch dimensionality/order-specific forward kernels.
    with wp.ScopedStream(wp_stream):
        if field_fp32.ndim == 1:
            wp_field = wp.from_torch(field_fp32, dtype=wp.float32)
            wp_grad0 = wp.from_torch(grad_components[0], dtype=wp.float32)
            if derivative_order == 1:
                inv_dx0 = 1.0 / float(spacing_tuple[0])
                kernel = (
                    _uniform_grid_gradient_1d_kernel
                    if order == 2
                    else _uniform_grid_gradient_1d_order4_kernel
                )
            else:
                inv_dx0 = 1.0 / float(spacing_tuple[0] * spacing_tuple[0])
                kernel = (
                    _uniform_grid_second_derivative_1d_kernel
                    if order == 2
                    else _uniform_grid_second_derivative_1d_order4_kernel
                )
            _wp_launch(
                kernel=kernel,
                dim=field_fp32.shape[0],
                inputs=[wp_field, inv_dx0, wp_grad0],
                device=wp_device,
                stream=wp_stream,
            )
            return

        if field_fp32.ndim == 2:
            wp_field = wp.from_torch(field_fp32, dtype=wp.float32)
            wp_grad0 = wp.from_torch(grad_components[0], dtype=wp.float32)
            wp_grad1 = wp.from_torch(grad_components[1], dtype=wp.float32)
            if derivative_order == 1:
                inv_dx0 = 1.0 / float(spacing_tuple[0])
                inv_dx1 = 1.0 / float(spacing_tuple[1])
                kernel = (
                    _uniform_grid_gradient_2d_kernel
                    if order == 2
                    else _uniform_grid_gradient_2d_order4_kernel
                )
            else:
                inv_dx0 = 1.0 / float(spacing_tuple[0] * spacing_tuple[0])
                inv_dx1 = 1.0 / float(spacing_tuple[1] * spacing_tuple[1])
                kernel = (
                    _uniform_grid_second_derivative_2d_kernel
                    if order == 2
                    else _uniform_grid_second_derivative_2d_order4_kernel
                )
            _wp_launch(
                kernel=kernel,
                dim=field_fp32.shape,
                inputs=[
                    wp_field,
                    inv_dx0,
                    inv_dx1,
                    wp_grad0,
                    wp_grad1,
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        wp_field = wp.from_torch(field_fp32, dtype=wp.float32)
        wp_grad0 = wp.from_torch(grad_components[0], dtype=wp.float32)
        wp_grad1 = wp.from_torch(grad_components[1], dtype=wp.float32)
        wp_grad2 = wp.from_torch(grad_components[2], dtype=wp.float32)
        if derivative_order == 1:
            inv_dx0 = 1.0 / float(spacing_tuple[0])
            inv_dx1 = 1.0 / float(spacing_tuple[1])
            inv_dx2 = 1.0 / float(spacing_tuple[2])
            kernel = (
                _uniform_grid_gradient_3d_kernel
                if order == 2
                else _uniform_grid_gradient_3d_order4_kernel
            )
        else:
            inv_dx0 = 1.0 / float(spacing_tuple[0] * spacing_tuple[0])
            inv_dx1 = 1.0 / float(spacing_tuple[1] * spacing_tuple[1])
            inv_dx2 = 1.0 / float(spacing_tuple[2] * spacing_tuple[2])
            kernel = (
                _uniform_grid_second_derivative_3d_kernel
                if order == 2
                else _uniform_grid_second_derivative_3d_order4_kernel
            )
        _wp_launch(
            kernel=kernel,
            dim=field_fp32.shape,
            inputs=[
                wp_field,
                inv_dx0,
                inv_dx1,
                inv_dx2,
                wp_grad0,
                wp_grad1,
                wp_grad2,
            ],
            device=wp_device,
            stream=wp_stream,
        )


def _launch_forward_fused_order2(
    *,
    field_fp32: torch.Tensor,
    spacing_tuple: tuple[float, ...],
    first_components: list[torch.Tensor],
    second_components: list[torch.Tensor],
    mixed_components: list[torch.Tensor],
    include_mixed: bool,
    wp_device,
    wp_stream,
) -> None:
    """Launch fused first/second/mixed derivative kernels (order=2 only)."""
    with wp.ScopedStream(wp_stream):
        if field_fp32.ndim == 1:
            wp_field = wp.from_torch(field_fp32, dtype=wp.float32)
            _wp_launch(
                kernel=_uniform_grid_derivatives_1d_order2_fused_kernel,
                dim=field_fp32.shape[0],
                inputs=[
                    wp_field,
                    1.0 / float(spacing_tuple[0]),
                    1.0 / float(spacing_tuple[0] * spacing_tuple[0]),
                    wp.from_torch(first_components[0], dtype=wp.float32),
                    wp.from_torch(second_components[0], dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        if field_fp32.ndim == 2:
            wp_field = wp.from_torch(field_fp32, dtype=wp.float32)
            if include_mixed:
                _wp_launch(
                    kernel=_uniform_grid_derivatives_2d_order2_fused_kernel,
                    dim=field_fp32.shape,
                    inputs=[
                        wp_field,
                        1.0 / float(spacing_tuple[0]),
                        1.0 / float(spacing_tuple[1]),
                        1.0 / float(spacing_tuple[0] * spacing_tuple[0]),
                        1.0 / float(spacing_tuple[1] * spacing_tuple[1]),
                        1.0 / float(spacing_tuple[0] * spacing_tuple[1]),
                        wp.from_torch(first_components[0], dtype=wp.float32),
                        wp.from_torch(first_components[1], dtype=wp.float32),
                        wp.from_torch(second_components[0], dtype=wp.float32),
                        wp.from_torch(second_components[1], dtype=wp.float32),
                        wp.from_torch(mixed_components[0], dtype=wp.float32),
                    ],
                    device=wp_device,
                    stream=wp_stream,
                )
            else:
                _wp_launch(
                    kernel=_uniform_grid_derivatives_2d_order2_fused_no_mixed_kernel,
                    dim=field_fp32.shape,
                    inputs=[
                        wp_field,
                        1.0 / float(spacing_tuple[0]),
                        1.0 / float(spacing_tuple[1]),
                        1.0 / float(spacing_tuple[0] * spacing_tuple[0]),
                        1.0 / float(spacing_tuple[1] * spacing_tuple[1]),
                        wp.from_torch(first_components[0], dtype=wp.float32),
                        wp.from_torch(first_components[1], dtype=wp.float32),
                        wp.from_torch(second_components[0], dtype=wp.float32),
                        wp.from_torch(second_components[1], dtype=wp.float32),
                    ],
                    device=wp_device,
                    stream=wp_stream,
                )
            return

        wp_field = wp.from_torch(field_fp32, dtype=wp.float32)
        if include_mixed:
            _wp_launch(
                kernel=_uniform_grid_derivatives_3d_order2_fused_kernel,
                dim=field_fp32.shape,
                inputs=[
                    wp_field,
                    1.0 / float(spacing_tuple[0]),
                    1.0 / float(spacing_tuple[1]),
                    1.0 / float(spacing_tuple[2]),
                    1.0 / float(spacing_tuple[0] * spacing_tuple[0]),
                    1.0 / float(spacing_tuple[1] * spacing_tuple[1]),
                    1.0 / float(spacing_tuple[2] * spacing_tuple[2]),
                    1.0 / float(spacing_tuple[0] * spacing_tuple[1]),
                    1.0 / float(spacing_tuple[0] * spacing_tuple[2]),
                    1.0 / float(spacing_tuple[1] * spacing_tuple[2]),
                    wp.from_torch(first_components[0], dtype=wp.float32),
                    wp.from_torch(first_components[1], dtype=wp.float32),
                    wp.from_torch(first_components[2], dtype=wp.float32),
                    wp.from_torch(second_components[0], dtype=wp.float32),
                    wp.from_torch(second_components[1], dtype=wp.float32),
                    wp.from_torch(second_components[2], dtype=wp.float32),
                    wp.from_torch(mixed_components[0], dtype=wp.float32),
                    wp.from_torch(mixed_components[1], dtype=wp.float32),
                    wp.from_torch(mixed_components[2], dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
        else:
            _wp_launch(
                kernel=_uniform_grid_derivatives_3d_order2_fused_no_mixed_kernel,
                dim=field_fp32.shape,
                inputs=[
                    wp_field,
                    1.0 / float(spacing_tuple[0]),
                    1.0 / float(spacing_tuple[1]),
                    1.0 / float(spacing_tuple[2]),
                    1.0 / float(spacing_tuple[0] * spacing_tuple[0]),
                    1.0 / float(spacing_tuple[1] * spacing_tuple[1]),
                    1.0 / float(spacing_tuple[2] * spacing_tuple[2]),
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
