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

"""Dimension-specific Warp launch helpers for interpolation forward passes."""

import torch
import warp as wp

from ._kernels.forward_1d_stride1 import interp_1d_stride1
from ._kernels.forward_1d_stride2 import interp_1d_stride2
from ._kernels.forward_1d_stride5 import interp_1d_stride5
from ._kernels.forward_2d_stride1 import interp_2d_stride1
from ._kernels.forward_2d_stride2 import interp_2d_stride2
from ._kernels.forward_2d_stride5 import interp_2d_stride5
from ._kernels.forward_3d_stride1 import interp_3d_stride1
from ._kernels.forward_3d_stride2 import interp_3d_stride2
from ._kernels.forward_3d_stride5 import interp_3d_stride5


# Launch the 1D forward interpolation kernel based on the current stride.
def _launch_forward_1d(
    query_points: torch.Tensor,
    context_grid: torch.Tensor,
    output: torch.Tensor,
    start_vals: list[float],
    dx_vals: list[float],
    padded_sizes: list[int],
    center_offset: float,
    interp_id: int,
    stride: int,
    num_points: int,
    wp_device,
    wp_stream,
) -> None:
    # Convert torch tensors to warp views with dtypes expected by 1D kernels.
    points = query_points[:, 0].contiguous()
    wp_points = wp.from_torch(points, dtype=wp.float32)
    wp_grid = wp.from_torch(context_grid.contiguous())
    wp_out = wp.from_torch(output, return_ctype=True)

    # Dispatch to the kernel that matches the interpolation stencil width.
    if stride == 1:
        wp.launch(
            interp_1d_stride1,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_out,
                float(start_vals[0]),
                float(dx_vals[0]),
                int(padded_sizes[0]),
                float(center_offset),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    elif stride == 2:
        wp.launch(
            interp_1d_stride2,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_out,
                float(start_vals[0]),
                float(dx_vals[0]),
                int(padded_sizes[0]),
                int(interp_id),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    else:
        wp.launch(
            interp_1d_stride5,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_out,
                float(start_vals[0]),
                float(dx_vals[0]),
                int(padded_sizes[0]),
                float(center_offset),
            ],
            device=wp_device,
            stream=wp_stream,
        )


# Launch the 2D forward interpolation kernel based on the current stride.
def _launch_forward_2d(
    query_points: torch.Tensor,
    context_grid: torch.Tensor,
    output: torch.Tensor,
    start_vals: list[float],
    dx_vals: list[float],
    padded_sizes: list[int],
    center_offset: float,
    interp_id: int,
    stride: int,
    num_points: int,
    wp_device,
    wp_stream,
) -> None:
    # Convert torch tensors to warp views with dtypes expected by 2D kernels.
    wp_points = wp.from_torch(query_points.contiguous(), dtype=wp.vec2f)
    wp_grid = wp.from_torch(context_grid.contiguous())
    wp_out = wp.from_torch(output, return_ctype=True)
    origin = wp.vec2f(float(start_vals[0]), float(start_vals[1]))
    spacing = wp.vec2f(float(dx_vals[0]), float(dx_vals[1]))
    size = wp.vec2i(int(padded_sizes[0]), int(padded_sizes[1]))

    # Dispatch to the kernel that matches the interpolation stencil width.
    if stride == 1:
        wp.launch(
            interp_2d_stride1,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_out,
                origin,
                spacing,
                size,
                float(center_offset),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    elif stride == 2:
        wp.launch(
            interp_2d_stride2,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_out,
                origin,
                spacing,
                size,
                int(interp_id),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    else:
        wp.launch(
            interp_2d_stride5,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_out,
                origin,
                spacing,
                size,
                float(center_offset),
            ],
            device=wp_device,
            stream=wp_stream,
        )


# Launch the 3D forward interpolation kernel based on the current stride.
def _launch_forward_3d(
    query_points: torch.Tensor,
    context_grid: torch.Tensor,
    output: torch.Tensor,
    start_vals: list[float],
    dx_vals: list[float],
    padded_sizes: list[int],
    center_offset: float,
    interp_id: int,
    stride: int,
    num_points: int,
    wp_device,
    wp_stream,
) -> None:
    # Convert torch tensors to warp views with dtypes expected by 3D kernels.
    wp_points = wp.from_torch(query_points.contiguous(), dtype=wp.vec3f)
    wp_grid = wp.from_torch(context_grid.contiguous())
    wp_out = wp.from_torch(output, return_ctype=True)
    origin = wp.vec3f(
        float(start_vals[0]),
        float(start_vals[1]),
        float(start_vals[2]),
    )
    spacing = wp.vec3f(float(dx_vals[0]), float(dx_vals[1]), float(dx_vals[2]))
    size = wp.vec3i(
        int(padded_sizes[0]),
        int(padded_sizes[1]),
        int(padded_sizes[2]),
    )

    # Dispatch to the kernel that matches the interpolation stencil width.
    if stride == 1:
        wp.launch(
            interp_3d_stride1,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_out,
                origin,
                spacing,
                size,
                float(center_offset),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    elif stride == 2:
        wp.launch(
            interp_3d_stride2,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_out,
                origin,
                spacing,
                size,
                int(interp_id),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    else:
        wp.launch(
            interp_3d_stride5,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_out,
                origin,
                spacing,
                size,
                float(center_offset),
            ],
            device=wp_device,
            stream=wp_stream,
        )


# Dispatch to the forward kernel launcher matching input dimensionality.
def launch_forward(
    query_points: torch.Tensor,
    context_grid: torch.Tensor,
    output: torch.Tensor,
    dims: int,
    start_vals: list[float],
    dx_vals: list[float],
    padded_sizes: list[int],
    center_offset: float,
    interp_id: int,
    stride: int,
    num_points: int,
    wp_device,
    wp_stream,
) -> None:
    if dims == 1:
        _launch_forward_1d(
            query_points=query_points,
            context_grid=context_grid,
            output=output,
            start_vals=start_vals,
            dx_vals=dx_vals,
            padded_sizes=padded_sizes,
            center_offset=center_offset,
            interp_id=interp_id,
            stride=stride,
            num_points=num_points,
            wp_device=wp_device,
            wp_stream=wp_stream,
        )
        return

    if dims == 2:
        _launch_forward_2d(
            query_points=query_points,
            context_grid=context_grid,
            output=output,
            start_vals=start_vals,
            dx_vals=dx_vals,
            padded_sizes=padded_sizes,
            center_offset=center_offset,
            interp_id=interp_id,
            stride=stride,
            num_points=num_points,
            wp_device=wp_device,
            wp_stream=wp_stream,
        )
        return

    if dims == 3:
        _launch_forward_3d(
            query_points=query_points,
            context_grid=context_grid,
            output=output,
            start_vals=start_vals,
            dx_vals=dx_vals,
            padded_sizes=padded_sizes,
            center_offset=center_offset,
            interp_id=interp_id,
            stride=stride,
            num_points=num_points,
            wp_device=wp_device,
            wp_stream=wp_stream,
        )
        return

    raise ValueError(f"Unsupported interpolation dimensionality {dims}")


__all__ = ["launch_forward"]
