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

"""Warp backward launch surface for grid-to-point interpolation.

This file is the single backward launch entrypoint for the interpolation
functional. It contains:

1. Private, dimension-specific launch helpers (1D/2D/3D)
2. The public backward dispatcher used by the torch custom op
"""

import torch
import torch.nn.functional as F
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from ._kernels.backward_1d_stride1 import backward_1d_stride1
from ._kernels.backward_1d_stride2 import backward_1d_stride2
from ._kernels.backward_1d_stride5 import backward_1d_stride5
from ._kernels.backward_2d_stride1 import backward_2d_stride1
from ._kernels.backward_2d_stride2 import backward_2d_stride2
from ._kernels.backward_2d_stride5 import backward_2d_stride5
from ._kernels.backward_3d_stride1 import backward_3d_stride1
from ._kernels.backward_3d_stride2 import backward_3d_stride2
from ._kernels.backward_3d_stride5 import backward_3d_stride5
from .utils import _INTERP_ID_TO_STRIDE, _INTERP_NEAREST


# Crop padded grid gradients back to original grid shape.
def crop_grad_grid(
    grad_padded: torch.Tensor | None,
    k: int,
    grid: list[tuple[float, float, int]],
    dims: int,
) -> torch.Tensor | None:
    if grad_padded is None:
        return None
    if k == 0:
        return grad_padded
    if dims == 1:
        return grad_padded[:, k : k + grid[0][2]]
    if dims == 2:
        return grad_padded[:, k : k + grid[0][2], k : k + grid[1][2]]
    return grad_padded[
        :,
        k : k + grid[0][2],
        k : k + grid[1][2],
        k : k + grid[2][2],
    ]


# Restore gradients to the input contract (dtype and shape).
def restore_grad_layout(
    grad_query: torch.Tensor | None,
    grad_grid: torch.Tensor | None,
    query_points: torch.Tensor,
    context_grid: torch.Tensor,
    squeeze_query: bool,
) -> tuple[torch.Tensor | None, torch.Tensor | None]:
    if grad_query is not None and query_points.dtype != torch.float32:
        grad_query = grad_query.to(query_points.dtype)
    if grad_grid is not None and context_grid.dtype != torch.float32:
        grad_grid = grad_grid.to(context_grid.dtype)
    if grad_query is not None and squeeze_query:
        grad_query = grad_query.squeeze(-1)
    return grad_query, grad_grid


# Launch the 1D backward kernel corresponding to the selected forward stride.
def _launch_backward_1d(
    query_points: torch.Tensor,
    padded_grid: torch.Tensor,
    grad_output: torch.Tensor,
    grad_query: torch.Tensor,
    grad_grid: torch.Tensor,
    start_vals: list[float],
    dx_vals: list[float],
    padded_sizes: list[int],
    center_offset: float,
    interp_id: int,
    stride: int,
    num_points: int,
    compute_query_grad: int,
    compute_grid_grad: int,
    wp_device,
    wp_stream,
) -> None:
    # Convert torch tensors to warp views with dtypes expected by 1D kernels.
    points = query_points[:, 0].contiguous()
    wp_points = wp.from_torch(points, dtype=wp.float32)
    wp_grad_out = wp.from_torch(grad_output.contiguous())
    wp_grad_query = wp.from_torch(grad_query.contiguous(), return_ctype=True)
    wp_grad_grid = wp.from_torch(grad_grid.contiguous(), return_ctype=True)

    # Dispatch to the backward kernel matching the forward stencil width.
    if stride == 1:
        wp.launch(
            backward_1d_stride1,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grad_out,
                wp_grad_grid,
                float(start_vals[0]),
                float(dx_vals[0]),
                int(padded_sizes[0]),
                float(center_offset),
                int(compute_grid_grad),
            ],
            device=wp_device,
            stream=wp_stream,
        )
        return

    wp_grid = wp.from_torch(padded_grid.contiguous())
    if stride == 2:
        wp.launch(
            backward_1d_stride2,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_grad_out,
                wp_grad_query,
                wp_grad_grid,
                float(start_vals[0]),
                float(dx_vals[0]),
                int(padded_sizes[0]),
                int(interp_id),
                int(compute_query_grad),
                int(compute_grid_grad),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    else:
        wp.launch(
            backward_1d_stride5,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_grad_out,
                wp_grad_query,
                wp_grad_grid,
                float(start_vals[0]),
                float(dx_vals[0]),
                int(padded_sizes[0]),
                float(center_offset),
                int(compute_query_grad),
                int(compute_grid_grad),
            ],
            device=wp_device,
            stream=wp_stream,
        )


# Launch the 2D backward kernel corresponding to the selected forward stride.
def _launch_backward_2d(
    query_points: torch.Tensor,
    padded_grid: torch.Tensor,
    grad_output: torch.Tensor,
    grad_query: torch.Tensor,
    grad_grid: torch.Tensor,
    start_vals: list[float],
    dx_vals: list[float],
    padded_sizes: list[int],
    center_offset: float,
    interp_id: int,
    stride: int,
    num_points: int,
    compute_query_grad: int,
    compute_grid_grad: int,
    wp_device,
    wp_stream,
) -> None:
    # Convert torch tensors to warp views with dtypes expected by 2D kernels.
    wp_points = wp.from_torch(query_points.contiguous(), dtype=wp.vec2f)
    wp_grad_out = wp.from_torch(grad_output.contiguous())
    wp_grad_query = wp.from_torch(grad_query.contiguous(), return_ctype=True)
    wp_grad_grid = wp.from_torch(grad_grid.contiguous(), return_ctype=True)
    origin = wp.vec2f(float(start_vals[0]), float(start_vals[1]))
    spacing = wp.vec2f(float(dx_vals[0]), float(dx_vals[1]))
    size = wp.vec2i(int(padded_sizes[0]), int(padded_sizes[1]))

    # Dispatch to the backward kernel matching the forward stencil width.
    if stride == 1:
        wp.launch(
            backward_2d_stride1,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grad_out,
                wp_grad_grid,
                origin,
                spacing,
                size,
                float(center_offset),
                int(compute_grid_grad),
            ],
            device=wp_device,
            stream=wp_stream,
        )
        return

    wp_grid = wp.from_torch(padded_grid.contiguous())
    if stride == 2:
        wp.launch(
            backward_2d_stride2,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_grad_out,
                wp_grad_query,
                wp_grad_grid,
                origin,
                spacing,
                size,
                int(interp_id),
                int(compute_query_grad),
                int(compute_grid_grad),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    else:
        wp.launch(
            backward_2d_stride5,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_grad_out,
                wp_grad_query,
                wp_grad_grid,
                origin,
                spacing,
                size,
                float(center_offset),
                int(compute_query_grad),
                int(compute_grid_grad),
            ],
            device=wp_device,
            stream=wp_stream,
        )


# Launch the 3D backward kernel corresponding to the selected forward stride.
def _launch_backward_3d(
    query_points: torch.Tensor,
    padded_grid: torch.Tensor,
    grad_output: torch.Tensor,
    grad_query: torch.Tensor,
    grad_grid: torch.Tensor,
    start_vals: list[float],
    dx_vals: list[float],
    padded_sizes: list[int],
    center_offset: float,
    interp_id: int,
    stride: int,
    num_points: int,
    compute_query_grad: int,
    compute_grid_grad: int,
    wp_device,
    wp_stream,
) -> None:
    # Convert torch tensors to warp views with dtypes expected by 3D kernels.
    wp_points = wp.from_torch(query_points.contiguous(), dtype=wp.vec3f)
    wp_grad_out = wp.from_torch(grad_output.contiguous())
    wp_grad_query = wp.from_torch(grad_query.contiguous(), return_ctype=True)
    wp_grad_grid = wp.from_torch(grad_grid.contiguous(), return_ctype=True)
    origin = wp.vec3f(
        float(start_vals[0]),
        float(start_vals[1]),
        float(start_vals[2]),
    )
    spacing = wp.vec3f(
        float(dx_vals[0]),
        float(dx_vals[1]),
        float(dx_vals[2]),
    )
    size = wp.vec3i(
        int(padded_sizes[0]),
        int(padded_sizes[1]),
        int(padded_sizes[2]),
    )

    # Dispatch to the backward kernel matching the forward stencil width.
    if stride == 1:
        wp.launch(
            backward_3d_stride1,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grad_out,
                wp_grad_grid,
                origin,
                spacing,
                size,
                float(center_offset),
                int(compute_grid_grad),
            ],
            device=wp_device,
            stream=wp_stream,
        )
        return

    wp_grid = wp.from_torch(padded_grid.contiguous())
    if stride == 2:
        wp.launch(
            backward_3d_stride2,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_grad_out,
                wp_grad_query,
                wp_grad_grid,
                origin,
                spacing,
                size,
                int(interp_id),
                int(compute_query_grad),
                int(compute_grid_grad),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    else:
        wp.launch(
            backward_3d_stride5,
            dim=num_points,
            inputs=[
                wp_points,
                wp_grid,
                wp_grad_out,
                wp_grad_query,
                wp_grad_grid,
                origin,
                spacing,
                size,
                float(center_offset),
                int(compute_query_grad),
                int(compute_grid_grad),
            ],
            device=wp_device,
            stream=wp_stream,
        )


# Compute backward using specialized Warp kernels for each interpolation family.
def launch_backward(
    query_points: torch.Tensor,
    context_grid: torch.Tensor,
    grid_meta: torch.Tensor,
    interp_id: int,
    grad_output: torch.Tensor,
    needs_input_grad: tuple[bool, ...],
) -> tuple[torch.Tensor | None, torch.Tensor | None]:
    # Convert metadata tensor into Python tuples for launch-parameter setup.
    grid = [(float(g[0]), float(g[1]), int(g[2])) for g in grid_meta.to("cpu").tolist()]
    dims = len(grid)
    if dims < 1 or dims > 3:
        raise ValueError(f"Unsupported dimensionality {dims} for backward")

    # Normalize query points to (N, dims) and keep squeeze state for return.
    squeeze_query = dims == 1 and query_points.ndim == 1
    if squeeze_query:
        query_points = query_points.unsqueeze(-1)

    # Promote tensors to float32 for Warp kernel compatibility.
    query_fp32 = query_points.to(torch.float32)
    context_fp32 = context_grid.to(torch.float32)
    grad_output_fp32 = grad_output.to(torch.float32)

    # Rebuild padded grid and launch geometry exactly like forward.
    stride = _INTERP_ID_TO_STRIDE[interp_id]
    k = stride // 2
    padded_grid = F.pad(context_fp32, dims * (k, k)) if k > 0 else context_fp32
    dx_vals = [(g[1] - g[0]) / (g[2] - 1) for g in grid]
    start_vals = [g[0] - k * dx for g, dx in zip(grid, dx_vals)]
    padded_sizes = [g[2] + 2 * k for g in grid]
    center_offset = 0.5 if stride % 2 == 1 else 0.0

    # Allocate gradient outputs and lightweight dummies for disabled branches.
    compute_query_grad = int(bool(needs_input_grad[0]) and interp_id != _INTERP_NEAREST)
    compute_grid_grad = int(bool(needs_input_grad[1]))
    num_points = query_fp32.shape[0]

    if compute_query_grad:
        grad_query_work = torch.zeros(
            (num_points, dims),
            dtype=torch.float32,
            device=query_fp32.device,
        )
    else:
        grad_query_work = torch.zeros(
            (1, dims),
            dtype=torch.float32,
            device=query_fp32.device,
        )

    if compute_grid_grad:
        grad_padded = torch.zeros_like(padded_grid, dtype=torch.float32)
    else:
        grad_padded = padded_grid

    # Launch backward kernels on the same Warp stream context as torch.
    wp_device, wp_stream = FunctionSpec.warp_launch_context(query_fp32)
    with wp.ScopedStream(wp_stream):
        if dims == 1:
            _launch_backward_1d(
                query_points=query_fp32,
                padded_grid=padded_grid,
                grad_output=grad_output_fp32,
                grad_query=grad_query_work,
                grad_grid=grad_padded,
                start_vals=start_vals,
                dx_vals=dx_vals,
                padded_sizes=padded_sizes,
                center_offset=center_offset,
                interp_id=interp_id,
                stride=stride,
                num_points=num_points,
                compute_query_grad=compute_query_grad,
                compute_grid_grad=compute_grid_grad,
                wp_device=wp_device,
                wp_stream=wp_stream,
            )
        elif dims == 2:
            _launch_backward_2d(
                query_points=query_fp32,
                padded_grid=padded_grid,
                grad_output=grad_output_fp32,
                grad_query=grad_query_work,
                grad_grid=grad_padded,
                start_vals=start_vals,
                dx_vals=dx_vals,
                padded_sizes=padded_sizes,
                center_offset=center_offset,
                interp_id=interp_id,
                stride=stride,
                num_points=num_points,
                compute_query_grad=compute_query_grad,
                compute_grid_grad=compute_grid_grad,
                wp_device=wp_device,
                wp_stream=wp_stream,
            )
        else:
            _launch_backward_3d(
                query_points=query_fp32,
                padded_grid=padded_grid,
                grad_output=grad_output_fp32,
                grad_query=grad_query_work,
                grad_grid=grad_padded,
                start_vals=start_vals,
                dx_vals=dx_vals,
                padded_sizes=padded_sizes,
                center_offset=center_offset,
                interp_id=interp_id,
                stride=stride,
                num_points=num_points,
                compute_query_grad=compute_query_grad,
                compute_grid_grad=compute_grid_grad,
                wp_device=wp_device,
                wp_stream=wp_stream,
            )

    # Recover gradients that were requested by autograd.
    grad_query = grad_query_work if compute_query_grad else None
    grad_grid = crop_grad_grid(
        grad_padded=grad_padded if compute_grid_grad else None,
        k=k,
        grid=grid,
        dims=dims,
    )

    # Restore gradients to the original dtype/shape contract.
    return restore_grad_layout(
        grad_query=grad_query,
        grad_grid=grad_grid,
        query_points=query_points,
        context_grid=context_grid,
        squeeze_query=squeeze_query,
    )


__all__ = ["launch_backward"]
