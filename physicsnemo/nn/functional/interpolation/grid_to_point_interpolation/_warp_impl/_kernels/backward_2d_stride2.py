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

"""2D linear/smooth backward interpolation kernel."""

import warp as wp

from ..utils import basis_derivative, basis_value


@wp.kernel
def backward_2d_stride2(
    points: wp.array(dtype=wp.vec2f),
    grid: wp.array3d(dtype=wp.float32),
    grad_output: wp.array2d(dtype=wp.float32),
    grad_query: wp.array2d(dtype=wp.float32),
    grad_grid: wp.array3d(dtype=wp.float32),
    origin: wp.vec2f,
    dx: wp.vec2f,
    size: wp.vec2i,
    interp_id: int,
    compute_query_grad: int,
    compute_grid_grad: int,
):
    tid = wp.tid()

    # Map one Warp thread to one query/scatter sample.
    p = points[tid]

    # Convert world-space coordinates into grid-space coordinates.
    pos_x = (p[0] - origin[0]) / dx[0]
    pos_y = (p[1] - origin[1]) / dx[1]
    center_x = wp.int32(pos_x)
    center_y = wp.int32(pos_y)
    frac_x = pos_x - wp.float32(center_x)
    frac_y = pos_y - wp.float32(center_y)

    lower_x = basis_value(interp_id, frac_x)
    upper_x = basis_value(interp_id, 1.0 - frac_x)
    lower_y = basis_value(interp_id, frac_y)
    upper_y = basis_value(interp_id, 1.0 - frac_y)
    d_lower_x = basis_derivative(interp_id, frac_x) / dx[0]
    d_upper_x = -basis_derivative(interp_id, 1.0 - frac_x) / dx[0]
    d_lower_y = basis_derivative(interp_id, frac_y) / dx[1]
    d_upper_y = -basis_derivative(interp_id, 1.0 - frac_y) / dx[1]

    # Clamp stencil indices so boundary samples stay in bounds.
    idx_x0 = center_x
    idx_x1 = center_x + 1
    idx_y0 = center_y
    idx_y1 = center_y + 1
    if idx_x0 < 0:
        idx_x0 = 0
    if idx_x0 >= size[0]:
        idx_x0 = size[0] - 1
    if idx_x1 < 0:
        idx_x1 = 0
    if idx_x1 >= size[0]:
        idx_x1 = size[0] - 1
    if idx_y0 < 0:
        idx_y0 = 0
    if idx_y0 >= size[1]:
        idx_y0 = size[1] - 1
    if idx_y1 < 0:
        idx_y1 = 0
    if idx_y1 >= size[1]:
        idx_y1 = size[1] - 1

    w00 = upper_x * upper_y
    w01 = upper_x * lower_y
    w10 = lower_x * upper_y
    w11 = lower_x * lower_y
    dw00_dx = d_upper_x * upper_y
    dw01_dx = d_upper_x * lower_y
    dw10_dx = d_lower_x * upper_y
    dw11_dx = d_lower_x * lower_y
    dw00_dy = upper_x * d_upper_y
    dw01_dy = upper_x * d_lower_y
    dw10_dy = lower_x * d_upper_y
    dw11_dy = lower_x * d_lower_y

    grad_x = wp.float32(0.0)
    grad_y = wp.float32(0.0)

    # Accumulate channel contributions for this sample.
    for c in range(grid.shape[0]):
        g = grad_output[tid, c]
        v00 = grid[c, idx_x0, idx_y0]
        v01 = grid[c, idx_x0, idx_y1]
        v10 = grid[c, idx_x1, idx_y0]
        v11 = grid[c, idx_x1, idx_y1]

        # Accumulate gradient contributions for the output grid.
        if compute_grid_grad != 0:
            wp.atomic_add(grad_grid, c, idx_x0, idx_y0, g * w00)
            wp.atomic_add(grad_grid, c, idx_x0, idx_y1, g * w01)
            wp.atomic_add(grad_grid, c, idx_x1, idx_y0, g * w10)
            wp.atomic_add(grad_grid, c, idx_x1, idx_y1, g * w11)

        # Accumulate gradient contributions for query-point coordinates.
        if compute_query_grad != 0:
            grad_x += g * (
                v00 * dw00_dx + v01 * dw01_dx + v10 * dw10_dx + v11 * dw11_dx
            )
            grad_y += g * (
                v00 * dw00_dy + v01 * dw01_dy + v10 * dw10_dy + v11 * dw11_dy
            )

    if compute_query_grad != 0:
        grad_query[tid, 0] = grad_x
        grad_query[tid, 1] = grad_y


__all__ = ["backward_2d_stride2"]
