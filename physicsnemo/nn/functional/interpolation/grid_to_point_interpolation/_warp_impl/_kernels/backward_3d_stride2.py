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

"""3D linear/smooth backward interpolation kernel."""

import warp as wp

from ..utils import basis_derivative, basis_value


@wp.kernel
def backward_3d_stride2(
    points: wp.array(dtype=wp.vec3f),
    grid: wp.array4d(dtype=wp.float32),
    grad_output: wp.array2d(dtype=wp.float32),
    grad_query: wp.array2d(dtype=wp.float32),
    grad_grid: wp.array4d(dtype=wp.float32),
    origin: wp.vec3f,
    dx: wp.vec3f,
    size: wp.vec3i,
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
    pos_z = (p[2] - origin[2]) / dx[2]
    center_x = wp.int32(pos_x)
    center_y = wp.int32(pos_y)
    center_z = wp.int32(pos_z)
    frac_x = pos_x - wp.float32(center_x)
    frac_y = pos_y - wp.float32(center_y)
    frac_z = pos_z - wp.float32(center_z)

    lower_x = basis_value(interp_id, frac_x)
    upper_x = basis_value(interp_id, 1.0 - frac_x)
    lower_y = basis_value(interp_id, frac_y)
    upper_y = basis_value(interp_id, 1.0 - frac_y)
    lower_z = basis_value(interp_id, frac_z)
    upper_z = basis_value(interp_id, 1.0 - frac_z)
    d_lower_x = basis_derivative(interp_id, frac_x) / dx[0]
    d_upper_x = -basis_derivative(interp_id, 1.0 - frac_x) / dx[0]
    d_lower_y = basis_derivative(interp_id, frac_y) / dx[1]
    d_upper_y = -basis_derivative(interp_id, 1.0 - frac_y) / dx[1]
    d_lower_z = basis_derivative(interp_id, frac_z) / dx[2]
    d_upper_z = -basis_derivative(interp_id, 1.0 - frac_z) / dx[2]

    # Clamp stencil indices so boundary samples stay in bounds.
    idx_x0 = center_x
    idx_x1 = center_x + 1
    idx_y0 = center_y
    idx_y1 = center_y + 1
    idx_z0 = center_z
    idx_z1 = center_z + 1
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
    if idx_z0 < 0:
        idx_z0 = 0
    if idx_z0 >= size[2]:
        idx_z0 = size[2] - 1
    if idx_z1 < 0:
        idx_z1 = 0
    if idx_z1 >= size[2]:
        idx_z1 = size[2] - 1

    w000 = upper_x * upper_y * upper_z
    w001 = upper_x * upper_y * lower_z
    w010 = upper_x * lower_y * upper_z
    w011 = upper_x * lower_y * lower_z
    w100 = lower_x * upper_y * upper_z
    w101 = lower_x * upper_y * lower_z
    w110 = lower_x * lower_y * upper_z
    w111 = lower_x * lower_y * lower_z
    dw000_dx = d_upper_x * upper_y * upper_z
    dw001_dx = d_upper_x * upper_y * lower_z
    dw010_dx = d_upper_x * lower_y * upper_z
    dw011_dx = d_upper_x * lower_y * lower_z
    dw100_dx = d_lower_x * upper_y * upper_z
    dw101_dx = d_lower_x * upper_y * lower_z
    dw110_dx = d_lower_x * lower_y * upper_z
    dw111_dx = d_lower_x * lower_y * lower_z
    dw000_dy = upper_x * d_upper_y * upper_z
    dw001_dy = upper_x * d_upper_y * lower_z
    dw010_dy = upper_x * d_lower_y * upper_z
    dw011_dy = upper_x * d_lower_y * lower_z
    dw100_dy = lower_x * d_upper_y * upper_z
    dw101_dy = lower_x * d_upper_y * lower_z
    dw110_dy = lower_x * d_lower_y * upper_z
    dw111_dy = lower_x * d_lower_y * lower_z
    dw000_dz = upper_x * upper_y * d_upper_z
    dw001_dz = upper_x * upper_y * d_lower_z
    dw010_dz = upper_x * lower_y * d_upper_z
    dw011_dz = upper_x * lower_y * d_lower_z
    dw100_dz = lower_x * upper_y * d_upper_z
    dw101_dz = lower_x * upper_y * d_lower_z
    dw110_dz = lower_x * lower_y * d_upper_z
    dw111_dz = lower_x * lower_y * d_lower_z

    grad_x = wp.float32(0.0)
    grad_y = wp.float32(0.0)
    grad_z = wp.float32(0.0)

    # Accumulate channel contributions for this sample.
    for c in range(grid.shape[0]):
        g = grad_output[tid, c]
        v000 = grid[c, idx_x0, idx_y0, idx_z0]
        v001 = grid[c, idx_x0, idx_y0, idx_z1]
        v010 = grid[c, idx_x0, idx_y1, idx_z0]
        v011 = grid[c, idx_x0, idx_y1, idx_z1]
        v100 = grid[c, idx_x1, idx_y0, idx_z0]
        v101 = grid[c, idx_x1, idx_y0, idx_z1]
        v110 = grid[c, idx_x1, idx_y1, idx_z0]
        v111 = grid[c, idx_x1, idx_y1, idx_z1]

        # Accumulate gradient contributions for the output grid.
        if compute_grid_grad != 0:
            wp.atomic_add(grad_grid, c, idx_x0, idx_y0, idx_z0, g * w000)
            wp.atomic_add(grad_grid, c, idx_x0, idx_y0, idx_z1, g * w001)
            wp.atomic_add(grad_grid, c, idx_x0, idx_y1, idx_z0, g * w010)
            wp.atomic_add(grad_grid, c, idx_x0, idx_y1, idx_z1, g * w011)
            wp.atomic_add(grad_grid, c, idx_x1, idx_y0, idx_z0, g * w100)
            wp.atomic_add(grad_grid, c, idx_x1, idx_y0, idx_z1, g * w101)
            wp.atomic_add(grad_grid, c, idx_x1, idx_y1, idx_z0, g * w110)
            wp.atomic_add(grad_grid, c, idx_x1, idx_y1, idx_z1, g * w111)

        # Accumulate gradient contributions for query-point coordinates.
        if compute_query_grad != 0:
            grad_x += g * (
                v000 * dw000_dx
                + v001 * dw001_dx
                + v010 * dw010_dx
                + v011 * dw011_dx
                + v100 * dw100_dx
                + v101 * dw101_dx
                + v110 * dw110_dx
                + v111 * dw111_dx
            )
            grad_y += g * (
                v000 * dw000_dy
                + v001 * dw001_dy
                + v010 * dw010_dy
                + v011 * dw011_dy
                + v100 * dw100_dy
                + v101 * dw101_dy
                + v110 * dw110_dy
                + v111 * dw111_dy
            )
            grad_z += g * (
                v000 * dw000_dz
                + v001 * dw001_dz
                + v010 * dw010_dz
                + v011 * dw011_dz
                + v100 * dw100_dz
                + v101 * dw101_dz
                + v110 * dw110_dz
                + v111 * dw111_dz
            )

    if compute_query_grad != 0:
        grad_query[tid, 0] = grad_x
        grad_query[tid, 1] = grad_y
        grad_query[tid, 2] = grad_z


__all__ = ["backward_3d_stride2"]
