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

"""2D gaussian point-to-grid backward kernel."""

import warp as wp


@wp.kernel
def point_to_grid_backward_2d_stride5(
    points: wp.array(dtype=wp.vec2f),
    point_values: wp.array2d(dtype=wp.float32),
    grad_grid_output: wp.array3d(dtype=wp.float32),
    grad_query: wp.array2d(dtype=wp.float32),
    grad_point_values: wp.array2d(dtype=wp.float32),
    origin: wp.vec2f,
    dx: wp.vec2f,
    size: wp.vec2i,
    center_offset: wp.float32,
    compute_query_grad: int,
    compute_values_grad: int,
):
    tid = wp.tid()

    # Map one Warp thread to one query/scatter sample.
    p = points[tid]

    # Convert world-space coordinates into grid-space coordinates.
    pos_x = (p[0] - origin[0]) / dx[0]
    pos_y = (p[1] - origin[1]) / dx[1]
    center_x = wp.int32(pos_x + center_offset)
    center_y = wp.int32(pos_y + center_offset)
    sigma_x = dx[0] / 2.0
    sigma_y = dx[1] / 2.0

    sum_w = wp.float32(0.0)
    for ox in range(-2, 3):
        idx_x = center_x + ox
        if idx_x < 0:
            idx_x = 0
        if idx_x >= size[0]:
            idx_x = size[0] - 1
        coord_x = origin[0] + wp.float32(idx_x) * dx[0]
        dist_x = (p[0] - coord_x) / sigma_x
        gx = wp.exp(-0.5 * dist_x * dist_x)
        for oy in range(-2, 3):
            idx_y = center_y + oy
            if idx_y < 0:
                idx_y = 0
            if idx_y >= size[1]:
                idx_y = size[1] - 1
            coord_y = origin[1] + wp.float32(idx_y) * dx[1]
            dist_y = (p[1] - coord_y) / sigma_y
            sum_w += gx * wp.exp(-0.5 * dist_y * dist_y)

    if sum_w <= 0.0:
        # Accumulate gradient contributions for query-point coordinates.
        if compute_query_grad != 0:
            grad_query[tid, 0] = 0.0
            grad_query[tid, 1] = 0.0

        # Accumulate gradient contributions for per-point input values.
        if compute_values_grad != 0:
            # Accumulate channel contributions for this sample.
            for c in range(point_values.shape[1]):
                grad_point_values[tid, c] = 0.0
        return
    inv_sum_w = 1.0 / sum_w

    grad_x = wp.float32(0.0)
    grad_y = wp.float32(0.0)
    for c in range(point_values.shape[1]):
        y = wp.float32(0.0)
        for ox in range(-2, 3):
            idx_x = center_x + ox
            if idx_x < 0:
                idx_x = 0
            if idx_x >= size[0]:
                idx_x = size[0] - 1
            coord_x = origin[0] + wp.float32(idx_x) * dx[0]
            dist_x = (p[0] - coord_x) / sigma_x
            gx = wp.exp(-0.5 * dist_x * dist_x)
            for oy in range(-2, 3):
                idx_y = center_y + oy
                if idx_y < 0:
                    idx_y = 0
                if idx_y >= size[1]:
                    idx_y = size[1] - 1
                coord_y = origin[1] + wp.float32(idx_y) * dx[1]
                dist_y = (p[1] - coord_y) / sigma_y
                w = gx * wp.exp(-0.5 * dist_y * dist_y)
                y += w * grad_grid_output[c, idx_x, idx_y]
        y = y * inv_sum_w

        if compute_values_grad != 0:
            grad_point_values[tid, c] = y

        if compute_query_grad != 0:
            value = point_values[tid, c]
            for ox in range(-2, 3):
                idx_x = center_x + ox
                if idx_x < 0:
                    idx_x = 0
                if idx_x >= size[0]:
                    idx_x = size[0] - 1
                coord_x = origin[0] + wp.float32(idx_x) * dx[0]
                dist_x = (p[0] - coord_x) / sigma_x
                gx = wp.exp(-0.5 * dist_x * dist_x)
                for oy in range(-2, 3):
                    idx_y = center_y + oy
                    if idx_y < 0:
                        idx_y = 0
                    if idx_y >= size[1]:
                        idx_y = size[1] - 1
                    coord_y = origin[1] + wp.float32(idx_y) * dx[1]
                    dist_y = (p[1] - coord_y) / sigma_y
                    gy = wp.exp(-0.5 * dist_y * dist_y)
                    w = gx * gy
                    dwdx = -w * dist_x / sigma_x
                    dwdy = -w * dist_y / sigma_y
                    grid_value = grad_grid_output[c, idx_x, idx_y]
                    grad_x += value * ((dwdx * inv_sum_w) * (grid_value - y))
                    grad_y += value * ((dwdy * inv_sum_w) * (grid_value - y))

    if compute_query_grad != 0:
        grad_query[tid, 0] = grad_x
        grad_query[tid, 1] = grad_y


__all__ = ["point_to_grid_backward_2d_stride5"]
