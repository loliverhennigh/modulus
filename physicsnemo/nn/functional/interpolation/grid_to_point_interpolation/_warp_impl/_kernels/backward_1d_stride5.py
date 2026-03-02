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

"""1D gaussian backward interpolation kernel."""

import warp as wp


@wp.kernel
def backward_1d_stride5(
    points: wp.array(dtype=wp.float32),
    grid: wp.array2d(dtype=wp.float32),
    grad_output: wp.array2d(dtype=wp.float32),
    grad_query: wp.array2d(dtype=wp.float32),
    grad_grid: wp.array2d(dtype=wp.float32),
    origin: wp.float32,
    dx: wp.float32,
    size_x: int,
    center_offset: wp.float32,
    compute_query_grad: int,
    compute_grid_grad: int,
):
    tid = wp.tid()

    # Map one Warp thread to one query/scatter sample.
    x = points[tid]

    # Convert world-space coordinates into grid-space coordinates.
    pos = (x - origin) / dx
    center = wp.int32(pos + center_offset)
    sigma = dx / 2.0

    sum_w = wp.float32(0.0)
    for ox in range(-2, 3):
        idx = center + ox
        if idx < 0:
            idx = 0
        if idx >= size_x:
            idx = size_x - 1
        coord = origin + wp.float32(idx) * dx
        dist = (x - coord) / sigma
        sum_w += wp.exp(-0.5 * dist * dist)

    if sum_w <= 0.0:
        # Accumulate gradient contributions for query-point coordinates.
        if compute_query_grad != 0:
            grad_query[tid, 0] = 0.0
        return
    inv_sum_w = 1.0 / sum_w

    grad_x = wp.float32(0.0)

    # Accumulate channel contributions for this sample.
    for c in range(grid.shape[0]):
        y = wp.float32(0.0)
        for ox in range(-2, 3):
            idx = center + ox
            if idx < 0:
                idx = 0
            if idx >= size_x:
                idx = size_x - 1
            coord = origin + wp.float32(idx) * dx
            dist = (x - coord) / sigma
            w = wp.exp(-0.5 * dist * dist)
            y += w * grid[c, idx]
        y = y * inv_sum_w

        g = grad_output[tid, c]
        for ox in range(-2, 3):
            idx = center + ox
            if idx < 0:
                idx = 0
            if idx >= size_x:
                idx = size_x - 1
            coord = origin + wp.float32(idx) * dx
            dist = (x - coord) / sigma
            w = wp.exp(-0.5 * dist * dist)
            dwdx = -w * dist / sigma

            # Accumulate gradient contributions for the output grid.
            if compute_grid_grad != 0:
                wp.atomic_add(grad_grid, c, idx, g * (w * inv_sum_w))
            if compute_query_grad != 0:
                grad_x += g * ((dwdx * inv_sum_w) * (grid[c, idx] - y))

    if compute_query_grad != 0:
        grad_query[tid, 0] = grad_x


__all__ = ["backward_1d_stride5"]
