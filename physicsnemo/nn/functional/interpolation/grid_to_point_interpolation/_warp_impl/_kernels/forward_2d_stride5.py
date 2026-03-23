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

"""2D gaussian forward interpolation kernel."""

import warp as wp


@wp.kernel
def interp_2d_stride5(
    points: wp.array(dtype=wp.vec2f),
    grid: wp.array3d(dtype=wp.float32),
    out: wp.array2d(dtype=wp.float32),
    origin: wp.vec2f,
    dx: wp.vec2f,
    size: wp.vec2i,
    center_offset: wp.float32,
):
    tid = wp.tid()

    # Map one Warp thread to one query/scatter sample.
    p = points[tid]

    # Convert world-space coordinates into grid-space coordinates.
    pos = wp.vec2f((p[0] - origin[0]) / dx[0], (p[1] - origin[1]) / dx[1])
    center_x = wp.int32(pos[0] + center_offset)
    center_y = wp.int32(pos[1] + center_offset)
    sigma_x = dx[0] / 2.0
    sigma_y = dx[1] / 2.0
    sum_w = 0.0

    # Accumulate channel contributions for this sample.
    for c in range(grid.shape[0]):
        out[tid, c] = 0.0
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
            weight = gx * wp.exp(-0.5 * dist_y * dist_y)
            sum_w += weight
            for c in range(grid.shape[0]):
                out[tid, c] += weight * grid[c, idx_x, idx_y]
    if sum_w > 0.0:
        inv = 1.0 / sum_w
        for c in range(grid.shape[0]):
            out[tid, c] = out[tid, c] * inv


__all__ = ["interp_2d_stride5"]
