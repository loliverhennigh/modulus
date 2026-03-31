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

"""1D gaussian point-to-grid forward scatter kernel."""

import warp as wp


@wp.kernel
def point_to_grid_forward_1d_stride5(
    points: wp.array(dtype=wp.float32),
    point_values: wp.array2d(dtype=wp.float32),
    out_grid: wp.array2d(dtype=wp.float32),
    origin: wp.float32,
    dx: wp.float32,
    size_x: int,
    center_offset: wp.float32,
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
        return
    inv_sum_w = 1.0 / sum_w

    for ox in range(-2, 3):
        idx = center + ox
        if idx < 0:
            idx = 0
        if idx >= size_x:
            idx = size_x - 1
        coord = origin + wp.float32(idx) * dx
        dist = (x - coord) / sigma
        weight = wp.exp(-0.5 * dist * dist) * inv_sum_w

        # Accumulate channel contributions for this sample.
        for c in range(point_values.shape[1]):
            wp.atomic_add(out_grid, c, idx, weight * point_values[tid, c])


__all__ = ["point_to_grid_forward_1d_stride5"]
