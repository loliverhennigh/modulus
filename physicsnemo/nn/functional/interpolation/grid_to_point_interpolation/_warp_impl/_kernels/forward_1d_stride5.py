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

"""1D gaussian forward interpolation kernel."""

import warp as wp


@wp.kernel
def interp_1d_stride5(
    points: wp.array(dtype=wp.float32),
    grid: wp.array2d(dtype=wp.float32),
    out: wp.array2d(dtype=wp.float32),
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
    sum_w = 0.0

    # Accumulate channel contributions for this sample.
    for c in range(grid.shape[0]):
        out[tid, c] = 0.0
    for ox in range(-2, 3):
        idx = center + ox
        if idx < 0:
            idx = 0
        if idx >= size_x:
            idx = size_x - 1
        coord = origin + wp.float32(idx) * dx
        dist = (x - coord) / sigma
        weight = wp.exp(-0.5 * dist * dist)
        sum_w += weight
        for c in range(grid.shape[0]):
            out[tid, c] += weight * grid[c, idx]
    if sum_w > 0.0:
        inv = 1.0 / sum_w
        for c in range(grid.shape[0]):
            out[tid, c] = out[tid, c] * inv


__all__ = ["interp_1d_stride5"]
