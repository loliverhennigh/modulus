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

"""3D linear/smooth forward interpolation kernel."""

import warp as wp

from ..utils import basis_value


@wp.kernel
def interp_3d_stride2(
    points: wp.array(dtype=wp.vec3f),
    grid: wp.array4d(dtype=wp.float32),
    out: wp.array2d(dtype=wp.float32),
    origin: wp.vec3f,
    dx: wp.vec3f,
    size: wp.vec3i,
    interp_id: int,
):
    tid = wp.tid()

    # Map one Warp thread to one query/scatter sample.
    p = points[tid]

    # Convert world-space coordinates into grid-space coordinates.
    pos = wp.vec3f(
        (p[0] - origin[0]) / dx[0],
        (p[1] - origin[1]) / dx[1],
        (p[2] - origin[2]) / dx[2],
    )
    center_x = wp.int32(pos[0])
    center_y = wp.int32(pos[1])
    center_z = wp.int32(pos[2])
    frac_x = pos[0] - wp.float32(center_x)
    frac_y = pos[1] - wp.float32(center_y)
    frac_z = pos[2] - wp.float32(center_z)
    lower_x = basis_value(interp_id, frac_x)
    upper_x = basis_value(interp_id, 1.0 - frac_x)
    lower_y = basis_value(interp_id, frac_y)
    upper_y = basis_value(interp_id, 1.0 - frac_y)
    lower_z = basis_value(interp_id, frac_z)
    upper_z = basis_value(interp_id, 1.0 - frac_z)

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

    # Accumulate channel contributions for this sample.
    for c in range(grid.shape[0]):
        out[tid, c] = (
            upper_x * upper_y * upper_z * grid[c, idx_x0, idx_y0, idx_z0]
            + upper_x * upper_y * lower_z * grid[c, idx_x0, idx_y0, idx_z1]
            + upper_x * lower_y * upper_z * grid[c, idx_x0, idx_y1, idx_z0]
            + upper_x * lower_y * lower_z * grid[c, idx_x0, idx_y1, idx_z1]
            + lower_x * upper_y * upper_z * grid[c, idx_x1, idx_y0, idx_z0]
            + lower_x * upper_y * lower_z * grid[c, idx_x1, idx_y0, idx_z1]
            + lower_x * lower_y * upper_z * grid[c, idx_x1, idx_y1, idx_z0]
            + lower_x * lower_y * lower_z * grid[c, idx_x1, idx_y1, idx_z1]
        )


__all__ = ["interp_3d_stride2"]
