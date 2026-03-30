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

"""2D linear/smooth point-to-grid forward scatter kernel."""

import warp as wp

from ..utils import basis_value


@wp.kernel
def point_to_grid_forward_2d_stride2(
    points: wp.array(dtype=wp.vec2f),
    point_values: wp.array2d(dtype=wp.float32),
    out_grid: wp.array3d(dtype=wp.float32),
    origin: wp.vec2f,
    dx: wp.vec2f,
    size: wp.vec2i,
    interp_id: int,
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

    # Accumulate channel contributions for this sample.
    for c in range(point_values.shape[1]):
        value = point_values[tid, c]
        wp.atomic_add(out_grid, c, idx_x0, idx_y0, w00 * value)
        wp.atomic_add(out_grid, c, idx_x0, idx_y1, w01 * value)
        wp.atomic_add(out_grid, c, idx_x1, idx_y0, w10 * value)
        wp.atomic_add(out_grid, c, idx_x1, idx_y1, w11 * value)


__all__ = ["point_to_grid_forward_2d_stride2"]
