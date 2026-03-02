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

"""3D nearest-neighbor point-to-grid forward scatter kernel."""

import warp as wp


@wp.kernel
def point_to_grid_forward_3d_stride1(
    points: wp.array(dtype=wp.vec3f),
    point_values: wp.array2d(dtype=wp.float32),
    out_grid: wp.array4d(dtype=wp.float32),
    origin: wp.vec3f,
    dx: wp.vec3f,
    size: wp.vec3i,
    center_offset: wp.float32,
):
    tid = wp.tid()

    # Map one Warp thread to one query/scatter sample.
    p = points[tid]
    center_x = wp.int32((p[0] - origin[0]) / dx[0] + center_offset)
    center_y = wp.int32((p[1] - origin[1]) / dx[1] + center_offset)
    center_z = wp.int32((p[2] - origin[2]) / dx[2] + center_offset)
    if center_x < 0:
        center_x = 0
    if center_x >= size[0]:
        center_x = size[0] - 1
    if center_y < 0:
        center_y = 0
    if center_y >= size[1]:
        center_y = size[1] - 1
    if center_z < 0:
        center_z = 0
    if center_z >= size[2]:
        center_z = size[2] - 1

    # Accumulate channel contributions for this sample.
    for c in range(point_values.shape[1]):
        wp.atomic_add(out_grid, c, center_x, center_y, center_z, point_values[tid, c])


__all__ = ["point_to_grid_forward_3d_stride1"]
