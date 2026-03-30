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

"""2D nearest-neighbor point-to-grid backward kernel."""

import warp as wp


@wp.kernel
def point_to_grid_backward_2d_stride1(
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
    center_x = wp.int32((p[0] - origin[0]) / dx[0] + center_offset)
    center_y = wp.int32((p[1] - origin[1]) / dx[1] + center_offset)
    if center_x < 0:
        center_x = 0
    if center_x >= size[0]:
        center_x = size[0] - 1
    if center_y < 0:
        center_y = 0
    if center_y >= size[1]:
        center_y = size[1] - 1

    # Accumulate gradient contributions for per-point input values.
    if compute_values_grad != 0:
        # Accumulate channel contributions for this sample.
        for c in range(point_values.shape[1]):
            grad_point_values[tid, c] = grad_grid_output[c, center_x, center_y]

    # Accumulate gradient contributions for query-point coordinates.
    if compute_query_grad != 0:
        grad_query[tid, 0] = 0.0
        grad_query[tid, 1] = 0.0


__all__ = ["point_to_grid_backward_2d_stride1"]
