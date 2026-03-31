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

"""1D nearest-neighbor point-to-grid forward scatter kernel."""

import warp as wp


@wp.kernel
def point_to_grid_forward_1d_stride1(
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
    center = wp.int32((x - origin) / dx + center_offset)

    # Clamp stencil indices so boundary samples stay in bounds.
    if center < 0:
        center = 0
    if center >= size_x:
        center = size_x - 1

    # Accumulate channel contributions for this sample.
    for c in range(point_values.shape[1]):
        wp.atomic_add(out_grid, c, center, point_values[tid, c])


__all__ = ["point_to_grid_forward_1d_stride1"]
