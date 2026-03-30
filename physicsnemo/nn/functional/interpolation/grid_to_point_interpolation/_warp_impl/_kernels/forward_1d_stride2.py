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

"""1D linear/smooth forward interpolation kernel."""

import warp as wp

from ..utils import basis_value


@wp.kernel
def interp_1d_stride2(
    points: wp.array(dtype=wp.float32),
    grid: wp.array2d(dtype=wp.float32),
    out: wp.array2d(dtype=wp.float32),
    origin: wp.float32,
    dx: wp.float32,
    size_x: int,
    interp_id: int,
):
    tid = wp.tid()

    # Map one Warp thread to one query/scatter sample.
    x = points[tid]

    # Convert world-space coordinates into grid-space coordinates.
    pos = (x - origin) / dx
    center = wp.int32(pos)
    frac = pos - wp.float32(center)
    lower = basis_value(interp_id, frac)
    upper = basis_value(interp_id, 1.0 - frac)

    # Clamp stencil indices so boundary samples stay in bounds.
    idx0 = center
    idx1 = center + 1
    if idx0 < 0:
        idx0 = 0
    if idx0 >= size_x:
        idx0 = size_x - 1
    if idx1 < 0:
        idx1 = 0
    if idx1 >= size_x:
        idx1 = size_x - 1

    # Accumulate channel contributions for this sample.
    for c in range(grid.shape[0]):
        out[tid, c] = upper * grid[c, idx0] + lower * grid[c, idx1]


__all__ = ["interp_1d_stride2"]
