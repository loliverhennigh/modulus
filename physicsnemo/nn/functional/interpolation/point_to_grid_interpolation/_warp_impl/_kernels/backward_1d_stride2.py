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

"""1D linear/smooth point-to-grid backward kernel."""

import warp as wp

from ..utils import basis_derivative, basis_value


@wp.kernel
def point_to_grid_backward_1d_stride2(
    points: wp.array(dtype=wp.float32),
    point_values: wp.array2d(dtype=wp.float32),
    grad_grid_output: wp.array2d(dtype=wp.float32),
    grad_query: wp.array2d(dtype=wp.float32),
    grad_point_values: wp.array2d(dtype=wp.float32),
    origin: wp.float32,
    dx: wp.float32,
    size_x: int,
    interp_id: int,
    compute_query_grad: int,
    compute_values_grad: int,
):
    tid = wp.tid()

    # Map one Warp thread to one query/scatter sample.

    # Convert world-space coordinates into grid-space coordinates.
    pos = (points[tid] - origin) / dx
    center = wp.int32(pos)
    frac = pos - wp.float32(center)
    lower = basis_value(interp_id, frac)
    upper = basis_value(interp_id, 1.0 - frac)
    d_lower = basis_derivative(interp_id, frac) / dx
    d_upper = -basis_derivative(interp_id, 1.0 - frac) / dx

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

    grad_x = wp.float32(0.0)

    # Accumulate channel contributions for this sample.
    for c in range(point_values.shape[1]):
        g0 = grad_grid_output[c, idx0]
        g1 = grad_grid_output[c, idx1]

        # Accumulate gradient contributions for per-point input values.
        if compute_values_grad != 0:
            grad_point_values[tid, c] = upper * g0 + lower * g1

        # Accumulate gradient contributions for query-point coordinates.
        if compute_query_grad != 0:
            value = point_values[tid, c]
            grad_x += value * (g0 * d_upper + g1 * d_lower)

    if compute_query_grad != 0:
        grad_query[tid, 0] = grad_x


__all__ = ["point_to_grid_backward_1d_stride2"]
