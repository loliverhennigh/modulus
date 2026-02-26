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

"""Shared constants and helpers for Warp point-to-grid interpolation kernels."""

import warp as wp

# Define interpolation identifiers used by both Python and Warp kernels.
_INTERP_NEAREST = 0
_INTERP_LINEAR = 1
_INTERP_SMOOTH_1 = 2
_INTERP_SMOOTH_2 = 3
_INTERP_GAUSSIAN = 4

# Map interpolation names to internal ids.
_INTERP_NAME_TO_ID = {
    "nearest_neighbor": _INTERP_NEAREST,
    "linear": _INTERP_LINEAR,
    "smooth_step_1": _INTERP_SMOOTH_1,
    "smooth_step_2": _INTERP_SMOOTH_2,
    "gaussian": _INTERP_GAUSSIAN,
}

# Map interpolation ids to neighborhood stride.
_INTERP_ID_TO_STRIDE = {
    _INTERP_NEAREST: 1,
    _INTERP_LINEAR: 2,
    _INTERP_SMOOTH_1: 2,
    _INTERP_SMOOTH_2: 2,
    _INTERP_GAUSSIAN: 5,
}

# Initialize Warp once for kernel launch.
wp.config.quiet = True
wp.init()


# Define scalar basis functions used by linear and smooth interpolation modes.
@wp.func
def smooth_step_1(x: wp.float32) -> wp.float32:
    return wp.clamp(3.0 * x * x - 2.0 * x * x * x, 0.0, 1.0)


@wp.func
def smooth_step_2(x: wp.float32) -> wp.float32:
    return wp.clamp(x * x * x * (6.0 * x * x - 15.0 * x + 10.0), 0.0, 1.0)


@wp.func
def basis_value(interp_id: int, x: wp.float32) -> wp.float32:
    if interp_id == _INTERP_SMOOTH_1:
        return smooth_step_1(x)
    if interp_id == _INTERP_SMOOTH_2:
        return smooth_step_2(x)
    return x


@wp.func
def basis_derivative(interp_id: int, x: wp.float32) -> wp.float32:
    if x < 0.0 or x > 1.0:
        return 0.0
    if interp_id == _INTERP_LINEAR:
        return 1.0
    if interp_id == _INTERP_SMOOTH_1:
        return 6.0 * x - 6.0 * x * x
    if interp_id == _INTERP_SMOOTH_2:
        return 30.0 * x * x * (x - 1.0) * (x - 1.0)
    return 0.0


__all__ = [
    "_INTERP_GAUSSIAN",
    "_INTERP_ID_TO_STRIDE",
    "_INTERP_LINEAR",
    "_INTERP_NAME_TO_ID",
    "_INTERP_NEAREST",
    "_INTERP_SMOOTH_1",
    "_INTERP_SMOOTH_2",
    "basis_derivative",
    "basis_value",
]
