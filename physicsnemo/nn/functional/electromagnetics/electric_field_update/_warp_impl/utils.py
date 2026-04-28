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

from __future__ import annotations

import warp as wp

wp.config.quiet = True
wp.init()

@wp.func
def _periodic_prev(index: int, size: int) -> int:
    if index == 0:
        return size - 1
    return index - 1


# Average one scalar field for the Yee Ex location.
@wp.func
def _material_avg_x(
    field: wp.array3d(dtype=wp.float32),
    i: int,
    j: int,
    k: int,
    i_prev: int,
    j_prev: int,
    k_prev: int,
) -> wp.float32:
    _ = i_prev
    return 0.25 * (
        field[i, j, k]
        + field[i, j, k_prev]
        + field[i, j_prev, k]
        + field[i, j_prev, k_prev]
    )


# Average one scalar field for the Yee Ey location.
@wp.func
def _material_avg_y(
    field: wp.array3d(dtype=wp.float32),
    i: int,
    j: int,
    k: int,
    i_prev: int,
    j_prev: int,
    k_prev: int,
) -> wp.float32:
    _ = j_prev
    return 0.25 * (
        field[i, j, k]
        + field[i, j, k_prev]
        + field[i_prev, j, k]
        + field[i_prev, j, k_prev]
    )


# Average one scalar field for the Yee Ez location.
@wp.func
def _material_avg_z(
    field: wp.array3d(dtype=wp.float32),
    i: int,
    j: int,
    k: int,
    i_prev: int,
    j_prev: int,
    k_prev: int,
) -> wp.float32:
    _ = k_prev
    return 0.25 * (
        field[i, j, k]
        + field[i, j_prev, k]
        + field[i_prev, j, k]
        + field[i_prev, j_prev, k]
    )


# Sample impressed current with offset + clipping.
@wp.func
def _sample_current(
    current: wp.array4d(dtype=wp.float32),
    component: int,
    i: int,
    j: int,
    k: int,
    offset_x: int,
    offset_y: int,
    offset_z: int,
    size_x: int,
    size_y: int,
    size_z: int,
) -> wp.float32:
    src_i = i - offset_x
    src_j = j - offset_y
    src_k = k - offset_z

    if src_i < 0 or src_i >= size_x:
        return 0.0
    if src_j < 0 or src_j >= size_y:
        return 0.0
    if src_k < 0 or src_k >= size_z:
        return 0.0

    return current[component, src_i, src_j, src_k]


# Compute curl(H) at one electric-grid cell.
@wp.func
def _curl_h(
    magnetic_field: wp.array4d(dtype=wp.float32),
    i: int,
    j: int,
    k: int,
    i_prev: int,
    j_prev: int,
    k_prev: int,
) -> wp.vec3f:
    hx = magnetic_field[0, i, j, k]
    hy = magnetic_field[1, i, j, k]
    hz = magnetic_field[2, i, j, k]

    m_x_1_0_1 = magnetic_field[0, i, j_prev, k]
    m_x_1_1_0 = magnetic_field[0, i, j, k_prev]
    m_y_0_1_1 = magnetic_field[1, i_prev, j, k]
    m_y_1_1_0 = magnetic_field[1, i, j, k_prev]
    m_z_0_1_1 = magnetic_field[2, i_prev, j, k]
    m_z_1_0_1 = magnetic_field[2, i, j_prev, k]

    curl_h_x = (hz - m_z_1_0_1) - (hy - m_y_1_1_0)
    curl_h_y = (hx - m_x_1_1_0) - (hz - m_z_0_1_1)
    curl_h_z = (hy - m_y_0_1_1) - (hx - m_x_1_0_1)

    return wp.vec3f(curl_h_x, curl_h_y, curl_h_z)


# Compute update coefficients (c_ee, c_eh, c_ej) for one component.
@wp.func
def _coefficients(
    eps_value: wp.float32,
    sigma_value: wp.float32,
    dt: wp.float32,
    spacing_value: wp.float32,
) -> wp.vec3f:
    denom = 2.0 * eps_value + sigma_value * dt
    c_ee = (2.0 * eps_value - sigma_value * dt) / denom
    c_eh = (2.0 * dt) / (spacing_value * denom)
    c_ej = (-2.0 * dt) / denom
    return wp.vec3f(c_ee, c_eh, c_ej)


# Compute local derivative wrt averaged eps/sigma for one component update.
@wp.func
def _material_partials(
    eps_value: wp.float32,
    sigma_value: wp.float32,
    e_value: wp.float32,
    curl_value: wp.float32,
    current_value: wp.float32,
    dt: wp.float32,
    spacing_value: wp.float32,
) -> wp.vec2f:
    denom = 2.0 * eps_value + sigma_value * dt
    inv_denom2 = 1.0 / (denom * denom)

    d_c_ee_d_eps = 4.0 * sigma_value * dt * inv_denom2
    d_c_eh_d_eps = (-4.0 * dt / spacing_value) * inv_denom2
    d_c_ej_d_eps = 4.0 * dt * inv_denom2

    d_c_ee_d_sigma = -4.0 * eps_value * dt * inv_denom2
    d_c_eh_d_sigma = (-2.0 * dt * dt / spacing_value) * inv_denom2
    d_c_ej_d_sigma = 2.0 * dt * dt * inv_denom2

    d_update_d_eps = (
        d_c_ee_d_eps * e_value
        + d_c_eh_d_eps * curl_value
        + d_c_ej_d_eps * current_value
    )
    d_update_d_sigma = (
        d_c_ee_d_sigma * e_value
        + d_c_eh_d_sigma * curl_value
        + d_c_ej_d_sigma * current_value
    )

    return wp.vec2f(d_update_d_eps, d_update_d_sigma)


# Apply one component update from local values and coefficients.
@wp.func
def _update_component(
    e_value: wp.float32,
    curl_value: wp.float32,
    current_value: wp.float32,
    coeffs: wp.vec3f,
) -> wp.float32:
    return coeffs[0] * e_value + coeffs[1] * curl_value + coeffs[2] * current_value


# Resolve per-component material values at Yee locations.
@wp.func
def _material_components(
    field: wp.array3d(dtype=wp.float32),
    scalar_value: wp.float32,
    is_scalar: int,
    i: int,
    j: int,
    k: int,
    i_prev: int,
    j_prev: int,
    k_prev: int,
) -> wp.vec3f:
    if is_scalar == 1:
        return wp.vec3f(scalar_value, scalar_value, scalar_value)
    return wp.vec3f(
        _material_avg_x(field, i, j, k, i_prev, j_prev, k_prev),
        _material_avg_y(field, i, j, k, i_prev, j_prev, k_prev),
        _material_avg_z(field, i, j, k, i_prev, j_prev, k_prev),
    )


# Accumulate gradient contributions into magnetic field neighbors.
@wp.func
def _accumulate_grad_magnetic(
    grad_magnetic: wp.array4d(dtype=wp.float32),
    i: int,
    j: int,
    k: int,
    i_prev: int,
    j_prev: int,
    k_prev: int,
    fx: wp.float32,
    fy: wp.float32,
    fz: wp.float32,
) -> None:
    wp.atomic_add(grad_magnetic, 2, i, j, k, fx)
    wp.atomic_add(grad_magnetic, 2, i, j_prev, k, -fx)
    wp.atomic_add(grad_magnetic, 1, i, j, k, -fx)
    wp.atomic_add(grad_magnetic, 1, i, j, k_prev, fx)

    wp.atomic_add(grad_magnetic, 0, i, j, k, fy)
    wp.atomic_add(grad_magnetic, 0, i, j, k_prev, -fy)
    wp.atomic_add(grad_magnetic, 2, i, j, k, -fy)
    wp.atomic_add(grad_magnetic, 2, i_prev, j, k, fy)

    wp.atomic_add(grad_magnetic, 1, i, j, k, fz)
    wp.atomic_add(grad_magnetic, 1, i_prev, j, k, -fz)
    wp.atomic_add(grad_magnetic, 0, i, j, k, -fz)
    wp.atomic_add(grad_magnetic, 0, i, j_prev, k, fz)


# Scatter one Ex-style averaged gradient contribution to scalar field cells.
@wp.func
def _scatter_avg_x(
    grad_field: wp.array3d(dtype=wp.float32),
    grad_value: wp.float32,
    i: int,
    j: int,
    k: int,
    i_prev: int,
    j_prev: int,
    k_prev: int,
) -> None:
    _ = i_prev
    contrib = 0.25 * grad_value
    wp.atomic_add(grad_field, i, j, k, contrib)
    wp.atomic_add(grad_field, i, j, k_prev, contrib)
    wp.atomic_add(grad_field, i, j_prev, k, contrib)
    wp.atomic_add(grad_field, i, j_prev, k_prev, contrib)


# Scatter one Ey-style averaged gradient contribution to scalar field cells.
@wp.func
def _scatter_avg_y(
    grad_field: wp.array3d(dtype=wp.float32),
    grad_value: wp.float32,
    i: int,
    j: int,
    k: int,
    i_prev: int,
    j_prev: int,
    k_prev: int,
) -> None:
    _ = j_prev
    contrib = 0.25 * grad_value
    wp.atomic_add(grad_field, i, j, k, contrib)
    wp.atomic_add(grad_field, i, j, k_prev, contrib)
    wp.atomic_add(grad_field, i_prev, j, k, contrib)
    wp.atomic_add(grad_field, i_prev, j, k_prev, contrib)


# Scatter one Ez-style averaged gradient contribution to scalar field cells.
@wp.func
def _scatter_avg_z(
    grad_field: wp.array3d(dtype=wp.float32),
    grad_value: wp.float32,
    i: int,
    j: int,
    k: int,
    i_prev: int,
    j_prev: int,
    k_prev: int,
) -> None:
    _ = k_prev
    contrib = 0.25 * grad_value
    wp.atomic_add(grad_field, i, j, k, contrib)
    wp.atomic_add(grad_field, i, j_prev, k, contrib)
    wp.atomic_add(grad_field, i_prev, j, k, contrib)
    wp.atomic_add(grad_field, i_prev, j_prev, k, contrib)
