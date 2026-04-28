# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warp as wp

wp.config.quiet = True
wp.init()


@wp.func
def _periodic_prev(index: int, size: int) -> int:
    if index == 0:
        return size - 1
    return index - 1


@wp.func
def _periodic_next(index: int, size: int) -> int:
    if index == size - 1:
        return 0
    return index + 1


@wp.func
def _harmonic_average(left: wp.float32, right: wp.float32) -> wp.float32:
    denom = left + right
    if denom == 0.0:
        return 0.0
    return (2.0 * left * right) / denom


@wp.func
def _harmonic_average_partials(left: wp.float32, right: wp.float32) -> wp.vec2f:
    denom = left + right
    if denom == 0.0:
        return wp.vec2f(0.0, 0.0)
    inv_denom2 = 1.0 / (denom * denom)
    d_left = 2.0 * right * right * inv_denom2
    d_right = 2.0 * left * left * inv_denom2
    return wp.vec2f(d_left, d_right)


@wp.func
def _field_component_x(
    field: wp.array3d(dtype=wp.float32),
    i: int,
    j: int,
    k: int,
    i_prev: int,
) -> wp.float32:
    return _harmonic_average(field[i, j, k], field[i_prev, j, k])


@wp.func
def _field_component_y(
    field: wp.array3d(dtype=wp.float32),
    i: int,
    j: int,
    k: int,
    j_prev: int,
) -> wp.float32:
    return _harmonic_average(field[i, j, k], field[i, j_prev, k])


@wp.func
def _field_component_z(
    field: wp.array3d(dtype=wp.float32),
    i: int,
    j: int,
    k: int,
    k_prev: int,
) -> wp.float32:
    return _harmonic_average(field[i, j, k], field[i, j, k_prev])


@wp.func
def _field_components(
    field: wp.array3d(dtype=wp.float32),
    i: int,
    j: int,
    k: int,
    i_prev: int,
    j_prev: int,
    k_prev: int,
) -> wp.vec3f:
    return wp.vec3f(
        _field_component_x(field, i, j, k, i_prev),
        _field_component_y(field, i, j, k, j_prev),
        _field_component_z(field, i, j, k, k_prev),
    )


@wp.func
def _curl_e(
    electric_field: wp.array4d(dtype=wp.float32),
    i: int,
    j: int,
    k: int,
    i_next: int,
    j_next: int,
    k_next: int,
) -> wp.vec3f:
    ex = electric_field[0, i, j, k]
    ey = electric_field[1, i, j, k]
    ez = electric_field[2, i, j, k]

    e_x_0_1_0 = electric_field[0, i, j_next, k]
    e_x_0_0_1 = electric_field[0, i, j, k_next]
    e_y_1_0_0 = electric_field[1, i_next, j, k]
    e_y_0_0_1 = electric_field[1, i, j, k_next]
    e_z_1_0_0 = electric_field[2, i_next, j, k]
    e_z_0_1_0 = electric_field[2, i, j_next, k]

    curl_e_x = (e_y_0_0_1 - ey) - (e_z_0_1_0 - ez)
    curl_e_y = (e_z_1_0_0 - ez) - (e_x_0_0_1 - ex)
    curl_e_z = (e_x_0_1_0 - ex) - (e_y_1_0_0 - ey)

    return wp.vec3f(curl_e_x, curl_e_y, curl_e_z)


@wp.func
def _coefficients(
    mu_value: wp.float32,
    sigma_value: wp.float32,
    dt: wp.float32,
    spacing_value: wp.float32,
) -> wp.vec2f:
    denom = 2.0 * mu_value + sigma_value * dt
    c_hh = (2.0 * mu_value - sigma_value * dt) / denom
    c_he = (2.0 * dt) / (spacing_value * denom)
    return wp.vec2f(c_hh, c_he)


@wp.func
def _material_partials(
    mu_value: wp.float32,
    sigma_value: wp.float32,
    h_value: wp.float32,
    curl_value: wp.float32,
    dt: wp.float32,
    spacing_value: wp.float32,
) -> wp.vec2f:
    denom = 2.0 * mu_value + sigma_value * dt
    inv_denom2 = 1.0 / (denom * denom)

    d_c_hh_d_mu = 4.0 * sigma_value * dt * inv_denom2
    d_c_he_d_mu = (-4.0 * dt / spacing_value) * inv_denom2

    d_c_hh_d_sigma = -4.0 * mu_value * dt * inv_denom2
    d_c_he_d_sigma = (-2.0 * dt * dt / spacing_value) * inv_denom2

    d_update_d_mu = d_c_hh_d_mu * h_value + d_c_he_d_mu * curl_value
    d_update_d_sigma = d_c_hh_d_sigma * h_value + d_c_he_d_sigma * curl_value

    return wp.vec2f(d_update_d_mu, d_update_d_sigma)


@wp.func
def _accumulate_grad_electric(
    grad_electric: wp.array4d(dtype=wp.float32),
    i: int,
    j: int,
    k: int,
    i_next: int,
    j_next: int,
    k_next: int,
    grad_curl_x: wp.float32,
    grad_curl_y: wp.float32,
    grad_curl_z: wp.float32,
) -> None:
    # curl_x = (Ey[k+1] - Ey) - (Ez[j+1] - Ez)
    wp.atomic_add(grad_electric, 1, i, j, k_next, grad_curl_x)
    wp.atomic_add(grad_electric, 1, i, j, k, -grad_curl_x)
    wp.atomic_add(grad_electric, 2, i, j_next, k, -grad_curl_x)
    wp.atomic_add(grad_electric, 2, i, j, k, grad_curl_x)

    # curl_y = (Ez[i+1] - Ez) - (Ex[k+1] - Ex)
    wp.atomic_add(grad_electric, 2, i_next, j, k, grad_curl_y)
    wp.atomic_add(grad_electric, 2, i, j, k, -grad_curl_y)
    wp.atomic_add(grad_electric, 0, i, j, k_next, -grad_curl_y)
    wp.atomic_add(grad_electric, 0, i, j, k, grad_curl_y)

    # curl_z = (Ex[j+1] - Ex) - (Ey[i+1] - Ey)
    wp.atomic_add(grad_electric, 0, i, j_next, k, grad_curl_z)
    wp.atomic_add(grad_electric, 0, i, j, k, -grad_curl_z)
    wp.atomic_add(grad_electric, 1, i_next, j, k, -grad_curl_z)
    wp.atomic_add(grad_electric, 1, i, j, k, grad_curl_z)


@wp.func
def _scatter_component_x(
    grad_field: wp.array3d(dtype=wp.float32),
    field: wp.array3d(dtype=wp.float32),
    grad_component: wp.float32,
    i: int,
    j: int,
    k: int,
    i_prev: int,
) -> None:
    center = field[i, j, k]
    prev = field[i_prev, j, k]
    partials = _harmonic_average_partials(center, prev)
    wp.atomic_add(grad_field, i, j, k, grad_component * partials[0])
    wp.atomic_add(grad_field, i_prev, j, k, grad_component * partials[1])


@wp.func
def _scatter_component_y(
    grad_field: wp.array3d(dtype=wp.float32),
    field: wp.array3d(dtype=wp.float32),
    grad_component: wp.float32,
    i: int,
    j: int,
    k: int,
    j_prev: int,
) -> None:
    center = field[i, j, k]
    prev = field[i, j_prev, k]
    partials = _harmonic_average_partials(center, prev)
    wp.atomic_add(grad_field, i, j, k, grad_component * partials[0])
    wp.atomic_add(grad_field, i, j_prev, k, grad_component * partials[1])


@wp.func
def _scatter_component_z(
    grad_field: wp.array3d(dtype=wp.float32),
    field: wp.array3d(dtype=wp.float32),
    grad_component: wp.float32,
    i: int,
    j: int,
    k: int,
    k_prev: int,
) -> None:
    center = field[i, j, k]
    prev = field[i, j, k_prev]
    partials = _harmonic_average_partials(center, prev)
    wp.atomic_add(grad_field, i, j, k, grad_component * partials[0])
    wp.atomic_add(grad_field, i, j, k_prev, grad_component * partials[1])
