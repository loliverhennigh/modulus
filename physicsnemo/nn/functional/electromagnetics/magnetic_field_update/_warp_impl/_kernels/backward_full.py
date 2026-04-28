# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warp as wp

from ..utils import (
    _accumulate_grad_electric,
    _coefficients,
    _curl_e,
    _field_components,
    _material_partials,
    _periodic_next,
    _periodic_prev,
    _scatter_component_x,
    _scatter_component_y,
    _scatter_component_z,
)


@wp.kernel
def _magnetic_field_update_backward_kernel_full(
    electric_field: wp.array4d(dtype=wp.float32),
    magnetic_field: wp.array4d(dtype=wp.float32),
    mu_field: wp.array3d(dtype=wp.float32),
    sigma_m_field: wp.array3d(dtype=wp.float32),
    grad_output: wp.array4d(dtype=wp.float32),
    grad_electric: wp.array4d(dtype=wp.float32),
    grad_magnetic: wp.array4d(dtype=wp.float32),
    grad_mu: wp.array3d(dtype=wp.float32),
    grad_sigma_m: wp.array3d(dtype=wp.float32),
    mu_scalar: wp.float32,
    sigma_m_scalar: wp.float32,
    mu_is_scalar: int,
    sigma_is_scalar: int,
    dt: wp.float32,
    spacing: wp.array(dtype=wp.float32),
    need_grad_mu: int,
    need_grad_sigma: int,
):
    i, j, k = wp.tid()

    nx = magnetic_field.shape[1]
    ny = magnetic_field.shape[2]
    nz = magnetic_field.shape[3]

    i_prev = _periodic_prev(i, nx)
    j_prev = _periodic_prev(j, ny)
    k_prev = _periodic_prev(k, nz)

    i_next = _periodic_next(i, nx)
    j_next = _periodic_next(j, ny)
    k_next = _periodic_next(k, nz)

    curl = _curl_e(electric_field, i, j, k, i_next, j_next, k_next)

    if mu_is_scalar == 1:
        mu_values = wp.vec3f(mu_scalar, mu_scalar, mu_scalar)
    else:
        mu_values = _field_components(mu_field, i, j, k, i_prev, j_prev, k_prev)

    if sigma_is_scalar == 1:
        sigma_values = wp.vec3f(sigma_m_scalar, sigma_m_scalar, sigma_m_scalar)
    else:
        sigma_values = _field_components(sigma_m_field, i, j, k, i_prev, j_prev, k_prev)

    coeff_x = _coefficients(mu_values[0], sigma_values[0], dt, spacing[0])
    coeff_y = _coefficients(mu_values[1], sigma_values[1], dt, spacing[1])
    coeff_z = _coefficients(mu_values[2], sigma_values[2], dt, spacing[2])

    grad_x = grad_output[0, i, j, k]
    grad_y = grad_output[1, i, j, k]
    grad_z = grad_output[2, i, j, k]

    h_x = magnetic_field[0, i, j, k]
    h_y = magnetic_field[1, i, j, k]
    h_z = magnetic_field[2, i, j, k]

    wp.atomic_add(grad_magnetic, 0, i, j, k, grad_x * coeff_x[0])
    wp.atomic_add(grad_magnetic, 1, i, j, k, grad_y * coeff_y[0])
    wp.atomic_add(grad_magnetic, 2, i, j, k, grad_z * coeff_z[0])

    grad_curl_x = grad_x * coeff_x[1]
    grad_curl_y = grad_y * coeff_y[1]
    grad_curl_z = grad_z * coeff_z[1]

    _accumulate_grad_electric(
        grad_electric,
        i,
        j,
        k,
        i_next,
        j_next,
        k_next,
        grad_curl_x,
        grad_curl_y,
        grad_curl_z,
    )

    partials_x = _material_partials(
        mu_values[0],
        sigma_values[0],
        h_x,
        curl[0],
        dt,
        spacing[0],
    )
    partials_y = _material_partials(
        mu_values[1],
        sigma_values[1],
        h_y,
        curl[1],
        dt,
        spacing[1],
    )
    partials_z = _material_partials(
        mu_values[2],
        sigma_values[2],
        h_z,
        curl[2],
        dt,
        spacing[2],
    )

    if need_grad_mu == 1 and mu_is_scalar == 0:
        _scatter_component_x(
            grad_mu,
            mu_field,
            grad_x * partials_x[0],
            i,
            j,
            k,
            i_prev,
        )
        _scatter_component_y(
            grad_mu,
            mu_field,
            grad_y * partials_y[0],
            i,
            j,
            k,
            j_prev,
        )
        _scatter_component_z(
            grad_mu,
            mu_field,
            grad_z * partials_z[0],
            i,
            j,
            k,
            k_prev,
        )

    if need_grad_sigma == 1 and sigma_is_scalar == 0:
        _scatter_component_x(
            grad_sigma_m,
            sigma_m_field,
            grad_x * partials_x[1],
            i,
            j,
            k,
            i_prev,
        )
        _scatter_component_y(
            grad_sigma_m,
            sigma_m_field,
            grad_y * partials_y[1],
            i,
            j,
            k,
            j_prev,
        )
        _scatter_component_z(
            grad_sigma_m,
            sigma_m_field,
            grad_z * partials_z[1],
            i,
            j,
            k,
            k_prev,
        )
