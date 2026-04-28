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
    _periodic_next,
    _periodic_prev,
)


@wp.kernel
def _magnetic_field_update_backward_kernel_fields(
    electric_field: wp.array4d(dtype=wp.float32),
    magnetic_field: wp.array4d(dtype=wp.float32),
    mu_field: wp.array3d(dtype=wp.float32),
    sigma_m_field: wp.array3d(dtype=wp.float32),
    grad_output: wp.array4d(dtype=wp.float32),
    grad_electric: wp.array4d(dtype=wp.float32),
    grad_magnetic: wp.array4d(dtype=wp.float32),
    mu_scalar: wp.float32,
    sigma_m_scalar: wp.float32,
    mu_is_scalar: int,
    sigma_is_scalar: int,
    dt: wp.float32,
    spacing: wp.array(dtype=wp.float32),
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

    wp.atomic_add(grad_magnetic, 0, i, j, k, grad_x * coeff_x[0])
    wp.atomic_add(grad_magnetic, 1, i, j, k, grad_y * coeff_y[0])
    wp.atomic_add(grad_magnetic, 2, i, j, k, grad_z * coeff_z[0])

    _accumulate_grad_electric(
        grad_electric,
        i,
        j,
        k,
        i_next,
        j_next,
        k_next,
        grad_x * coeff_x[1],
        grad_y * coeff_y[1],
        grad_z * coeff_z[1],
    )
