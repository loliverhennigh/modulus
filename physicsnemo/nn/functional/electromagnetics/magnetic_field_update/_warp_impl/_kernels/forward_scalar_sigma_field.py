# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warp as wp

from ..utils import _coefficients, _curl_e, _field_components, _periodic_next, _periodic_prev


@wp.kernel
def _magnetic_field_update_kernel_scalar_sigma_field(
    electric_field: wp.array4d(dtype=wp.float32),
    magnetic_field: wp.array4d(dtype=wp.float32),
    sigma_m_field: wp.array3d(dtype=wp.float32),
    output: wp.array4d(dtype=wp.float32),
    mu_scalar: wp.float32,
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
    sigma_values = _field_components(sigma_m_field, i, j, k, i_prev, j_prev, k_prev)

    coeff_x = _coefficients(mu_scalar, sigma_values[0], dt, spacing[0])
    coeff_y = _coefficients(mu_scalar, sigma_values[1], dt, spacing[1])
    coeff_z = _coefficients(mu_scalar, sigma_values[2], dt, spacing[2])

    output[0, i, j, k] = coeff_x[0] * magnetic_field[0, i, j, k] + coeff_x[1] * curl[0]
    output[1, i, j, k] = coeff_y[0] * magnetic_field[1, i, j, k] + coeff_y[1] * curl[1]
    output[2, i, j, k] = coeff_z[0] * magnetic_field[2, i, j, k] + coeff_z[1] * curl[2]
