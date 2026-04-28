# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warp as wp

from ..utils import (
    _coefficients,
    _curl_h,
    _material_avg_x,
    _material_avg_y,
    _material_avg_z,
    _periodic_prev,
    _sample_current,
    _update_component,
)


@wp.kernel
def _electric_field_update_kernel_scalar_sigma_field(
    electric_field: wp.array4d(dtype=wp.float32),
    magnetic_field: wp.array4d(dtype=wp.float32),
    sigma_field: wp.array3d(dtype=wp.float32),
    impressed_current: wp.array4d(dtype=wp.float32),
    output: wp.array4d(dtype=wp.float32),
    eps_scalar: wp.float32,
    dt: wp.float32,
    spacing: wp.array(dtype=wp.float32),
    offset_x: int,
    offset_y: int,
    offset_z: int,
    current_x: int,
    current_y: int,
    current_z: int,
):
    i, j, k = wp.tid()
    nx = electric_field.shape[1]
    ny = electric_field.shape[2]
    nz = electric_field.shape[3]

    i_prev = _periodic_prev(i, nx)
    j_prev = _periodic_prev(j, ny)
    k_prev = _periodic_prev(k, nz)

    sigma_x = _material_avg_x(sigma_field, i, j, k, i_prev, j_prev, k_prev)
    sigma_y = _material_avg_y(sigma_field, i, j, k, i_prev, j_prev, k_prev)
    sigma_z = _material_avg_z(sigma_field, i, j, k, i_prev, j_prev, k_prev)

    curl = _curl_h(magnetic_field, i, j, k, i_prev, j_prev, k_prev)
    j_x = _sample_current(
        impressed_current,
        0,
        i,
        j,
        k,
        offset_x,
        offset_y,
        offset_z,
        current_x,
        current_y,
        current_z,
    )
    j_y = _sample_current(
        impressed_current,
        1,
        i,
        j,
        k,
        offset_x,
        offset_y,
        offset_z,
        current_x,
        current_y,
        current_z,
    )
    j_z = _sample_current(
        impressed_current,
        2,
        i,
        j,
        k,
        offset_x,
        offset_y,
        offset_z,
        current_x,
        current_y,
        current_z,
    )

    coeff_x = _coefficients(eps_scalar, sigma_x, dt, spacing[0])
    coeff_y = _coefficients(eps_scalar, sigma_y, dt, spacing[1])
    coeff_z = _coefficients(eps_scalar, sigma_z, dt, spacing[2])

    output[0, i, j, k] = _update_component(
        electric_field[0, i, j, k], curl[0], j_x, coeff_x
    )
    output[1, i, j, k] = _update_component(
        electric_field[1, i, j, k], curl[1], j_y, coeff_y
    )
    output[2, i, j, k] = _update_component(
        electric_field[2, i, j, k], curl[2], j_z, coeff_z
    )
