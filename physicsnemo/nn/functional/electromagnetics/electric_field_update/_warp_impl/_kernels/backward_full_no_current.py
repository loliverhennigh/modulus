# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warp as wp

from ..utils import (
    _accumulate_grad_magnetic,
    _coefficients,
    _curl_h,
    _material_components,
    _material_partials,
    _periodic_prev,
    _scatter_avg_x,
    _scatter_avg_y,
    _scatter_avg_z,
)


@wp.kernel
def _electric_field_update_backward_kernel_full_no_current(
    electric_field: wp.array4d(dtype=wp.float32),
    magnetic_field: wp.array4d(dtype=wp.float32),
    eps_field: wp.array3d(dtype=wp.float32),
    sigma_field: wp.array3d(dtype=wp.float32),
    grad_output: wp.array4d(dtype=wp.float32),
    grad_electric: wp.array4d(dtype=wp.float32),
    grad_magnetic: wp.array4d(dtype=wp.float32),
    grad_eps: wp.array3d(dtype=wp.float32),
    grad_sigma: wp.array3d(dtype=wp.float32),
    eps_scalar: wp.float32,
    sigma_scalar: wp.float32,
    eps_is_scalar: int,
    sigma_is_scalar: int,
    dt: wp.float32,
    spacing: wp.array(dtype=wp.float32),
    write_grad_eps: int,
    write_grad_sigma: int,
):
    i, j, k = wp.tid()
    nx = electric_field.shape[1]
    ny = electric_field.shape[2]
    nz = electric_field.shape[3]

    i_prev = _periodic_prev(i, nx)
    j_prev = _periodic_prev(j, ny)
    k_prev = _periodic_prev(k, nz)

    eps_components = _material_components(
        eps_field, eps_scalar, eps_is_scalar, i, j, k, i_prev, j_prev, k_prev
    )
    sigma_components = _material_components(
        sigma_field, sigma_scalar, sigma_is_scalar, i, j, k, i_prev, j_prev, k_prev
    )

    curl = _curl_h(magnetic_field, i, j, k, i_prev, j_prev, k_prev)
    coeff_x = _coefficients(eps_components[0], sigma_components[0], dt, spacing[0])
    coeff_y = _coefficients(eps_components[1], sigma_components[1], dt, spacing[1])
    coeff_z = _coefficients(eps_components[2], sigma_components[2], dt, spacing[2])

    go_x = grad_output[0, i, j, k]
    go_y = grad_output[1, i, j, k]
    go_z = grad_output[2, i, j, k]

    grad_electric[0, i, j, k] = go_x * coeff_x[0]
    grad_electric[1, i, j, k] = go_y * coeff_y[0]
    grad_electric[2, i, j, k] = go_z * coeff_z[0]

    fx = go_x * coeff_x[1]
    fy = go_y * coeff_y[1]
    fz = go_z * coeff_z[1]
    _accumulate_grad_magnetic(
        grad_magnetic, i, j, k, i_prev, j_prev, k_prev, fx, fy, fz
    )

    if write_grad_eps == 1 or write_grad_sigma == 1:
        partial_x = _material_partials(
            eps_components[0],
            sigma_components[0],
            electric_field[0, i, j, k],
            curl[0],
            0.0,
            dt,
            spacing[0],
        )
        partial_y = _material_partials(
            eps_components[1],
            sigma_components[1],
            electric_field[1, i, j, k],
            curl[1],
            0.0,
            dt,
            spacing[1],
        )
        partial_z = _material_partials(
            eps_components[2],
            sigma_components[2],
            electric_field[2, i, j, k],
            curl[2],
            0.0,
            dt,
            spacing[2],
        )

        if write_grad_eps == 1:
            _scatter_avg_x(
                grad_eps, go_x * partial_x[0], i, j, k, i_prev, j_prev, k_prev
            )
            _scatter_avg_y(
                grad_eps, go_y * partial_y[0], i, j, k, i_prev, j_prev, k_prev
            )
            _scatter_avg_z(
                grad_eps, go_z * partial_z[0], i, j, k, i_prev, j_prev, k_prev
            )

        if write_grad_sigma == 1:
            _scatter_avg_x(
                grad_sigma, go_x * partial_x[1], i, j, k, i_prev, j_prev, k_prev
            )
            _scatter_avg_y(
                grad_sigma, go_y * partial_y[1], i, j, k, i_prev, j_prev, k_prev
            )
            _scatter_avg_z(
                grad_sigma, go_z * partial_z[1], i, j, k, i_prev, j_prev, k_prev
            )
