# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warp as wp


@wp.func
def _periodic_indexing_scalar(
    field: wp.array3d(dtype=wp.float32),
    shape: wp.vec3i,
    i: wp.int32,
    j: wp.int32,
    k: wp.int32,
) -> wp.float32:
    i = (i + shape[0]) % shape[0]
    j = (j + shape[1]) % shape[1]
    k = (k + shape[2]) % shape[2]
    return field[i, j, k]


@wp.func
def _periodic_write_index(index: wp.int32, size: wp.int32) -> wp.int32:
    return (index + size) % size


@wp.func
def _harmonic_average(left: wp.float32, right: wp.float32) -> wp.float32:
    denom = left + right
    if denom == 0.0:
        return 0.0
    return (2.0 * left * right) / denom


@wp.kernel
def _pml_magnetic_field_update_backward_kernel_scalar(
    pml_layer: wp.array4d(dtype=wp.float32),
    grad_output: wp.array4d(dtype=wp.float32),
    grad_pml: wp.array4d(dtype=wp.float32),
    spacing: wp.array(dtype=wp.float32),
    pml_layer_offset: wp.vec3i,
    dt: wp.float32,
    mu_scalar: wp.float32,
    need_grad_pml: int,
):
    i, j, k = wp.tid()

    if need_grad_pml != 1:
        return

    coeff_x = wp.float32(0.0)
    coeff_y = wp.float32(0.0)
    coeff_z = wp.float32(0.0)
    if mu_scalar != 0.0:
        coeff_x = dt / (spacing[0] * mu_scalar)
        coeff_y = dt / (spacing[1] * mu_scalar)
        coeff_z = dt / (spacing[2] * mu_scalar)

    i_h = i + pml_layer_offset[0]
    j_h = j + pml_layer_offset[1]
    k_h = k + pml_layer_offset[2]

    i_w = _periodic_write_index(i_h, grad_output.shape[1])
    j_w = _periodic_write_index(j_h, grad_output.shape[2])
    k_w = _periodic_write_index(k_h, grad_output.shape[3])

    grad_x = grad_output[0, i_w, j_w, k_w]
    grad_y = grad_output[1, i_w, j_w, k_w]
    grad_z = grad_output[2, i_w, j_w, k_w]

    wp.atomic_add(grad_pml, 3, i, j, k, -grad_x * coeff_x)
    wp.atomic_add(grad_pml, 4, i, j, k, -grad_y * coeff_y)
    wp.atomic_add(grad_pml, 5, i, j, k, -grad_z * coeff_z)


@wp.kernel
def _pml_magnetic_field_update_backward_kernel_mu_field(
    pml_layer: wp.array4d(dtype=wp.float32),
    mu_field: wp.array3d(dtype=wp.float32),
    grad_output: wp.array4d(dtype=wp.float32),
    grad_pml: wp.array4d(dtype=wp.float32),
    grad_mu: wp.array3d(dtype=wp.float32),
    spacing: wp.array(dtype=wp.float32),
    pml_layer_offset: wp.vec3i,
    dt: wp.float32,
    need_grad_pml: int,
    need_grad_mu: int,
):
    i, j, k = wp.tid()

    shape = wp.vec3i(
        mu_field.shape[0],
        mu_field.shape[1],
        mu_field.shape[2],
    )

    i_h = i + pml_layer_offset[0]
    j_h = j + pml_layer_offset[1]
    k_h = k + pml_layer_offset[2]

    mu_0_1_1 = _periodic_indexing_scalar(mu_field, shape, i_h - 1, j_h, k_h)
    mu_1_0_1 = _periodic_indexing_scalar(mu_field, shape, i_h, j_h - 1, k_h)
    mu_1_1_0 = _periodic_indexing_scalar(mu_field, shape, i_h, j_h, k_h - 1)
    mu_1_1_1 = _periodic_indexing_scalar(mu_field, shape, i_h, j_h, k_h)

    mu_x = _harmonic_average(mu_1_1_1, mu_0_1_1)
    mu_y = _harmonic_average(mu_1_1_1, mu_1_0_1)
    mu_z = _harmonic_average(mu_1_1_1, mu_1_1_0)

    coeff_x = wp.float32(0.0)
    coeff_y = wp.float32(0.0)
    coeff_z = wp.float32(0.0)
    if spacing[0] != 0.0 and mu_x != 0.0:
        coeff_x = dt / (spacing[0] * mu_x)
    if spacing[1] != 0.0 and mu_y != 0.0:
        coeff_y = dt / (spacing[1] * mu_y)
    if spacing[2] != 0.0 and mu_z != 0.0:
        coeff_z = dt / (spacing[2] * mu_z)

    phi_x = pml_layer[3, i, j, k]
    phi_y = pml_layer[4, i, j, k]
    phi_z = pml_layer[5, i, j, k]

    i_w = _periodic_write_index(i_h, shape[0])
    j_w = _periodic_write_index(j_h, shape[1])
    k_w = _periodic_write_index(k_h, shape[2])

    grad_x = grad_output[0, i_w, j_w, k_w]
    grad_y = grad_output[1, i_w, j_w, k_w]
    grad_z = grad_output[2, i_w, j_w, k_w]

    if need_grad_pml == 1:
        wp.atomic_add(grad_pml, 3, i, j, k, -grad_x * coeff_x)
        wp.atomic_add(grad_pml, 4, i, j, k, -grad_y * coeff_y)
        wp.atomic_add(grad_pml, 5, i, j, k, -grad_z * coeff_z)

    if need_grad_mu == 1:
        grad_mu_x = wp.float32(0.0)
        grad_mu_y = wp.float32(0.0)
        grad_mu_z = wp.float32(0.0)

        if spacing[0] != 0.0 and mu_x != 0.0:
            grad_mu_x = grad_x * phi_x * (dt / (spacing[0] * mu_x * mu_x))
        if spacing[1] != 0.0 and mu_y != 0.0:
            grad_mu_y = grad_y * phi_y * (dt / (spacing[1] * mu_y * mu_y))
        if spacing[2] != 0.0 and mu_z != 0.0:
            grad_mu_z = grad_z * phi_z * (dt / (spacing[2] * mu_z * mu_z))

        i_1_1_1 = _periodic_write_index(i_h, shape[0])
        j_1_1_1 = _periodic_write_index(j_h, shape[1])
        k_1_1_1 = _periodic_write_index(k_h, shape[2])

        i_0_1_1 = _periodic_write_index(i_h - 1, shape[0])
        j_0_1_1 = _periodic_write_index(j_h, shape[1])
        k_0_1_1 = _periodic_write_index(k_h, shape[2])

        i_1_0_1 = _periodic_write_index(i_h, shape[0])
        j_1_0_1 = _periodic_write_index(j_h - 1, shape[1])
        k_1_0_1 = _periodic_write_index(k_h, shape[2])

        i_1_1_0 = _periodic_write_index(i_h, shape[0])
        j_1_1_0 = _periodic_write_index(j_h, shape[1])
        k_1_1_0 = _periodic_write_index(k_h - 1, shape[2])

        denom_x = mu_1_1_1 + mu_0_1_1
        if denom_x != 0.0:
            denom_x_sq = denom_x * denom_x
            dmu_x_d_mu_111 = (2.0 * mu_0_1_1 * mu_0_1_1) / denom_x_sq
            dmu_x_d_mu_011 = (2.0 * mu_1_1_1 * mu_1_1_1) / denom_x_sq
            wp.atomic_add(grad_mu, i_1_1_1, j_1_1_1, k_1_1_1, grad_mu_x * dmu_x_d_mu_111)
            wp.atomic_add(grad_mu, i_0_1_1, j_0_1_1, k_0_1_1, grad_mu_x * dmu_x_d_mu_011)

        denom_y = mu_1_1_1 + mu_1_0_1
        if denom_y != 0.0:
            denom_y_sq = denom_y * denom_y
            dmu_y_d_mu_111 = (2.0 * mu_1_0_1 * mu_1_0_1) / denom_y_sq
            dmu_y_d_mu_101 = (2.0 * mu_1_1_1 * mu_1_1_1) / denom_y_sq
            wp.atomic_add(grad_mu, i_1_1_1, j_1_1_1, k_1_1_1, grad_mu_y * dmu_y_d_mu_111)
            wp.atomic_add(grad_mu, i_1_0_1, j_1_0_1, k_1_0_1, grad_mu_y * dmu_y_d_mu_101)

        denom_z = mu_1_1_1 + mu_1_1_0
        if denom_z != 0.0:
            denom_z_sq = denom_z * denom_z
            dmu_z_d_mu_111 = (2.0 * mu_1_1_0 * mu_1_1_0) / denom_z_sq
            dmu_z_d_mu_110 = (2.0 * mu_1_1_1 * mu_1_1_1) / denom_z_sq
            wp.atomic_add(grad_mu, i_1_1_1, j_1_1_1, k_1_1_1, grad_mu_z * dmu_z_d_mu_111)
            wp.atomic_add(grad_mu, i_1_1_0, j_1_1_0, k_1_1_0, grad_mu_z * dmu_z_d_mu_110)
