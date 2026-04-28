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


@wp.func
def _safe_c_he(
    mu_value: wp.float32,
    spacing_value: wp.float32,
    dt: wp.float32,
) -> wp.float32:
    denom = spacing_value * (2.0 * mu_value)
    if denom == 0.0:
        return 0.0
    return (2.0 * dt) / denom


@wp.kernel
def _pml_magnetic_field_update_kernel_mu_field(
    magnetic_field: wp.array4d(dtype=wp.float32),
    pml_layer: wp.array4d(dtype=wp.float32),
    mu_field: wp.array3d(dtype=wp.float32),
    spacing: wp.array(dtype=wp.float32),
    pml_layer_offset: wp.vec3i,
    dt: wp.float32,
):
    i, j, k = wp.tid()

    shape = wp.vec3i(
        magnetic_field.shape[1],
        magnetic_field.shape[2],
        magnetic_field.shape[3],
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

    c_he = wp.vec3f(
        _safe_c_he(mu_x, spacing[0], dt),
        _safe_c_he(mu_y, spacing[1], dt),
        _safe_c_he(mu_z, spacing[2], dt),
    )

    phi_h = wp.vec3f(
        pml_layer[3, i, j, k],
        pml_layer[4, i, j, k],
        pml_layer[5, i, j, k],
    )
    h_add = wp.cw_mul(c_he, phi_h)

    i_w = _periodic_write_index(i_h, shape[0])
    j_w = _periodic_write_index(j_h, shape[1])
    k_w = _periodic_write_index(k_h, shape[2])

    wp.atomic_add(magnetic_field, 0, i_w, j_w, k_w, -h_add[0])
    wp.atomic_add(magnetic_field, 1, i_w, j_w, k_w, -h_add[1])
    wp.atomic_add(magnetic_field, 2, i_w, j_w, k_w, -h_add[2])


@wp.kernel
def _pml_magnetic_field_update_kernel_scalar(
    magnetic_field: wp.array4d(dtype=wp.float32),
    pml_layer: wp.array4d(dtype=wp.float32),
    mu_scalar: wp.float32,
    spacing: wp.array(dtype=wp.float32),
    pml_layer_offset: wp.vec3i,
    dt: wp.float32,
):
    i, j, k = wp.tid()

    shape = wp.vec3i(
        magnetic_field.shape[1],
        magnetic_field.shape[2],
        magnetic_field.shape[3],
    )

    i_h = i + pml_layer_offset[0]
    j_h = j + pml_layer_offset[1]
    k_h = k + pml_layer_offset[2]

    c_he = wp.vec3f(
        _safe_c_he(mu_scalar, spacing[0], dt),
        _safe_c_he(mu_scalar, spacing[1], dt),
        _safe_c_he(mu_scalar, spacing[2], dt),
    )

    phi_h = wp.vec3f(
        pml_layer[3, i, j, k],
        pml_layer[4, i, j, k],
        pml_layer[5, i, j, k],
    )
    h_add = wp.cw_mul(c_he, phi_h)

    i_w = _periodic_write_index(i_h, shape[0])
    j_w = _periodic_write_index(j_h, shape[1])
    k_w = _periodic_write_index(k_h, shape[2])

    wp.atomic_add(magnetic_field, 0, i_w, j_w, k_w, -h_add[0])
    wp.atomic_add(magnetic_field, 1, i_w, j_w, k_w, -h_add[1])
    wp.atomic_add(magnetic_field, 2, i_w, j_w, k_w, -h_add[2])
