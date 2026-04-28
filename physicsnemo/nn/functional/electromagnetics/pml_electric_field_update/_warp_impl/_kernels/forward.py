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
def _safe_c_eh(
    eps_value: wp.float32,
    spacing_value: wp.float32,
    dt: wp.float32,
) -> wp.float32:
    denom = spacing_value * (2.0 * eps_value)
    if denom == 0.0:
        return 0.0
    return (2.0 * dt) / denom


@wp.kernel
def _pml_electric_field_update_kernel_eps_field(
    electric_field: wp.array4d(dtype=wp.float32),
    pml_layer: wp.array4d(dtype=wp.float32),
    eps_field: wp.array3d(dtype=wp.float32),
    spacing: wp.array(dtype=wp.float32),
    pml_layer_offset: wp.vec3i,
    dt: wp.float32,
):
    i, j, k = wp.tid()

    shape = wp.vec3i(
        electric_field.shape[1],
        electric_field.shape[2],
        electric_field.shape[3],
    )

    i_e = i + pml_layer_offset[0]
    j_e = j + pml_layer_offset[1]
    k_e = k + pml_layer_offset[2]

    eps_0_0_1 = _periodic_indexing_scalar(eps_field, shape, i_e - 1, j_e - 1, k_e)
    eps_0_1_0 = _periodic_indexing_scalar(eps_field, shape, i_e - 1, j_e, k_e - 1)
    eps_0_1_1 = _periodic_indexing_scalar(eps_field, shape, i_e - 1, j_e, k_e)
    eps_1_0_0 = _periodic_indexing_scalar(eps_field, shape, i_e, j_e - 1, k_e - 1)
    eps_1_0_1 = _periodic_indexing_scalar(eps_field, shape, i_e, j_e - 1, k_e)
    eps_1_1_0 = _periodic_indexing_scalar(eps_field, shape, i_e, j_e, k_e - 1)
    eps_1_1_1 = _periodic_indexing_scalar(eps_field, shape, i_e, j_e, k_e)

    eps_x = 0.25 * (eps_1_1_1 + eps_1_1_0 + eps_1_0_1 + eps_1_0_0)
    eps_y = 0.25 * (eps_1_1_1 + eps_1_1_0 + eps_0_1_1 + eps_0_1_0)
    eps_z = 0.25 * (eps_1_1_1 + eps_1_0_1 + eps_0_1_1 + eps_0_0_1)

    c_eh = wp.vec3f(
        _safe_c_eh(eps_x, spacing[0], dt),
        _safe_c_eh(eps_y, spacing[1], dt),
        _safe_c_eh(eps_z, spacing[2], dt),
    )

    phi_e = wp.vec3f(
        pml_layer[0, i, j, k],
        pml_layer[1, i, j, k],
        pml_layer[2, i, j, k],
    )
    e_add = wp.cw_mul(c_eh, phi_e)

    i_w = _periodic_write_index(i_e, shape[0])
    j_w = _periodic_write_index(j_e, shape[1])
    k_w = _periodic_write_index(k_e, shape[2])

    wp.atomic_add(electric_field, 0, i_w, j_w, k_w, e_add[0])
    wp.atomic_add(electric_field, 1, i_w, j_w, k_w, e_add[1])
    wp.atomic_add(electric_field, 2, i_w, j_w, k_w, e_add[2])


@wp.kernel
def _pml_electric_field_update_kernel_scalar(
    electric_field: wp.array4d(dtype=wp.float32),
    pml_layer: wp.array4d(dtype=wp.float32),
    eps_scalar: wp.float32,
    spacing: wp.array(dtype=wp.float32),
    pml_layer_offset: wp.vec3i,
    dt: wp.float32,
):
    i, j, k = wp.tid()

    shape = wp.vec3i(
        electric_field.shape[1],
        electric_field.shape[2],
        electric_field.shape[3],
    )

    i_e = i + pml_layer_offset[0]
    j_e = j + pml_layer_offset[1]
    k_e = k + pml_layer_offset[2]

    c_eh = wp.vec3f(
        _safe_c_eh(eps_scalar, spacing[0], dt),
        _safe_c_eh(eps_scalar, spacing[1], dt),
        _safe_c_eh(eps_scalar, spacing[2], dt),
    )

    phi_e = wp.vec3f(
        pml_layer[0, i, j, k],
        pml_layer[1, i, j, k],
        pml_layer[2, i, j, k],
    )
    e_add = wp.cw_mul(c_eh, phi_e)

    i_w = _periodic_write_index(i_e, shape[0])
    j_w = _periodic_write_index(j_e, shape[1])
    k_w = _periodic_write_index(k_e, shape[2])

    wp.atomic_add(electric_field, 0, i_w, j_w, k_w, e_add[0])
    wp.atomic_add(electric_field, 1, i_w, j_w, k_w, e_add[1])
    wp.atomic_add(electric_field, 2, i_w, j_w, k_w, e_add[2])
