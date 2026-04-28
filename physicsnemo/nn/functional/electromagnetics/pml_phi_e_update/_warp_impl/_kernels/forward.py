# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warp as wp


@wp.func
def _periodic_indexing(
    field: wp.array4d(dtype=wp.float32),
    shape: wp.vec3i,
    component: wp.int32,
    i: wp.int32,
    j: wp.int32,
    k: wp.int32,
):
    i = (i + shape[0]) % shape[0]
    j = (j + shape[1]) % shape[1]
    k = (k + shape[2]) % shape[2]
    return field[component, i, j, k]


@wp.kernel
def _pml_phi_e_update_kernel(
    magnetic_field: wp.array4d(dtype=wp.float32),
    pml_layer_in: wp.array4d(dtype=wp.float32),
    pml_layer_out: wp.array4d(dtype=wp.float32),
    pml_layer_offset: wp.vec3i,
):
    i, j, k = wp.tid()

    shape = wp.vec3i(
        magnetic_field.shape[1],
        magnetic_field.shape[2],
        magnetic_field.shape[3],
    )

    i_m = i + pml_layer_offset[0]
    j_m = j + pml_layer_offset[1]
    k_m = k + pml_layer_offset[2]

    psi_ex = wp.vec3f(
        pml_layer_in[6, i, j, k],
        pml_layer_in[7, i, j, k],
        pml_layer_in[8, i, j, k],
    )
    psi_ey = wp.vec3f(
        pml_layer_in[9, i, j, k],
        pml_layer_in[10, i, j, k],
        pml_layer_in[11, i, j, k],
    )
    psi_ez = wp.vec3f(
        pml_layer_in[12, i, j, k],
        pml_layer_in[13, i, j, k],
        pml_layer_in[14, i, j, k],
    )
    be = wp.vec3f(
        pml_layer_in[24, i, j, k],
        pml_layer_in[25, i, j, k],
        pml_layer_in[26, i, j, k],
    )
    ce = wp.vec3f(
        pml_layer_in[27, i, j, k],
        pml_layer_in[28, i, j, k],
        pml_layer_in[29, i, j, k],
    )

    h_x_1_1_1 = _periodic_indexing(magnetic_field, shape, 0, i_m, j_m, k_m)
    h_x_1_0_1 = _periodic_indexing(magnetic_field, shape, 0, i_m, j_m - 1, k_m)
    h_x_1_1_0 = _periodic_indexing(magnetic_field, shape, 0, i_m, j_m, k_m - 1)
    h_y_1_1_1 = _periodic_indexing(magnetic_field, shape, 1, i_m, j_m, k_m)
    h_y_0_1_1 = _periodic_indexing(magnetic_field, shape, 1, i_m - 1, j_m, k_m)
    h_y_1_1_0 = _periodic_indexing(magnetic_field, shape, 1, i_m, j_m, k_m - 1)
    h_z_1_1_1 = _periodic_indexing(magnetic_field, shape, 2, i_m, j_m, k_m)
    h_z_0_1_1 = _periodic_indexing(magnetic_field, shape, 2, i_m - 1, j_m, k_m)
    h_z_1_0_1 = _periodic_indexing(magnetic_field, shape, 2, i_m, j_m - 1, k_m)

    psi_ex = wp.cw_mul(be, psi_ex)
    psi_ey = wp.cw_mul(be, psi_ey)
    psi_ez = wp.cw_mul(be, psi_ez)

    if i != 0:
        psi_ey[0] += (h_z_1_1_1 - h_z_0_1_1) * ce[0]
        psi_ez[0] += (h_y_1_1_1 - h_y_0_1_1) * ce[0]
    if j != 0:
        psi_ex[1] += (h_z_1_1_1 - h_z_1_0_1) * ce[1]
        psi_ez[1] += (h_x_1_1_1 - h_x_1_0_1) * ce[1]
    if k != 0:
        psi_ex[2] += (h_y_1_1_1 - h_y_1_1_0) * ce[2]
        psi_ey[2] += (h_x_1_1_1 - h_x_1_1_0) * ce[2]

    phi_e = wp.vec3f(
        psi_ex[1] - psi_ex[2],
        psi_ey[2] - psi_ey[0],
        psi_ez[0] - psi_ez[1],
    )

    pml_layer_out[0, i, j, k] = phi_e[0]
    pml_layer_out[1, i, j, k] = phi_e[1]
    pml_layer_out[2, i, j, k] = phi_e[2]
    pml_layer_out[6, i, j, k] = psi_ex[0]
    pml_layer_out[7, i, j, k] = psi_ex[1]
    pml_layer_out[8, i, j, k] = psi_ex[2]
    pml_layer_out[9, i, j, k] = psi_ey[0]
    pml_layer_out[10, i, j, k] = psi_ey[1]
    pml_layer_out[11, i, j, k] = psi_ey[2]
    pml_layer_out[12, i, j, k] = psi_ez[0]
    pml_layer_out[13, i, j, k] = psi_ez[1]
    pml_layer_out[14, i, j, k] = psi_ez[2]
