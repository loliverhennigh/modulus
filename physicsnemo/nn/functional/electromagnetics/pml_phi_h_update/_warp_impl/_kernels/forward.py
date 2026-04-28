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
def _pml_phi_h_update_kernel(
    electric_field: wp.array4d(dtype=wp.float32),
    pml_layer_in: wp.array4d(dtype=wp.float32),
    pml_layer_out: wp.array4d(dtype=wp.float32),
    pml_layer_offset: wp.vec3i,
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

    psi_hx = wp.vec3f(
        pml_layer_in[15, i, j, k],
        pml_layer_in[16, i, j, k],
        pml_layer_in[17, i, j, k],
    )
    psi_hy = wp.vec3f(
        pml_layer_in[18, i, j, k],
        pml_layer_in[19, i, j, k],
        pml_layer_in[20, i, j, k],
    )
    psi_hz = wp.vec3f(
        pml_layer_in[21, i, j, k],
        pml_layer_in[22, i, j, k],
        pml_layer_in[23, i, j, k],
    )
    bh = wp.vec3f(
        pml_layer_in[30, i, j, k],
        pml_layer_in[31, i, j, k],
        pml_layer_in[32, i, j, k],
    )
    ch = wp.vec3f(
        pml_layer_in[33, i, j, k],
        pml_layer_in[34, i, j, k],
        pml_layer_in[35, i, j, k],
    )

    e_x_0_0_0 = _periodic_indexing(electric_field, shape, 0, i_e, j_e, k_e)
    e_x_0_1_0 = _periodic_indexing(electric_field, shape, 0, i_e, j_e + 1, k_e)
    e_x_0_0_1 = _periodic_indexing(electric_field, shape, 0, i_e, j_e, k_e + 1)
    e_y_0_0_0 = _periodic_indexing(electric_field, shape, 1, i_e, j_e, k_e)
    e_y_1_0_0 = _periodic_indexing(electric_field, shape, 1, i_e + 1, j_e, k_e)
    e_y_0_0_1 = _periodic_indexing(electric_field, shape, 1, i_e, j_e, k_e + 1)
    e_z_0_0_0 = _periodic_indexing(electric_field, shape, 2, i_e, j_e, k_e)
    e_z_1_0_0 = _periodic_indexing(electric_field, shape, 2, i_e + 1, j_e, k_e)
    e_z_0_1_0 = _periodic_indexing(electric_field, shape, 2, i_e, j_e + 1, k_e)

    psi_hx = wp.cw_mul(bh, psi_hx)
    psi_hy = wp.cw_mul(bh, psi_hy)
    psi_hz = wp.cw_mul(bh, psi_hz)

    if i != pml_layer_out.shape[1] - 1:
        psi_hy[0] += (e_z_1_0_0 - e_z_0_0_0) * ch[0]
        psi_hz[0] += (e_y_1_0_0 - e_y_0_0_0) * ch[0]
    if j != pml_layer_out.shape[2] - 1:
        psi_hx[1] += (e_z_0_1_0 - e_z_0_0_0) * ch[1]
        psi_hz[1] += (e_x_0_1_0 - e_x_0_0_0) * ch[1]
    if k != pml_layer_out.shape[3] - 1:
        psi_hx[2] += (e_y_0_0_1 - e_y_0_0_0) * ch[2]
        psi_hy[2] += (e_x_0_0_1 - e_x_0_0_0) * ch[2]

    phi_h = wp.vec3f(
        psi_hx[1] - psi_hx[2],
        psi_hy[2] - psi_hy[0],
        psi_hz[0] - psi_hz[1],
    )

    pml_layer_out[3, i, j, k] = phi_h[0]
    pml_layer_out[4, i, j, k] = phi_h[1]
    pml_layer_out[5, i, j, k] = phi_h[2]
    pml_layer_out[15, i, j, k] = psi_hx[0]
    pml_layer_out[16, i, j, k] = psi_hx[1]
    pml_layer_out[17, i, j, k] = psi_hx[2]
    pml_layer_out[18, i, j, k] = psi_hy[0]
    pml_layer_out[19, i, j, k] = psi_hy[1]
    pml_layer_out[20, i, j, k] = psi_hy[2]
    pml_layer_out[21, i, j, k] = psi_hz[0]
    pml_layer_out[22, i, j, k] = psi_hz[1]
    pml_layer_out[23, i, j, k] = psi_hz[2]
