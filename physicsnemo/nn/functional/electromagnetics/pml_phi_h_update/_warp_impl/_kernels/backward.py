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
) -> wp.float32:
    i = (i + shape[0]) % shape[0]
    j = (j + shape[1]) % shape[1]
    k = (k + shape[2]) % shape[2]
    return field[component, i, j, k]


@wp.func
def _periodic_write_index(index: wp.int32, size: wp.int32) -> wp.int32:
    return (index + size) % size


@wp.kernel
def _pml_phi_h_update_backward_kernel(
    electric_field: wp.array4d(dtype=wp.float32),
    pml_layer_in: wp.array4d(dtype=wp.float32),
    grad_output: wp.array4d(dtype=wp.float32),
    grad_electric: wp.array4d(dtype=wp.float32),
    grad_pml: wp.array4d(dtype=wp.float32),
    pml_layer_offset: wp.vec3i,
    need_grad_electric: int,
    need_grad_pml: int,
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

    e_x_0_0_0 = _periodic_indexing(electric_field, shape, 0, i_e, j_e, k_e)
    e_x_0_1_0 = _periodic_indexing(electric_field, shape, 0, i_e, j_e + 1, k_e)
    e_x_0_0_1 = _periodic_indexing(electric_field, shape, 0, i_e, j_e, k_e + 1)
    e_y_0_0_0 = _periodic_indexing(electric_field, shape, 1, i_e, j_e, k_e)
    e_y_1_0_0 = _periodic_indexing(electric_field, shape, 1, i_e + 1, j_e, k_e)
    e_y_0_0_1 = _periodic_indexing(electric_field, shape, 1, i_e, j_e, k_e + 1)
    e_z_0_0_0 = _periodic_indexing(electric_field, shape, 2, i_e, j_e, k_e)
    e_z_1_0_0 = _periodic_indexing(electric_field, shape, 2, i_e + 1, j_e, k_e)
    e_z_0_1_0 = _periodic_indexing(electric_field, shape, 2, i_e, j_e + 1, k_e)

    psi_hx_in_0 = pml_layer_in[15, i, j, k]
    psi_hx_in_1 = pml_layer_in[16, i, j, k]
    psi_hx_in_2 = pml_layer_in[17, i, j, k]
    psi_hy_in_0 = pml_layer_in[18, i, j, k]
    psi_hy_in_1 = pml_layer_in[19, i, j, k]
    psi_hy_in_2 = pml_layer_in[20, i, j, k]
    psi_hz_in_0 = pml_layer_in[21, i, j, k]
    psi_hz_in_1 = pml_layer_in[22, i, j, k]
    psi_hz_in_2 = pml_layer_in[23, i, j, k]

    bh_0 = pml_layer_in[30, i, j, k]
    bh_1 = pml_layer_in[31, i, j, k]
    bh_2 = pml_layer_in[32, i, j, k]

    ch_0 = pml_layer_in[33, i, j, k]
    ch_1 = pml_layer_in[34, i, j, k]
    ch_2 = pml_layer_in[35, i, j, k]

    grad_phi_0 = grad_output[3, i, j, k]
    grad_phi_1 = grad_output[4, i, j, k]
    grad_phi_2 = grad_output[5, i, j, k]

    grad_psi_hx_0 = grad_output[15, i, j, k]
    grad_psi_hx_1 = grad_output[16, i, j, k] + grad_phi_0
    grad_psi_hx_2 = grad_output[17, i, j, k] - grad_phi_0

    grad_psi_hy_0 = grad_output[18, i, j, k] - grad_phi_1
    grad_psi_hy_1 = grad_output[19, i, j, k]
    grad_psi_hy_2 = grad_output[20, i, j, k] + grad_phi_1

    grad_psi_hz_0 = grad_output[21, i, j, k] + grad_phi_2
    grad_psi_hz_1 = grad_output[22, i, j, k] - grad_phi_2
    grad_psi_hz_2 = grad_output[23, i, j, k]

    if need_grad_pml == 1:
        wp.atomic_add(grad_pml, 15, i, j, k, grad_psi_hx_0 * bh_0)
        wp.atomic_add(grad_pml, 16, i, j, k, grad_psi_hx_1 * bh_1)
        wp.atomic_add(grad_pml, 17, i, j, k, grad_psi_hx_2 * bh_2)

        wp.atomic_add(grad_pml, 18, i, j, k, grad_psi_hy_0 * bh_0)
        wp.atomic_add(grad_pml, 19, i, j, k, grad_psi_hy_1 * bh_1)
        wp.atomic_add(grad_pml, 20, i, j, k, grad_psi_hy_2 * bh_2)

        wp.atomic_add(grad_pml, 21, i, j, k, grad_psi_hz_0 * bh_0)
        wp.atomic_add(grad_pml, 22, i, j, k, grad_psi_hz_1 * bh_1)
        wp.atomic_add(grad_pml, 23, i, j, k, grad_psi_hz_2 * bh_2)

        wp.atomic_add(
            grad_pml,
            30,
            i,
            j,
            k,
            grad_psi_hx_0 * psi_hx_in_0
            + grad_psi_hy_0 * psi_hy_in_0
            + grad_psi_hz_0 * psi_hz_in_0,
        )
        wp.atomic_add(
            grad_pml,
            31,
            i,
            j,
            k,
            grad_psi_hx_1 * psi_hx_in_1
            + grad_psi_hy_1 * psi_hy_in_1
            + grad_psi_hz_1 * psi_hz_in_1,
        )
        wp.atomic_add(
            grad_pml,
            32,
            i,
            j,
            k,
            grad_psi_hx_2 * psi_hx_in_2
            + grad_psi_hy_2 * psi_hy_in_2
            + grad_psi_hz_2 * psi_hz_in_2,
        )

        if i != pml_layer_in.shape[1] - 1:
            wp.atomic_add(
                grad_pml,
                33,
                i,
                j,
                k,
                grad_psi_hy_0 * (e_z_1_0_0 - e_z_0_0_0)
                + grad_psi_hz_0 * (e_y_1_0_0 - e_y_0_0_0),
            )

        if j != pml_layer_in.shape[2] - 1:
            wp.atomic_add(
                grad_pml,
                34,
                i,
                j,
                k,
                grad_psi_hx_1 * (e_z_0_1_0 - e_z_0_0_0)
                + grad_psi_hz_1 * (e_x_0_1_0 - e_x_0_0_0),
            )

        if k != pml_layer_in.shape[3] - 1:
            wp.atomic_add(
                grad_pml,
                35,
                i,
                j,
                k,
                grad_psi_hx_2 * (e_y_0_0_1 - e_y_0_0_0)
                + grad_psi_hy_2 * (e_x_0_0_1 - e_x_0_0_0),
            )

    if need_grad_electric == 1:
        i_0_0_0 = _periodic_write_index(i_e, shape[0])
        j_0_0_0 = _periodic_write_index(j_e, shape[1])
        k_0_0_0 = _periodic_write_index(k_e, shape[2])

        i_1_0_0 = _periodic_write_index(i_e + 1, shape[0])
        j_1_0_0 = _periodic_write_index(j_e, shape[1])
        k_1_0_0 = _periodic_write_index(k_e, shape[2])

        i_0_1_0 = _periodic_write_index(i_e, shape[0])
        j_0_1_0 = _periodic_write_index(j_e + 1, shape[1])
        k_0_1_0 = _periodic_write_index(k_e, shape[2])

        i_0_0_1 = _periodic_write_index(i_e, shape[0])
        j_0_0_1 = _periodic_write_index(j_e, shape[1])
        k_0_0_1 = _periodic_write_index(k_e + 1, shape[2])

        if i != pml_layer_in.shape[1] - 1:
            coeff = ch_0
            wp.atomic_add(grad_electric, 2, i_1_0_0, j_1_0_0, k_1_0_0, grad_psi_hy_0 * coeff)
            wp.atomic_add(grad_electric, 2, i_0_0_0, j_0_0_0, k_0_0_0, -grad_psi_hy_0 * coeff)
            wp.atomic_add(grad_electric, 1, i_1_0_0, j_1_0_0, k_1_0_0, grad_psi_hz_0 * coeff)
            wp.atomic_add(grad_electric, 1, i_0_0_0, j_0_0_0, k_0_0_0, -grad_psi_hz_0 * coeff)

        if j != pml_layer_in.shape[2] - 1:
            coeff = ch_1
            wp.atomic_add(grad_electric, 2, i_0_1_0, j_0_1_0, k_0_1_0, grad_psi_hx_1 * coeff)
            wp.atomic_add(grad_electric, 2, i_0_0_0, j_0_0_0, k_0_0_0, -grad_psi_hx_1 * coeff)
            wp.atomic_add(grad_electric, 0, i_0_1_0, j_0_1_0, k_0_1_0, grad_psi_hz_1 * coeff)
            wp.atomic_add(grad_electric, 0, i_0_0_0, j_0_0_0, k_0_0_0, -grad_psi_hz_1 * coeff)

        if k != pml_layer_in.shape[3] - 1:
            coeff = ch_2
            wp.atomic_add(grad_electric, 1, i_0_0_1, j_0_0_1, k_0_0_1, grad_psi_hx_2 * coeff)
            wp.atomic_add(grad_electric, 1, i_0_0_0, j_0_0_0, k_0_0_0, -grad_psi_hx_2 * coeff)
            wp.atomic_add(grad_electric, 0, i_0_0_1, j_0_0_1, k_0_0_1, grad_psi_hy_2 * coeff)
            wp.atomic_add(grad_electric, 0, i_0_0_0, j_0_0_0, k_0_0_0, -grad_psi_hy_2 * coeff)
