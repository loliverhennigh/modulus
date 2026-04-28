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
def _pml_phi_e_update_backward_kernel(
    magnetic_field: wp.array4d(dtype=wp.float32),
    pml_layer_in: wp.array4d(dtype=wp.float32),
    grad_output: wp.array4d(dtype=wp.float32),
    grad_magnetic: wp.array4d(dtype=wp.float32),
    grad_pml: wp.array4d(dtype=wp.float32),
    pml_layer_offset: wp.vec3i,
    need_grad_magnetic: int,
    need_grad_pml: int,
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

    h_x_1_1_1 = _periodic_indexing(magnetic_field, shape, 0, i_m, j_m, k_m)
    h_x_1_0_1 = _periodic_indexing(magnetic_field, shape, 0, i_m, j_m - 1, k_m)
    h_x_1_1_0 = _periodic_indexing(magnetic_field, shape, 0, i_m, j_m, k_m - 1)
    h_y_1_1_1 = _periodic_indexing(magnetic_field, shape, 1, i_m, j_m, k_m)
    h_y_0_1_1 = _periodic_indexing(magnetic_field, shape, 1, i_m - 1, j_m, k_m)
    h_y_1_1_0 = _periodic_indexing(magnetic_field, shape, 1, i_m, j_m, k_m - 1)
    h_z_1_1_1 = _periodic_indexing(magnetic_field, shape, 2, i_m, j_m, k_m)
    h_z_0_1_1 = _periodic_indexing(magnetic_field, shape, 2, i_m - 1, j_m, k_m)
    h_z_1_0_1 = _periodic_indexing(magnetic_field, shape, 2, i_m, j_m - 1, k_m)

    psi_ex_in_0 = pml_layer_in[6, i, j, k]
    psi_ex_in_1 = pml_layer_in[7, i, j, k]
    psi_ex_in_2 = pml_layer_in[8, i, j, k]
    psi_ey_in_0 = pml_layer_in[9, i, j, k]
    psi_ey_in_1 = pml_layer_in[10, i, j, k]
    psi_ey_in_2 = pml_layer_in[11, i, j, k]
    psi_ez_in_0 = pml_layer_in[12, i, j, k]
    psi_ez_in_1 = pml_layer_in[13, i, j, k]
    psi_ez_in_2 = pml_layer_in[14, i, j, k]

    be_0 = pml_layer_in[24, i, j, k]
    be_1 = pml_layer_in[25, i, j, k]
    be_2 = pml_layer_in[26, i, j, k]

    ce_0 = pml_layer_in[27, i, j, k]
    ce_1 = pml_layer_in[28, i, j, k]
    ce_2 = pml_layer_in[29, i, j, k]

    grad_phi_0 = grad_output[0, i, j, k]
    grad_phi_1 = grad_output[1, i, j, k]
    grad_phi_2 = grad_output[2, i, j, k]

    grad_psi_ex_0 = grad_output[6, i, j, k]
    grad_psi_ex_1 = grad_output[7, i, j, k] + grad_phi_0
    grad_psi_ex_2 = grad_output[8, i, j, k] - grad_phi_0

    grad_psi_ey_0 = grad_output[9, i, j, k] - grad_phi_1
    grad_psi_ey_1 = grad_output[10, i, j, k]
    grad_psi_ey_2 = grad_output[11, i, j, k] + grad_phi_1

    grad_psi_ez_0 = grad_output[12, i, j, k] + grad_phi_2
    grad_psi_ez_1 = grad_output[13, i, j, k] - grad_phi_2
    grad_psi_ez_2 = grad_output[14, i, j, k]

    if need_grad_pml == 1:
        wp.atomic_add(grad_pml, 6, i, j, k, grad_psi_ex_0 * be_0)
        wp.atomic_add(grad_pml, 7, i, j, k, grad_psi_ex_1 * be_1)
        wp.atomic_add(grad_pml, 8, i, j, k, grad_psi_ex_2 * be_2)

        wp.atomic_add(grad_pml, 9, i, j, k, grad_psi_ey_0 * be_0)
        wp.atomic_add(grad_pml, 10, i, j, k, grad_psi_ey_1 * be_1)
        wp.atomic_add(grad_pml, 11, i, j, k, grad_psi_ey_2 * be_2)

        wp.atomic_add(grad_pml, 12, i, j, k, grad_psi_ez_0 * be_0)
        wp.atomic_add(grad_pml, 13, i, j, k, grad_psi_ez_1 * be_1)
        wp.atomic_add(grad_pml, 14, i, j, k, grad_psi_ez_2 * be_2)

        wp.atomic_add(
            grad_pml,
            24,
            i,
            j,
            k,
            grad_psi_ex_0 * psi_ex_in_0
            + grad_psi_ey_0 * psi_ey_in_0
            + grad_psi_ez_0 * psi_ez_in_0,
        )
        wp.atomic_add(
            grad_pml,
            25,
            i,
            j,
            k,
            grad_psi_ex_1 * psi_ex_in_1
            + grad_psi_ey_1 * psi_ey_in_1
            + grad_psi_ez_1 * psi_ez_in_1,
        )
        wp.atomic_add(
            grad_pml,
            26,
            i,
            j,
            k,
            grad_psi_ex_2 * psi_ex_in_2
            + grad_psi_ey_2 * psi_ey_in_2
            + grad_psi_ez_2 * psi_ez_in_2,
        )

        if i != 0:
            wp.atomic_add(
                grad_pml,
                27,
                i,
                j,
                k,
                grad_psi_ey_0 * (h_z_1_1_1 - h_z_0_1_1)
                + grad_psi_ez_0 * (h_y_1_1_1 - h_y_0_1_1),
            )

        if j != 0:
            wp.atomic_add(
                grad_pml,
                28,
                i,
                j,
                k,
                grad_psi_ex_1 * (h_z_1_1_1 - h_z_1_0_1)
                + grad_psi_ez_1 * (h_x_1_1_1 - h_x_1_0_1),
            )

        if k != 0:
            wp.atomic_add(
                grad_pml,
                29,
                i,
                j,
                k,
                grad_psi_ex_2 * (h_y_1_1_1 - h_y_1_1_0)
                + grad_psi_ey_2 * (h_x_1_1_1 - h_x_1_1_0),
            )

    if need_grad_magnetic == 1:
        i_1_1_1 = _periodic_write_index(i_m, shape[0])
        j_1_1_1 = _periodic_write_index(j_m, shape[1])
        k_1_1_1 = _periodic_write_index(k_m, shape[2])

        i_0_1_1 = _periodic_write_index(i_m - 1, shape[0])
        j_0_1_1 = _periodic_write_index(j_m, shape[1])
        k_0_1_1 = _periodic_write_index(k_m, shape[2])

        i_1_0_1 = _periodic_write_index(i_m, shape[0])
        j_1_0_1 = _periodic_write_index(j_m - 1, shape[1])
        k_1_0_1 = _periodic_write_index(k_m, shape[2])

        i_1_1_0 = _periodic_write_index(i_m, shape[0])
        j_1_1_0 = _periodic_write_index(j_m, shape[1])
        k_1_1_0 = _periodic_write_index(k_m - 1, shape[2])

        if i != 0:
            coeff = ce_0
            wp.atomic_add(grad_magnetic, 2, i_1_1_1, j_1_1_1, k_1_1_1, grad_psi_ey_0 * coeff)
            wp.atomic_add(grad_magnetic, 2, i_0_1_1, j_0_1_1, k_0_1_1, -grad_psi_ey_0 * coeff)
            wp.atomic_add(grad_magnetic, 1, i_1_1_1, j_1_1_1, k_1_1_1, grad_psi_ez_0 * coeff)
            wp.atomic_add(grad_magnetic, 1, i_0_1_1, j_0_1_1, k_0_1_1, -grad_psi_ez_0 * coeff)

        if j != 0:
            coeff = ce_1
            wp.atomic_add(grad_magnetic, 2, i_1_1_1, j_1_1_1, k_1_1_1, grad_psi_ex_1 * coeff)
            wp.atomic_add(grad_magnetic, 2, i_1_0_1, j_1_0_1, k_1_0_1, -grad_psi_ex_1 * coeff)
            wp.atomic_add(grad_magnetic, 0, i_1_1_1, j_1_1_1, k_1_1_1, grad_psi_ez_1 * coeff)
            wp.atomic_add(grad_magnetic, 0, i_1_0_1, j_1_0_1, k_1_0_1, -grad_psi_ez_1 * coeff)

        if k != 0:
            coeff = ce_2
            wp.atomic_add(grad_magnetic, 1, i_1_1_1, j_1_1_1, k_1_1_1, grad_psi_ex_2 * coeff)
            wp.atomic_add(grad_magnetic, 1, i_1_1_0, j_1_1_0, k_1_1_0, -grad_psi_ex_2 * coeff)
            wp.atomic_add(grad_magnetic, 0, i_1_1_1, j_1_1_1, k_1_1_1, grad_psi_ey_2 * coeff)
            wp.atomic_add(grad_magnetic, 0, i_1_1_0, j_1_1_0, k_1_1_0, -grad_psi_ey_2 * coeff)
