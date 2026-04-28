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


@wp.kernel
def _pml_electric_field_update_backward_kernel_scalar(
    pml_layer: wp.array4d(dtype=wp.float32),
    grad_output: wp.array4d(dtype=wp.float32),
    grad_pml: wp.array4d(dtype=wp.float32),
    spacing: wp.array(dtype=wp.float32),
    pml_layer_offset: wp.vec3i,
    dt: wp.float32,
    eps_scalar: wp.float32,
    need_grad_pml: int,
):
    i, j, k = wp.tid()

    if need_grad_pml != 1:
        return

    coeff_x = wp.float32(0.0)
    coeff_y = wp.float32(0.0)
    coeff_z = wp.float32(0.0)
    if eps_scalar != 0.0:
        coeff_x = dt / (spacing[0] * eps_scalar)
        coeff_y = dt / (spacing[1] * eps_scalar)
        coeff_z = dt / (spacing[2] * eps_scalar)

    i_e = i + pml_layer_offset[0]
    j_e = j + pml_layer_offset[1]
    k_e = k + pml_layer_offset[2]

    i_w = _periodic_write_index(i_e, grad_output.shape[1])
    j_w = _periodic_write_index(j_e, grad_output.shape[2])
    k_w = _periodic_write_index(k_e, grad_output.shape[3])

    grad_x = grad_output[0, i_w, j_w, k_w]
    grad_y = grad_output[1, i_w, j_w, k_w]
    grad_z = grad_output[2, i_w, j_w, k_w]

    wp.atomic_add(grad_pml, 0, i, j, k, grad_x * coeff_x)
    wp.atomic_add(grad_pml, 1, i, j, k, grad_y * coeff_y)
    wp.atomic_add(grad_pml, 2, i, j, k, grad_z * coeff_z)


@wp.kernel
def _pml_electric_field_update_backward_kernel_eps_field(
    pml_layer: wp.array4d(dtype=wp.float32),
    eps_field: wp.array3d(dtype=wp.float32),
    grad_output: wp.array4d(dtype=wp.float32),
    grad_pml: wp.array4d(dtype=wp.float32),
    grad_eps: wp.array3d(dtype=wp.float32),
    spacing: wp.array(dtype=wp.float32),
    pml_layer_offset: wp.vec3i,
    dt: wp.float32,
    need_grad_pml: int,
    need_grad_eps: int,
):
    i, j, k = wp.tid()

    shape = wp.vec3i(
        eps_field.shape[0],
        eps_field.shape[1],
        eps_field.shape[2],
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

    coeff_x = wp.float32(0.0)
    coeff_y = wp.float32(0.0)
    coeff_z = wp.float32(0.0)
    if eps_x != 0.0:
        coeff_x = dt / (spacing[0] * eps_x)
    if eps_y != 0.0:
        coeff_y = dt / (spacing[1] * eps_y)
    if eps_z != 0.0:
        coeff_z = dt / (spacing[2] * eps_z)

    phi_x = pml_layer[0, i, j, k]
    phi_y = pml_layer[1, i, j, k]
    phi_z = pml_layer[2, i, j, k]

    i_w = _periodic_write_index(i_e, grad_output.shape[1])
    j_w = _periodic_write_index(j_e, grad_output.shape[2])
    k_w = _periodic_write_index(k_e, grad_output.shape[3])

    grad_x = grad_output[0, i_w, j_w, k_w]
    grad_y = grad_output[1, i_w, j_w, k_w]
    grad_z = grad_output[2, i_w, j_w, k_w]

    if need_grad_pml == 1:
        wp.atomic_add(grad_pml, 0, i, j, k, grad_x * coeff_x)
        wp.atomic_add(grad_pml, 1, i, j, k, grad_y * coeff_y)
        wp.atomic_add(grad_pml, 2, i, j, k, grad_z * coeff_z)

    if need_grad_eps == 1:
        grad_eps_x = wp.float32(0.0)
        grad_eps_y = wp.float32(0.0)
        grad_eps_z = wp.float32(0.0)
        if eps_x != 0.0:
            grad_eps_x = grad_x * phi_x * (-dt / (spacing[0] * eps_x * eps_x))
        if eps_y != 0.0:
            grad_eps_y = grad_y * phi_y * (-dt / (spacing[1] * eps_y * eps_y))
        if eps_z != 0.0:
            grad_eps_z = grad_z * phi_z * (-dt / (spacing[2] * eps_z * eps_z))

        i_0_0_1 = _periodic_write_index(i_e - 1, shape[0])
        j_0_0_1 = _periodic_write_index(j_e - 1, shape[1])
        k_0_0_1 = _periodic_write_index(k_e, shape[2])
        i_0_1_0 = _periodic_write_index(i_e - 1, shape[0])
        j_0_1_0 = _periodic_write_index(j_e, shape[1])
        k_0_1_0 = _periodic_write_index(k_e - 1, shape[2])
        i_0_1_1 = _periodic_write_index(i_e - 1, shape[0])
        j_0_1_1 = _periodic_write_index(j_e, shape[1])
        k_0_1_1 = _periodic_write_index(k_e, shape[2])
        i_1_0_0 = _periodic_write_index(i_e, shape[0])
        j_1_0_0 = _periodic_write_index(j_e - 1, shape[1])
        k_1_0_0 = _periodic_write_index(k_e - 1, shape[2])
        i_1_0_1 = _periodic_write_index(i_e, shape[0])
        j_1_0_1 = _periodic_write_index(j_e - 1, shape[1])
        k_1_0_1 = _periodic_write_index(k_e, shape[2])
        i_1_1_0 = _periodic_write_index(i_e, shape[0])
        j_1_1_0 = _periodic_write_index(j_e, shape[1])
        k_1_1_0 = _periodic_write_index(k_e - 1, shape[2])
        i_1_1_1 = _periodic_write_index(i_e, shape[0])
        j_1_1_1 = _periodic_write_index(j_e, shape[1])
        k_1_1_1 = _periodic_write_index(k_e, shape[2])

        wp.atomic_add(
            grad_eps,
            i_1_1_1,
            j_1_1_1,
            k_1_1_1,
            0.25 * (grad_eps_x + grad_eps_y + grad_eps_z),
        )
        wp.atomic_add(
            grad_eps,
            i_1_1_0,
            j_1_1_0,
            k_1_1_0,
            0.25 * (grad_eps_x + grad_eps_y),
        )
        wp.atomic_add(
            grad_eps,
            i_1_0_1,
            j_1_0_1,
            k_1_0_1,
            0.25 * (grad_eps_x + grad_eps_z),
        )
        wp.atomic_add(grad_eps, i_1_0_0, j_1_0_0, k_1_0_0, 0.25 * grad_eps_x)
        wp.atomic_add(
            grad_eps,
            i_0_1_1,
            j_0_1_1,
            k_0_1_1,
            0.25 * (grad_eps_y + grad_eps_z),
        )
        wp.atomic_add(grad_eps, i_0_1_0, j_0_1_0, k_0_1_0, 0.25 * grad_eps_y)
        wp.atomic_add(grad_eps, i_0_0_1, j_0_0_1, k_0_0_1, 0.25 * grad_eps_z)
