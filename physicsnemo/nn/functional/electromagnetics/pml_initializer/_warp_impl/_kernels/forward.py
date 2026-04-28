# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warp as wp


@wp.kernel
def _pml_initializer_kernel(
    pml_layer: wp.array4d(dtype=wp.float32),
    direction: wp.vec3f,
    thickness: wp.int32,
    courant_number: wp.float32,
    kappa: wp.float32,
    a: wp.float32,
):
    i, j, k = wp.tid()

    ijk_f = wp.vec3f(wp.float32(i), wp.float32(j), wp.float32(k))

    if direction[0] == 1.0 or direction[1] == 1.0 or direction[2] == 1.0:
        step_e = wp.float32(thickness) - wp.dot(ijk_f, direction) - 0.5
        step_h = wp.float32(thickness) - wp.dot(ijk_f, direction) - 1.0
    else:
        step_e = -wp.dot(ijk_f, direction) + 0.5
        step_h = -wp.dot(ijk_f, direction) + 1.0

    sigma_e = (40.0 * step_e**3.0) / (wp.float32(thickness) + 1.0) ** 4.0
    sigma_h = (40.0 * step_h**3.0) / (wp.float32(thickness) + 1.0) ** 4.0

    if direction[0] != 0.0:
        vec_sigma_e = wp.vec3f(sigma_e, 0.0, 0.0)
        vec_sigma_h = wp.vec3f(sigma_h, 0.0, 0.0)
    elif direction[1] != 0.0:
        vec_sigma_e = wp.vec3f(0.0, sigma_e, 0.0)
        vec_sigma_h = wp.vec3f(0.0, sigma_h, 0.0)
    else:
        vec_sigma_e = wp.vec3f(0.0, 0.0, sigma_e)
        vec_sigma_h = wp.vec3f(0.0, 0.0, sigma_h)

    be = wp.vec3f(
        wp.exp(-((vec_sigma_e[0] / kappa) + a) * courant_number),
        wp.exp(-((vec_sigma_e[1] / kappa) + a) * courant_number),
        wp.exp(-((vec_sigma_e[2] / kappa) + a) * courant_number),
    )
    ce = wp.cw_div(
        wp.cw_mul(be - wp.vec3f(1.0, 1.0, 1.0), vec_sigma_e),
        vec_sigma_e * kappa + wp.vec3f(1.0, 1.0, 1.0) * a * kappa**2.0,
    )

    bh = wp.vec3f(
        wp.exp(-((vec_sigma_h[0] / kappa) + a) * courant_number),
        wp.exp(-((vec_sigma_h[1] / kappa) + a) * courant_number),
        wp.exp(-((vec_sigma_h[2] / kappa) + a) * courant_number),
    )
    ch = wp.cw_div(
        wp.cw_mul(bh - wp.vec3f(1.0, 1.0, 1.0), vec_sigma_h),
        vec_sigma_h * kappa + wp.vec3f(1.0, 1.0, 1.0) * a * kappa**2.0,
    )

    pml_layer[24, i, j, k] = be[0]
    pml_layer[25, i, j, k] = be[1]
    pml_layer[26, i, j, k] = be[2]
    pml_layer[27, i, j, k] = ce[0]
    pml_layer[28, i, j, k] = ce[1]
    pml_layer[29, i, j, k] = ce[2]
    pml_layer[30, i, j, k] = bh[0]
    pml_layer[31, i, j, k] = bh[1]
    pml_layer[32, i, j, k] = bh[2]
    pml_layer[33, i, j, k] = ch[0]
    pml_layer[34, i, j, k] = ch[1]
    pml_layer[35, i, j, k] = ch[2]
