# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from collections.abc import Sequence

import torch
import warp as wp

_SUPPORTED_ORDERS = (2, 4)
_SUPPORTED_DERIVATIVE_ORDERS = (1, 2)

### Warp runtime initialization for custom kernels.
wp.init()
wp.config.quiet = True

### Optional launch block size override; <=0 uses Warp default autotuning.
_WARP_BLOCK_DIM = -1

### ============================================================
### Index wrapping helpers (periodic boundaries without modulo)
### ============================================================


@wp.func
def _wrap_plus1(i: int, n: int) -> int:
    return (i + 1) % n


@wp.func
def _wrap_minus1(i: int, n: int) -> int:
    return (i + n - 1) % n


@wp.func
def _wrap_plus2(i: int, n: int) -> int:
    return (i + 2) % n


@wp.func
def _wrap_minus2(i: int, n: int) -> int:
    return (i + n - 2) % n


### ============================================================
### Forward kernels (periodic central differences)
### ============================================================


@wp.kernel
def _uniform_grid_gradient_1d_kernel(
    field: wp.array(dtype=wp.float32),
    inv_dx: float,
    grad0: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = field.shape[0]

    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)

    grad0[i] = (field[ip] - field[im]) * (0.5 * inv_dx)


@wp.kernel
def _uniform_grid_gradient_1d_order4_kernel(
    field: wp.array(dtype=wp.float32),
    inv_dx: float,
    grad0: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = field.shape[0]

    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)

    grad0[i] = (-field[ip2] + 8.0 * field[ip1] - 8.0 * field[im1] + field[im2]) * (
        inv_dx / 12.0
    )


@wp.kernel
def _uniform_grid_gradient_2d_kernel(
    field: wp.array2d(dtype=wp.float32),
    inv_dx0: float,
    inv_dx1: float,
    grad0: wp.array2d(dtype=wp.float32),
    grad1: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]

    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)
    jm = _wrap_minus1(j, n1)
    jp = _wrap_plus1(j, n1)

    grad0[i, j] = (field[ip, j] - field[im, j]) * (0.5 * inv_dx0)
    grad1[i, j] = (field[i, jp] - field[i, jm]) * (0.5 * inv_dx1)


@wp.kernel
def _uniform_grid_gradient_2d_order4_kernel(
    field: wp.array2d(dtype=wp.float32),
    inv_dx0: float,
    inv_dx1: float,
    grad0: wp.array2d(dtype=wp.float32),
    grad1: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]

    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)

    jm1 = _wrap_minus1(j, n1)
    jp1 = _wrap_plus1(j, n1)
    jm2 = _wrap_minus2(j, n1)
    jp2 = _wrap_plus2(j, n1)

    grad0[i, j] = (
        -field[ip2, j] + 8.0 * field[ip1, j] - 8.0 * field[im1, j] + field[im2, j]
    ) * (inv_dx0 / 12.0)
    grad1[i, j] = (
        -field[i, jp2] + 8.0 * field[i, jp1] - 8.0 * field[i, jm1] + field[i, jm2]
    ) * (inv_dx1 / 12.0)


@wp.kernel
def _uniform_grid_gradient_3d_kernel(
    field: wp.array3d(dtype=wp.float32),
    inv_dx0: float,
    inv_dx1: float,
    inv_dx2: float,
    grad0: wp.array3d(dtype=wp.float32),
    grad1: wp.array3d(dtype=wp.float32),
    grad2: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]
    n2 = field.shape[2]

    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)
    jm = _wrap_minus1(j, n1)
    jp = _wrap_plus1(j, n1)
    km = _wrap_minus1(k, n2)
    kp = _wrap_plus1(k, n2)

    grad0[i, j, k] = (field[ip, j, k] - field[im, j, k]) * (0.5 * inv_dx0)
    grad1[i, j, k] = (field[i, jp, k] - field[i, jm, k]) * (0.5 * inv_dx1)
    grad2[i, j, k] = (field[i, j, kp] - field[i, j, km]) * (0.5 * inv_dx2)


@wp.kernel
def _uniform_grid_gradient_3d_order4_kernel(
    field: wp.array3d(dtype=wp.float32),
    inv_dx0: float,
    inv_dx1: float,
    inv_dx2: float,
    grad0: wp.array3d(dtype=wp.float32),
    grad1: wp.array3d(dtype=wp.float32),
    grad2: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]
    n2 = field.shape[2]

    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)

    jm1 = _wrap_minus1(j, n1)
    jp1 = _wrap_plus1(j, n1)
    jm2 = _wrap_minus2(j, n1)
    jp2 = _wrap_plus2(j, n1)

    km1 = _wrap_minus1(k, n2)
    kp1 = _wrap_plus1(k, n2)
    km2 = _wrap_minus2(k, n2)
    kp2 = _wrap_plus2(k, n2)

    grad0[i, j, k] = (
        -field[ip2, j, k]
        + 8.0 * field[ip1, j, k]
        - 8.0 * field[im1, j, k]
        + field[im2, j, k]
    ) * (inv_dx0 / 12.0)
    grad1[i, j, k] = (
        -field[i, jp2, k]
        + 8.0 * field[i, jp1, k]
        - 8.0 * field[i, jm1, k]
        + field[i, jm2, k]
    ) * (inv_dx1 / 12.0)
    grad2[i, j, k] = (
        -field[i, j, kp2]
        + 8.0 * field[i, j, kp1]
        - 8.0 * field[i, j, km1]
        + field[i, j, km2]
    ) * (inv_dx2 / 12.0)


@wp.kernel
def _uniform_grid_second_derivative_1d_kernel(
    field: wp.array(dtype=wp.float32),
    inv_dx2: float,
    grad0: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = field.shape[0]

    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)
    grad0[i] = (field[ip] - 2.0 * field[i] + field[im]) * inv_dx2


@wp.kernel
def _uniform_grid_second_derivative_1d_order4_kernel(
    field: wp.array(dtype=wp.float32),
    inv_dx2: float,
    grad0: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = field.shape[0]

    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)
    grad0[i] = (
        -field[ip2]
        + 16.0 * field[ip1]
        - 30.0 * field[i]
        + 16.0 * field[im1]
        - field[im2]
    ) * (inv_dx2 / 12.0)


@wp.kernel
def _uniform_grid_second_derivative_2d_kernel(
    field: wp.array2d(dtype=wp.float32),
    inv_dx20: float,
    inv_dx21: float,
    grad0: wp.array2d(dtype=wp.float32),
    grad1: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]

    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)
    jm = _wrap_minus1(j, n1)
    jp = _wrap_plus1(j, n1)

    grad0[i, j] = (field[ip, j] - 2.0 * field[i, j] + field[im, j]) * inv_dx20
    grad1[i, j] = (field[i, jp] - 2.0 * field[i, j] + field[i, jm]) * inv_dx21


@wp.kernel
def _uniform_grid_second_derivative_2d_order4_kernel(
    field: wp.array2d(dtype=wp.float32),
    inv_dx20: float,
    inv_dx21: float,
    grad0: wp.array2d(dtype=wp.float32),
    grad1: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]

    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)

    jm1 = _wrap_minus1(j, n1)
    jp1 = _wrap_plus1(j, n1)
    jm2 = _wrap_minus2(j, n1)
    jp2 = _wrap_plus2(j, n1)

    grad0[i, j] = (
        -field[ip2, j]
        + 16.0 * field[ip1, j]
        - 30.0 * field[i, j]
        + 16.0 * field[im1, j]
        - field[im2, j]
    ) * (inv_dx20 / 12.0)
    grad1[i, j] = (
        -field[i, jp2]
        + 16.0 * field[i, jp1]
        - 30.0 * field[i, j]
        + 16.0 * field[i, jm1]
        - field[i, jm2]
    ) * (inv_dx21 / 12.0)


@wp.kernel
def _uniform_grid_second_derivative_3d_kernel(
    field: wp.array3d(dtype=wp.float32),
    inv_dx20: float,
    inv_dx21: float,
    inv_dx22: float,
    grad0: wp.array3d(dtype=wp.float32),
    grad1: wp.array3d(dtype=wp.float32),
    grad2: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]
    n2 = field.shape[2]

    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)
    jm = _wrap_minus1(j, n1)
    jp = _wrap_plus1(j, n1)
    km = _wrap_minus1(k, n2)
    kp = _wrap_plus1(k, n2)

    grad0[i, j, k] = (
        field[ip, j, k] - 2.0 * field[i, j, k] + field[im, j, k]
    ) * inv_dx20
    grad1[i, j, k] = (
        field[i, jp, k] - 2.0 * field[i, j, k] + field[i, jm, k]
    ) * inv_dx21
    grad2[i, j, k] = (
        field[i, j, kp] - 2.0 * field[i, j, k] + field[i, j, km]
    ) * inv_dx22


@wp.kernel
def _uniform_grid_second_derivative_3d_order4_kernel(
    field: wp.array3d(dtype=wp.float32),
    inv_dx20: float,
    inv_dx21: float,
    inv_dx22: float,
    grad0: wp.array3d(dtype=wp.float32),
    grad1: wp.array3d(dtype=wp.float32),
    grad2: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]
    n2 = field.shape[2]

    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)

    jm1 = _wrap_minus1(j, n1)
    jp1 = _wrap_plus1(j, n1)
    jm2 = _wrap_minus2(j, n1)
    jp2 = _wrap_plus2(j, n1)

    km1 = _wrap_minus1(k, n2)
    kp1 = _wrap_plus1(k, n2)
    km2 = _wrap_minus2(k, n2)
    kp2 = _wrap_plus2(k, n2)

    grad0[i, j, k] = (
        -field[ip2, j, k]
        + 16.0 * field[ip1, j, k]
        - 30.0 * field[i, j, k]
        + 16.0 * field[im1, j, k]
        - field[im2, j, k]
    ) * (inv_dx20 / 12.0)
    grad1[i, j, k] = (
        -field[i, jp2, k]
        + 16.0 * field[i, jp1, k]
        - 30.0 * field[i, j, k]
        + 16.0 * field[i, jm1, k]
        - field[i, jm2, k]
    ) * (inv_dx21 / 12.0)
    grad2[i, j, k] = (
        -field[i, j, kp2]
        + 16.0 * field[i, j, kp1]
        - 30.0 * field[i, j, k]
        + 16.0 * field[i, j, km1]
        - field[i, j, km2]
    ) * (inv_dx22 / 12.0)


### ============================================================
### Backward kernels (adjoint central differences)
### ============================================================


@wp.kernel
def _uniform_grid_gradient_1d_backward_kernel(
    grad0: wp.array(dtype=wp.float32),
    inv_dx: float,
    grad_field: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = grad0.shape[0]

    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)

    grad_field[i] = (grad0[im] - grad0[ip]) * (0.5 * inv_dx)


@wp.kernel
def _uniform_grid_gradient_1d_order4_backward_kernel(
    grad0: wp.array(dtype=wp.float32),
    inv_dx: float,
    grad_field: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = grad0.shape[0]

    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)

    grad_field[i] = (grad0[ip2] - 8.0 * grad0[ip1] + 8.0 * grad0[im1] - grad0[im2]) * (
        inv_dx / 12.0
    )


@wp.kernel
def _uniform_grid_gradient_2d_backward_kernel(
    grad0: wp.array2d(dtype=wp.float32),
    grad1: wp.array2d(dtype=wp.float32),
    inv_dx0: float,
    inv_dx1: float,
    grad_field: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = grad0.shape[0]
    n1 = grad0.shape[1]

    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)
    jm = _wrap_minus1(j, n1)
    jp = _wrap_plus1(j, n1)

    gx = (grad0[im, j] - grad0[ip, j]) * (0.5 * inv_dx0)
    gy = (grad1[i, jm] - grad1[i, jp]) * (0.5 * inv_dx1)
    grad_field[i, j] = gx + gy


@wp.kernel
def _uniform_grid_gradient_2d_order4_backward_kernel(
    grad0: wp.array2d(dtype=wp.float32),
    grad1: wp.array2d(dtype=wp.float32),
    inv_dx0: float,
    inv_dx1: float,
    grad_field: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = grad0.shape[0]
    n1 = grad0.shape[1]

    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)

    jm1 = _wrap_minus1(j, n1)
    jp1 = _wrap_plus1(j, n1)
    jm2 = _wrap_minus2(j, n1)
    jp2 = _wrap_plus2(j, n1)

    gx = (grad0[ip2, j] - 8.0 * grad0[ip1, j] + 8.0 * grad0[im1, j] - grad0[im2, j]) * (
        inv_dx0 / 12.0
    )
    gy = (grad1[i, jp2] - 8.0 * grad1[i, jp1] + 8.0 * grad1[i, jm1] - grad1[i, jm2]) * (
        inv_dx1 / 12.0
    )
    grad_field[i, j] = gx + gy


@wp.kernel
def _uniform_grid_gradient_3d_backward_kernel(
    grad0: wp.array3d(dtype=wp.float32),
    grad1: wp.array3d(dtype=wp.float32),
    grad2: wp.array3d(dtype=wp.float32),
    inv_dx0: float,
    inv_dx1: float,
    inv_dx2: float,
    grad_field: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = grad0.shape[0]
    n1 = grad0.shape[1]
    n2 = grad0.shape[2]

    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)
    jm = _wrap_minus1(j, n1)
    jp = _wrap_plus1(j, n1)
    km = _wrap_minus1(k, n2)
    kp = _wrap_plus1(k, n2)

    gx = (grad0[im, j, k] - grad0[ip, j, k]) * (0.5 * inv_dx0)
    gy = (grad1[i, jm, k] - grad1[i, jp, k]) * (0.5 * inv_dx1)
    gz = (grad2[i, j, km] - grad2[i, j, kp]) * (0.5 * inv_dx2)
    grad_field[i, j, k] = gx + gy + gz


@wp.kernel
def _uniform_grid_gradient_3d_order4_backward_kernel(
    grad0: wp.array3d(dtype=wp.float32),
    grad1: wp.array3d(dtype=wp.float32),
    grad2: wp.array3d(dtype=wp.float32),
    inv_dx0: float,
    inv_dx1: float,
    inv_dx2: float,
    grad_field: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = grad0.shape[0]
    n1 = grad0.shape[1]
    n2 = grad0.shape[2]

    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)

    jm1 = _wrap_minus1(j, n1)
    jp1 = _wrap_plus1(j, n1)
    jm2 = _wrap_minus2(j, n1)
    jp2 = _wrap_plus2(j, n1)

    km1 = _wrap_minus1(k, n2)
    kp1 = _wrap_plus1(k, n2)
    km2 = _wrap_minus2(k, n2)
    kp2 = _wrap_plus2(k, n2)

    gx = (
        grad0[ip2, j, k]
        - 8.0 * grad0[ip1, j, k]
        + 8.0 * grad0[im1, j, k]
        - grad0[im2, j, k]
    ) * (inv_dx0 / 12.0)
    gy = (
        grad1[i, jp2, k]
        - 8.0 * grad1[i, jp1, k]
        + 8.0 * grad1[i, jm1, k]
        - grad1[i, jm2, k]
    ) * (inv_dx1 / 12.0)
    gz = (
        grad2[i, j, kp2]
        - 8.0 * grad2[i, j, kp1]
        + 8.0 * grad2[i, j, km1]
        - grad2[i, j, km2]
    ) * (inv_dx2 / 12.0)
    grad_field[i, j, k] = gx + gy + gz


@wp.kernel
def _uniform_grid_second_derivative_1d_backward_kernel(
    grad0: wp.array(dtype=wp.float32),
    inv_dx2: float,
    grad_field: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = grad0.shape[0]
    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)
    grad_field[i] = (grad0[ip] - 2.0 * grad0[i] + grad0[im]) * inv_dx2


@wp.kernel
def _uniform_grid_second_derivative_1d_order4_backward_kernel(
    grad0: wp.array(dtype=wp.float32),
    inv_dx2: float,
    grad_field: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = grad0.shape[0]
    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)
    grad_field[i] = (
        -grad0[ip2]
        + 16.0 * grad0[ip1]
        - 30.0 * grad0[i]
        + 16.0 * grad0[im1]
        - grad0[im2]
    ) * (inv_dx2 / 12.0)


@wp.kernel
def _uniform_grid_second_derivative_2d_backward_kernel(
    grad0: wp.array2d(dtype=wp.float32),
    grad1: wp.array2d(dtype=wp.float32),
    inv_dx20: float,
    inv_dx21: float,
    grad_field: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = grad0.shape[0]
    n1 = grad0.shape[1]
    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)
    jm = _wrap_minus1(j, n1)
    jp = _wrap_plus1(j, n1)

    gxx = (grad0[ip, j] - 2.0 * grad0[i, j] + grad0[im, j]) * inv_dx20
    gyy = (grad1[i, jp] - 2.0 * grad1[i, j] + grad1[i, jm]) * inv_dx21
    grad_field[i, j] = gxx + gyy


@wp.kernel
def _uniform_grid_second_derivative_2d_order4_backward_kernel(
    grad0: wp.array2d(dtype=wp.float32),
    grad1: wp.array2d(dtype=wp.float32),
    inv_dx20: float,
    inv_dx21: float,
    grad_field: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = grad0.shape[0]
    n1 = grad0.shape[1]
    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)
    jm1 = _wrap_minus1(j, n1)
    jp1 = _wrap_plus1(j, n1)
    jm2 = _wrap_minus2(j, n1)
    jp2 = _wrap_plus2(j, n1)

    gxx = (
        -grad0[ip2, j]
        + 16.0 * grad0[ip1, j]
        - 30.0 * grad0[i, j]
        + 16.0 * grad0[im1, j]
        - grad0[im2, j]
    ) * (inv_dx20 / 12.0)
    gyy = (
        -grad1[i, jp2]
        + 16.0 * grad1[i, jp1]
        - 30.0 * grad1[i, j]
        + 16.0 * grad1[i, jm1]
        - grad1[i, jm2]
    ) * (inv_dx21 / 12.0)
    grad_field[i, j] = gxx + gyy


@wp.kernel
def _uniform_grid_second_derivative_3d_backward_kernel(
    grad0: wp.array3d(dtype=wp.float32),
    grad1: wp.array3d(dtype=wp.float32),
    grad2: wp.array3d(dtype=wp.float32),
    inv_dx20: float,
    inv_dx21: float,
    inv_dx22: float,
    grad_field: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = grad0.shape[0]
    n1 = grad0.shape[1]
    n2 = grad0.shape[2]
    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)
    jm = _wrap_minus1(j, n1)
    jp = _wrap_plus1(j, n1)
    km = _wrap_minus1(k, n2)
    kp = _wrap_plus1(k, n2)

    gxx = (grad0[ip, j, k] - 2.0 * grad0[i, j, k] + grad0[im, j, k]) * inv_dx20
    gyy = (grad1[i, jp, k] - 2.0 * grad1[i, j, k] + grad1[i, jm, k]) * inv_dx21
    gzz = (grad2[i, j, kp] - 2.0 * grad2[i, j, k] + grad2[i, j, km]) * inv_dx22
    grad_field[i, j, k] = gxx + gyy + gzz


@wp.kernel
def _uniform_grid_second_derivative_3d_order4_backward_kernel(
    grad0: wp.array3d(dtype=wp.float32),
    grad1: wp.array3d(dtype=wp.float32),
    grad2: wp.array3d(dtype=wp.float32),
    inv_dx20: float,
    inv_dx21: float,
    inv_dx22: float,
    grad_field: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = grad0.shape[0]
    n1 = grad0.shape[1]
    n2 = grad0.shape[2]
    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)
    jm1 = _wrap_minus1(j, n1)
    jp1 = _wrap_plus1(j, n1)
    jm2 = _wrap_minus2(j, n1)
    jp2 = _wrap_plus2(j, n1)
    km1 = _wrap_minus1(k, n2)
    kp1 = _wrap_plus1(k, n2)
    km2 = _wrap_minus2(k, n2)
    kp2 = _wrap_plus2(k, n2)

    gxx = (
        -grad0[ip2, j, k]
        + 16.0 * grad0[ip1, j, k]
        - 30.0 * grad0[i, j, k]
        + 16.0 * grad0[im1, j, k]
        - grad0[im2, j, k]
    ) * (inv_dx20 / 12.0)
    gyy = (
        -grad1[i, jp2, k]
        + 16.0 * grad1[i, jp1, k]
        - 30.0 * grad1[i, j, k]
        + 16.0 * grad1[i, jm1, k]
        - grad1[i, jm2, k]
    ) * (inv_dx21 / 12.0)
    gzz = (
        -grad2[i, j, kp2]
        + 16.0 * grad2[i, j, kp1]
        - 30.0 * grad2[i, j, k]
        + 16.0 * grad2[i, j, km1]
        - grad2[i, j, km2]
    ) * (inv_dx22 / 12.0)
    grad_field[i, j, k] = gxx + gyy + gzz


def _normalize_spacing(
    spacing: float | Sequence[float], ndim: int
) -> tuple[float, ...]:
    ### Normalize scalar/list spacing into one value per axis.
    if isinstance(spacing, (float, int)):
        return tuple(float(spacing) for _ in range(ndim))
    spacing_tuple = tuple(float(x) for x in spacing)
    if len(spacing_tuple) != ndim:
        raise ValueError(
            f"spacing must have {ndim} entries for a {ndim}D field, got {len(spacing_tuple)}"
        )
    return spacing_tuple


def _validate_order(order: int) -> int:
    ### Validate finite-difference order selection.
    if not isinstance(order, int):
        raise TypeError(f"order must be an integer, got {type(order)}")
    if order not in _SUPPORTED_ORDERS:
        raise ValueError(
            f"uniform_grid_gradient supports {list(_SUPPORTED_ORDERS)} central orders, got order={order}"
        )
    return order


def _validate_derivative_order(derivative_order: int) -> int:
    ### Validate derivative-order selection (first vs pure second derivative).
    if not isinstance(derivative_order, int):
        raise TypeError(
            f"derivative_order must be an integer, got {type(derivative_order)}"
        )
    if derivative_order not in _SUPPORTED_DERIVATIVE_ORDERS:
        raise ValueError(
            "uniform_grid_gradient supports derivative_order in [1, 2], "
            f"got derivative_order={derivative_order}"
        )
    return derivative_order


def _validate_include_mixed(
    *,
    derivative_order: int,
    include_mixed: bool,
) -> None:
    ### Phase-1 guard: mixed second derivatives are intentionally not yet exposed.
    if not isinstance(include_mixed, bool):
        raise TypeError(f"include_mixed must be a bool, got {type(include_mixed)}")
    if include_mixed and derivative_order != 2:
        raise ValueError("include_mixed is only valid when derivative_order=2")
    if include_mixed:
        raise NotImplementedError(
            "include_mixed=True is not yet supported; phase-1 supports pure axis-wise "
            "second derivatives only"
        )


def _validate_field(field: torch.Tensor) -> None:
    ### Validate field shape and dtype.
    if field.ndim < 1 or field.ndim > 3:
        raise ValueError(
            f"uniform_grid_gradient supports 1D-3D fields, got {field.shape=}"
        )
    if not torch.is_floating_point(field):
        raise TypeError("field must be a floating-point tensor")


def _wp_launch(
    *,
    kernel,
    dim,
    inputs,
    device,
    stream,
) -> None:
    ### Launch a Warp kernel, optionally overriding block size.
    if _WARP_BLOCK_DIM > 0:
        wp.launch(
            kernel=kernel,
            dim=dim,
            inputs=inputs,
            device=device,
            stream=stream,
            block_dim=_WARP_BLOCK_DIM,
        )
        return
    wp.launch(
        kernel=kernel,
        dim=dim,
        inputs=inputs,
        device=device,
        stream=stream,
    )


def _launch_forward(
    *,
    field_fp32: torch.Tensor,
    spacing_tuple: tuple[float, ...],
    order: int,
    derivative_order: int,
    grad_components: list[torch.Tensor],
    wp_device,
    wp_stream,
) -> None:
    ### Launch dimensionality/order-specific forward kernels.
    with wp.ScopedStream(wp_stream):
        if field_fp32.ndim == 1:
            wp_field = wp.from_torch(field_fp32, dtype=wp.float32)
            wp_grad0 = wp.from_torch(grad_components[0], dtype=wp.float32)
            if derivative_order == 1:
                inv_dx0 = 1.0 / float(spacing_tuple[0])
                kernel = (
                    _uniform_grid_gradient_1d_kernel
                    if order == 2
                    else _uniform_grid_gradient_1d_order4_kernel
                )
            else:
                inv_dx0 = 1.0 / float(spacing_tuple[0] * spacing_tuple[0])
                kernel = (
                    _uniform_grid_second_derivative_1d_kernel
                    if order == 2
                    else _uniform_grid_second_derivative_1d_order4_kernel
                )
            _wp_launch(
                kernel=kernel,
                dim=field_fp32.shape[0],
                inputs=[wp_field, inv_dx0, wp_grad0],
                device=wp_device,
                stream=wp_stream,
            )
            return

        if field_fp32.ndim == 2:
            wp_field = wp.from_torch(field_fp32, dtype=wp.float32)
            wp_grad0 = wp.from_torch(grad_components[0], dtype=wp.float32)
            wp_grad1 = wp.from_torch(grad_components[1], dtype=wp.float32)
            if derivative_order == 1:
                inv_dx0 = 1.0 / float(spacing_tuple[0])
                inv_dx1 = 1.0 / float(spacing_tuple[1])
                kernel = (
                    _uniform_grid_gradient_2d_kernel
                    if order == 2
                    else _uniform_grid_gradient_2d_order4_kernel
                )
            else:
                inv_dx0 = 1.0 / float(spacing_tuple[0] * spacing_tuple[0])
                inv_dx1 = 1.0 / float(spacing_tuple[1] * spacing_tuple[1])
                kernel = (
                    _uniform_grid_second_derivative_2d_kernel
                    if order == 2
                    else _uniform_grid_second_derivative_2d_order4_kernel
                )
            _wp_launch(
                kernel=kernel,
                dim=field_fp32.shape,
                inputs=[
                    wp_field,
                    inv_dx0,
                    inv_dx1,
                    wp_grad0,
                    wp_grad1,
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        wp_field = wp.from_torch(field_fp32, dtype=wp.float32)
        wp_grad0 = wp.from_torch(grad_components[0], dtype=wp.float32)
        wp_grad1 = wp.from_torch(grad_components[1], dtype=wp.float32)
        wp_grad2 = wp.from_torch(grad_components[2], dtype=wp.float32)
        if derivative_order == 1:
            inv_dx0 = 1.0 / float(spacing_tuple[0])
            inv_dx1 = 1.0 / float(spacing_tuple[1])
            inv_dx2 = 1.0 / float(spacing_tuple[2])
            kernel = (
                _uniform_grid_gradient_3d_kernel
                if order == 2
                else _uniform_grid_gradient_3d_order4_kernel
            )
        else:
            inv_dx0 = 1.0 / float(spacing_tuple[0] * spacing_tuple[0])
            inv_dx1 = 1.0 / float(spacing_tuple[1] * spacing_tuple[1])
            inv_dx2 = 1.0 / float(spacing_tuple[2] * spacing_tuple[2])
            kernel = (
                _uniform_grid_second_derivative_3d_kernel
                if order == 2
                else _uniform_grid_second_derivative_3d_order4_kernel
            )
        _wp_launch(
            kernel=kernel,
            dim=field_fp32.shape,
            inputs=[
                wp_field,
                inv_dx0,
                inv_dx1,
                inv_dx2,
                wp_grad0,
                wp_grad1,
                wp_grad2,
            ],
            device=wp_device,
            stream=wp_stream,
        )


def _launch_backward(
    *,
    grad_output_fp32: torch.Tensor,
    spacing_tuple: tuple[float, ...],
    order: int,
    derivative_order: int,
    grad_field: torch.Tensor,
    wp_device,
    wp_stream,
) -> None:
    ### Launch dimensionality/order-specific backward kernels.
    with wp.ScopedStream(wp_stream):
        if grad_output_fp32.ndim == 2:
            wp_grad0 = wp.from_torch(grad_output_fp32[0], dtype=wp.float32)
            wp_grad_field = wp.from_torch(grad_field, dtype=wp.float32)
            if derivative_order == 1:
                inv_dx0 = 1.0 / float(spacing_tuple[0])
                kernel = (
                    _uniform_grid_gradient_1d_backward_kernel
                    if order == 2
                    else _uniform_grid_gradient_1d_order4_backward_kernel
                )
            else:
                inv_dx0 = 1.0 / float(spacing_tuple[0] * spacing_tuple[0])
                kernel = (
                    _uniform_grid_second_derivative_1d_backward_kernel
                    if order == 2
                    else _uniform_grid_second_derivative_1d_order4_backward_kernel
                )
            _wp_launch(
                kernel=kernel,
                dim=grad_field.shape[0],
                inputs=[wp_grad0, inv_dx0, wp_grad_field],
                device=wp_device,
                stream=wp_stream,
            )
            return

        if grad_output_fp32.ndim == 3:
            wp_grad0 = wp.from_torch(grad_output_fp32[0], dtype=wp.float32)
            wp_grad1 = wp.from_torch(grad_output_fp32[1], dtype=wp.float32)
            wp_grad_field = wp.from_torch(grad_field, dtype=wp.float32)
            if derivative_order == 1:
                inv_dx0 = 1.0 / float(spacing_tuple[0])
                inv_dx1 = 1.0 / float(spacing_tuple[1])
                kernel = (
                    _uniform_grid_gradient_2d_backward_kernel
                    if order == 2
                    else _uniform_grid_gradient_2d_order4_backward_kernel
                )
            else:
                inv_dx0 = 1.0 / float(spacing_tuple[0] * spacing_tuple[0])
                inv_dx1 = 1.0 / float(spacing_tuple[1] * spacing_tuple[1])
                kernel = (
                    _uniform_grid_second_derivative_2d_backward_kernel
                    if order == 2
                    else _uniform_grid_second_derivative_2d_order4_backward_kernel
                )
            _wp_launch(
                kernel=kernel,
                dim=grad_field.shape,
                inputs=[
                    wp_grad0,
                    wp_grad1,
                    inv_dx0,
                    inv_dx1,
                    wp_grad_field,
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        wp_grad0 = wp.from_torch(grad_output_fp32[0], dtype=wp.float32)
        wp_grad1 = wp.from_torch(grad_output_fp32[1], dtype=wp.float32)
        wp_grad2 = wp.from_torch(grad_output_fp32[2], dtype=wp.float32)
        wp_grad_field = wp.from_torch(grad_field, dtype=wp.float32)
        if derivative_order == 1:
            inv_dx0 = 1.0 / float(spacing_tuple[0])
            inv_dx1 = 1.0 / float(spacing_tuple[1])
            inv_dx2 = 1.0 / float(spacing_tuple[2])
            kernel = (
                _uniform_grid_gradient_3d_backward_kernel
                if order == 2
                else _uniform_grid_gradient_3d_order4_backward_kernel
            )
        else:
            inv_dx0 = 1.0 / float(spacing_tuple[0] * spacing_tuple[0])
            inv_dx1 = 1.0 / float(spacing_tuple[1] * spacing_tuple[1])
            inv_dx2 = 1.0 / float(spacing_tuple[2] * spacing_tuple[2])
            kernel = (
                _uniform_grid_second_derivative_3d_backward_kernel
                if order == 2
                else _uniform_grid_second_derivative_3d_order4_backward_kernel
            )
        _wp_launch(
            kernel=kernel,
            dim=grad_field.shape,
            inputs=[
                wp_grad0,
                wp_grad1,
                wp_grad2,
                inv_dx0,
                inv_dx1,
                inv_dx2,
                wp_grad_field,
            ],
            device=wp_device,
            stream=wp_stream,
        )


def _warp_launch_context(field: torch.Tensor):
    ### Resolve warp launch context without per-call dynamic imports.
    if field.device.type == "cuda":
        return None, wp.stream_from_torch(torch.cuda.current_stream(field.device))
    return "cpu", None


@torch.library.custom_op(
    "physicsnemo::uniform_grid_gradient_warp_impl", mutates_args=()
)
def uniform_grid_gradient_impl(
    field: torch.Tensor,
    spacing_meta: torch.Tensor,
    order: int,
    derivative_order: int,
    include_mixed: bool,
) -> torch.Tensor:
    """Compute periodic first or pure second derivatives on a uniform grid."""
    _validate_field(field)
    spacing_tuple = tuple(float(v) for v in spacing_meta.tolist())
    for dx in spacing_tuple:
        if dx <= 0.0:
            raise ValueError("all spacing entries must be strictly positive")
    order = _validate_order(int(order))
    derivative_order = _validate_derivative_order(int(derivative_order))
    _validate_include_mixed(
        derivative_order=derivative_order,
        include_mixed=bool(include_mixed),
    )

    orig_dtype = field.dtype
    field_fp32 = (
        field
        if field.dtype == torch.float32 and field.is_contiguous()
        else field.to(dtype=torch.float32).contiguous()
    )

    ### Write gradients directly into preallocated output slices to avoid stack copy.
    output_fp32 = torch.empty(
        (field_fp32.ndim, *field_fp32.shape),
        device=field_fp32.device,
        dtype=torch.float32,
    )
    grad_components = [output_fp32[axis] for axis in range(field_fp32.ndim)]

    wp_device, wp_stream = _warp_launch_context(field_fp32)
    _launch_forward(
        field_fp32=field_fp32,
        spacing_tuple=spacing_tuple,
        order=order,
        derivative_order=derivative_order,
        grad_components=grad_components,
        wp_device=wp_device,
        wp_stream=wp_stream,
    )

    if output_fp32.dtype != orig_dtype:
        return output_fp32.to(dtype=orig_dtype)
    return output_fp32


@uniform_grid_gradient_impl.register_fake
def _uniform_grid_gradient_impl_fake(
    field: torch.Tensor,
    spacing_meta: torch.Tensor,
    order: int,
    derivative_order: int,
    include_mixed: bool,
) -> torch.Tensor:
    """Fake tensor propagation for uniform-grid custom op."""
    _ = (spacing_meta, order, derivative_order, include_mixed)
    return torch.empty(
        (field.ndim, *field.shape),
        device=field.device,
        dtype=field.dtype,
    )


def setup_uniform_grid_gradient_context(
    ctx: torch.autograd.function.FunctionCtx, inputs: tuple, output: torch.Tensor
) -> None:
    """Store backward context for uniform-grid custom-op autograd."""
    field, spacing_meta, order, derivative_order, include_mixed = inputs
    _ = output
    ctx.spacing_tuple = tuple(float(v) for v in spacing_meta.tolist())
    ctx.order = int(order)
    ctx.derivative_order = int(derivative_order)
    ctx.include_mixed = bool(include_mixed)
    ctx.orig_dtype = field.dtype


def backward_uniform_grid_gradient(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[torch.Tensor | None, None, None, None, None]:
    """Backward pass for uniform-grid custom op (gradients wrt field only)."""
    if grad_output is None or not ctx.needs_input_grad[0]:
        return None, None, None, None, None

    grad_output_fp32 = (
        grad_output
        if grad_output.dtype == torch.float32 and grad_output.is_contiguous()
        else grad_output.to(dtype=torch.float32).contiguous()
    )
    grad_field = torch.empty_like(grad_output_fp32[0])

    wp_device, wp_stream = _warp_launch_context(grad_output_fp32)
    _launch_backward(
        grad_output_fp32=grad_output_fp32,
        spacing_tuple=ctx.spacing_tuple,
        order=ctx.order,
        derivative_order=ctx.derivative_order,
        grad_field=grad_field,
        wp_device=wp_device,
        wp_stream=wp_stream,
    )

    if grad_field.dtype != ctx.orig_dtype:
        grad_field = grad_field.to(dtype=ctx.orig_dtype)
    return grad_field, None, None, None, None


uniform_grid_gradient_impl.register_autograd(
    backward_uniform_grid_gradient,
    setup_context=setup_uniform_grid_gradient_context,
)


def uniform_grid_gradient_warp(
    field: torch.Tensor,
    spacing: float | Sequence[float] = 1.0,
    order: int = 2,
    derivative_order: int = 1,
    include_mixed: bool = False,
) -> torch.Tensor:
    """Compute periodic first or pure second derivatives on a uniform grid."""
    _validate_field(field)
    spacing_tuple = _normalize_spacing(spacing, field.ndim)
    for dx in spacing_tuple:
        if dx <= 0.0:
            raise ValueError("all spacing entries must be strictly positive")
    order = _validate_order(order)
    derivative_order = _validate_derivative_order(derivative_order)
    _validate_include_mixed(
        derivative_order=derivative_order,
        include_mixed=include_mixed,
    )
    spacing_meta = torch.tensor(spacing_tuple, dtype=torch.float32, device="cpu")
    return uniform_grid_gradient_impl(
        field,
        spacing_meta,
        int(order),
        int(derivative_order),
        bool(include_mixed),
    )
