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

from physicsnemo.core.function_spec import FunctionSpec

from ._torch_impl import rectilinear_grid_gradient_torch
from .utils import (
    validate_and_normalize_coordinates,
    validate_derivative_request,
    validate_field,
)

### Warp runtime initialization for custom kernels.
wp.init()
wp.config.quiet = True


@wp.func
def _axis_coeff(
    coords: wp.array(dtype=wp.float32),
    period: float,
    idx: int,
) -> wp.vec3f:
    ### Compute nonuniform periodic central-difference weights at one index.
    n = coords.shape[0]
    im = (idx + n - 1) % n
    ip = (idx + 1) % n

    xi = coords[idx]
    xim = coords[im]
    xip = coords[ip]

    h_minus = xi - xim
    if idx == 0:
        h_minus = xi + period - xim

    h_plus = xip - xi
    if idx == (n - 1):
        h_plus = xip + period - xi

    denom = h_minus + h_plus
    w_minus = -h_plus / (h_minus * denom)
    w_center = (h_plus - h_minus) / (h_minus * h_plus)
    w_plus = h_minus / (h_plus * denom)
    return wp.vec3f(w_minus, w_center, w_plus)


@wp.func
def _axis_second_coeff(
    coords: wp.array(dtype=wp.float32),
    period: float,
    idx: int,
) -> wp.vec3f:
    ### Compute nonuniform periodic second-derivative weights at one index.
    n = coords.shape[0]
    im = (idx + n - 1) % n
    ip = (idx + 1) % n

    xi = coords[idx]
    xim = coords[im]
    xip = coords[ip]

    h_minus = xi - xim
    if idx == 0:
        h_minus = xi + period - xim

    h_plus = xip - xi
    if idx == (n - 1):
        h_plus = xip + period - xi

    denom = h_minus + h_plus
    w_minus = 2.0 / (h_minus * denom)
    w_center = -2.0 / (h_minus * h_plus)
    w_plus = 2.0 / (h_plus * denom)
    return wp.vec3f(w_minus, w_center, w_plus)


### ============================================================
### Forward kernels (rectilinear periodic central differences)
### ============================================================


@wp.kernel
def _rectilinear_gradient_1d_kernel(
    field: wp.array(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    period0: float,
    grad0: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = field.shape[0]
    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0

    coeff = _axis_coeff(x0, period0, i)
    grad0[i] = coeff[0] * field[im] + coeff[1] * field[i] + coeff[2] * field[ip]


@wp.kernel
def _rectilinear_gradient_2d_kernel(
    field: wp.array2d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    x1: wp.array(dtype=wp.float32),
    period0: float,
    period1: float,
    grad0: wp.array2d(dtype=wp.float32),
    grad1: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]
    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0
    jm = (j + n1 - 1) % n1
    jp = (j + 1) % n1

    cx = _axis_coeff(x0, period0, i)
    cy = _axis_coeff(x1, period1, j)

    grad0[i, j] = cx[0] * field[im, j] + cx[1] * field[i, j] + cx[2] * field[ip, j]
    grad1[i, j] = cy[0] * field[i, jm] + cy[1] * field[i, j] + cy[2] * field[i, jp]


@wp.kernel
def _rectilinear_gradient_3d_kernel(
    field: wp.array3d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    x1: wp.array(dtype=wp.float32),
    x2: wp.array(dtype=wp.float32),
    period0: float,
    period1: float,
    period2: float,
    grad0: wp.array3d(dtype=wp.float32),
    grad1: wp.array3d(dtype=wp.float32),
    grad2: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]
    n2 = field.shape[2]
    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0
    jm = (j + n1 - 1) % n1
    jp = (j + 1) % n1
    km = (k + n2 - 1) % n2
    kp = (k + 1) % n2

    cx = _axis_coeff(x0, period0, i)
    cy = _axis_coeff(x1, period1, j)
    cz = _axis_coeff(x2, period2, k)

    grad0[i, j, k] = (
        cx[0] * field[im, j, k] + cx[1] * field[i, j, k] + cx[2] * field[ip, j, k]
    )
    grad1[i, j, k] = (
        cy[0] * field[i, jm, k] + cy[1] * field[i, j, k] + cy[2] * field[i, jp, k]
    )
    grad2[i, j, k] = (
        cz[0] * field[i, j, km] + cz[1] * field[i, j, k] + cz[2] * field[i, j, kp]
    )


@wp.kernel
def _rectilinear_second_derivative_1d_kernel(
    field: wp.array(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    period0: float,
    grad0: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = field.shape[0]
    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0

    coeff = _axis_second_coeff(x0, period0, i)
    grad0[i] = coeff[0] * field[im] + coeff[1] * field[i] + coeff[2] * field[ip]


@wp.kernel
def _rectilinear_second_derivative_2d_kernel(
    field: wp.array2d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    x1: wp.array(dtype=wp.float32),
    period0: float,
    period1: float,
    grad0: wp.array2d(dtype=wp.float32),
    grad1: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]
    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0
    jm = (j + n1 - 1) % n1
    jp = (j + 1) % n1

    cx = _axis_second_coeff(x0, period0, i)
    cy = _axis_second_coeff(x1, period1, j)

    grad0[i, j] = cx[0] * field[im, j] + cx[1] * field[i, j] + cx[2] * field[ip, j]
    grad1[i, j] = cy[0] * field[i, jm] + cy[1] * field[i, j] + cy[2] * field[i, jp]


@wp.kernel
def _rectilinear_second_derivative_3d_kernel(
    field: wp.array3d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    x1: wp.array(dtype=wp.float32),
    x2: wp.array(dtype=wp.float32),
    period0: float,
    period1: float,
    period2: float,
    grad0: wp.array3d(dtype=wp.float32),
    grad1: wp.array3d(dtype=wp.float32),
    grad2: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]
    n2 = field.shape[2]
    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0
    jm = (j + n1 - 1) % n1
    jp = (j + 1) % n1
    km = (k + n2 - 1) % n2
    kp = (k + 1) % n2

    cx = _axis_second_coeff(x0, period0, i)
    cy = _axis_second_coeff(x1, period1, j)
    cz = _axis_second_coeff(x2, period2, k)

    grad0[i, j, k] = (
        cx[0] * field[im, j, k] + cx[1] * field[i, j, k] + cx[2] * field[ip, j, k]
    )
    grad1[i, j, k] = (
        cy[0] * field[i, jm, k] + cy[1] * field[i, j, k] + cy[2] * field[i, jp, k]
    )
    grad2[i, j, k] = (
        cz[0] * field[i, j, km] + cz[1] * field[i, j, k] + cz[2] * field[i, j, kp]
    )


### ============================================================
### Backward kernels (adjoint of rectilinear central differences)
### ============================================================


@wp.kernel
def _rectilinear_gradient_1d_backward_kernel(
    grad0: wp.array(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    period0: float,
    grad_field: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = grad0.shape[0]
    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0

    ci = _axis_coeff(x0, period0, i)
    cip = _axis_coeff(x0, period0, ip)
    cim = _axis_coeff(x0, period0, im)
    grad_field[i] = ci[1] * grad0[i] + cip[0] * grad0[ip] + cim[2] * grad0[im]


@wp.kernel
def _rectilinear_gradient_2d_backward_kernel(
    grad0: wp.array2d(dtype=wp.float32),
    grad1: wp.array2d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    x1: wp.array(dtype=wp.float32),
    period0: float,
    period1: float,
    grad_field: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = grad0.shape[0]
    n1 = grad0.shape[1]

    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0
    jm = (j + n1 - 1) % n1
    jp = (j + 1) % n1

    cxi = _axis_coeff(x0, period0, i)
    cxip = _axis_coeff(x0, period0, ip)
    cxim = _axis_coeff(x0, period0, im)

    cyi = _axis_coeff(x1, period1, j)
    cyip = _axis_coeff(x1, period1, jp)
    cyim = _axis_coeff(x1, period1, jm)

    gx = cxi[1] * grad0[i, j] + cxip[0] * grad0[ip, j] + cxim[2] * grad0[im, j]
    gy = cyi[1] * grad1[i, j] + cyip[0] * grad1[i, jp] + cyim[2] * grad1[i, jm]
    grad_field[i, j] = gx + gy


@wp.kernel
def _rectilinear_gradient_3d_backward_kernel(
    grad0: wp.array3d(dtype=wp.float32),
    grad1: wp.array3d(dtype=wp.float32),
    grad2: wp.array3d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    x1: wp.array(dtype=wp.float32),
    x2: wp.array(dtype=wp.float32),
    period0: float,
    period1: float,
    period2: float,
    grad_field: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = grad0.shape[0]
    n1 = grad0.shape[1]
    n2 = grad0.shape[2]

    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0
    jm = (j + n1 - 1) % n1
    jp = (j + 1) % n1
    km = (k + n2 - 1) % n2
    kp = (k + 1) % n2

    cxi = _axis_coeff(x0, period0, i)
    cxip = _axis_coeff(x0, period0, ip)
    cxim = _axis_coeff(x0, period0, im)

    cyi = _axis_coeff(x1, period1, j)
    cyip = _axis_coeff(x1, period1, jp)
    cyim = _axis_coeff(x1, period1, jm)

    czi = _axis_coeff(x2, period2, k)
    czip = _axis_coeff(x2, period2, kp)
    czim = _axis_coeff(x2, period2, km)

    gx = cxi[1] * grad0[i, j, k] + cxip[0] * grad0[ip, j, k] + cxim[2] * grad0[im, j, k]
    gy = cyi[1] * grad1[i, j, k] + cyip[0] * grad1[i, jp, k] + cyim[2] * grad1[i, jm, k]
    gz = czi[1] * grad2[i, j, k] + czip[0] * grad2[i, j, kp] + czim[2] * grad2[i, j, km]
    grad_field[i, j, k] = gx + gy + gz


@wp.kernel
def _rectilinear_second_derivative_1d_backward_kernel(
    grad0: wp.array(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    period0: float,
    grad_field: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = grad0.shape[0]
    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0

    ci = _axis_second_coeff(x0, period0, i)
    cip = _axis_second_coeff(x0, period0, ip)
    cim = _axis_second_coeff(x0, period0, im)
    grad_field[i] = ci[1] * grad0[i] + cip[0] * grad0[ip] + cim[2] * grad0[im]


@wp.kernel
def _rectilinear_second_derivative_2d_backward_kernel(
    grad0: wp.array2d(dtype=wp.float32),
    grad1: wp.array2d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    x1: wp.array(dtype=wp.float32),
    period0: float,
    period1: float,
    grad_field: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = grad0.shape[0]
    n1 = grad0.shape[1]

    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0
    jm = (j + n1 - 1) % n1
    jp = (j + 1) % n1

    cxi = _axis_second_coeff(x0, period0, i)
    cxip = _axis_second_coeff(x0, period0, ip)
    cxim = _axis_second_coeff(x0, period0, im)

    cyi = _axis_second_coeff(x1, period1, j)
    cyip = _axis_second_coeff(x1, period1, jp)
    cyim = _axis_second_coeff(x1, period1, jm)

    gx = cxi[1] * grad0[i, j] + cxip[0] * grad0[ip, j] + cxim[2] * grad0[im, j]
    gy = cyi[1] * grad1[i, j] + cyip[0] * grad1[i, jp] + cyim[2] * grad1[i, jm]
    grad_field[i, j] = gx + gy


@wp.kernel
def _rectilinear_second_derivative_3d_backward_kernel(
    grad0: wp.array3d(dtype=wp.float32),
    grad1: wp.array3d(dtype=wp.float32),
    grad2: wp.array3d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    x1: wp.array(dtype=wp.float32),
    x2: wp.array(dtype=wp.float32),
    period0: float,
    period1: float,
    period2: float,
    grad_field: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = grad0.shape[0]
    n1 = grad0.shape[1]
    n2 = grad0.shape[2]

    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0
    jm = (j + n1 - 1) % n1
    jp = (j + 1) % n1
    km = (k + n2 - 1) % n2
    kp = (k + 1) % n2

    cxi = _axis_second_coeff(x0, period0, i)
    cxip = _axis_second_coeff(x0, period0, ip)
    cxim = _axis_second_coeff(x0, period0, im)

    cyi = _axis_second_coeff(x1, period1, j)
    cyip = _axis_second_coeff(x1, period1, jp)
    cyim = _axis_second_coeff(x1, period1, jm)

    czi = _axis_second_coeff(x2, period2, k)
    czip = _axis_second_coeff(x2, period2, kp)
    czim = _axis_second_coeff(x2, period2, km)

    gx = cxi[1] * grad0[i, j, k] + cxip[0] * grad0[ip, j, k] + cxim[2] * grad0[im, j, k]
    gy = cyi[1] * grad1[i, j, k] + cyip[0] * grad1[i, jp, k] + cyim[2] * grad1[i, jm, k]
    gz = czi[1] * grad2[i, j, k] + czip[0] * grad2[i, j, kp] + czim[2] * grad2[i, j, km]
    grad_field[i, j, k] = gx + gy + gz


def _launch_forward(
    *,
    field_fp32: torch.Tensor,
    coords_tuple: tuple[torch.Tensor, ...],
    period_tuple: tuple[float, ...],
    derivative_order: int,
    grad_components: list[torch.Tensor],
    wp_device,
    wp_stream,
) -> None:
    ### Launch dimensionality-specific forward kernels.
    with wp.ScopedStream(wp_stream):
        if field_fp32.ndim == 1:
            wp.launch(
                kernel=(
                    _rectilinear_gradient_1d_kernel
                    if derivative_order == 1
                    else _rectilinear_second_derivative_1d_kernel
                ),
                dim=field_fp32.shape[0],
                inputs=[
                    wp.from_torch(field_fp32, dtype=wp.float32),
                    wp.from_torch(coords_tuple[0], dtype=wp.float32),
                    float(period_tuple[0]),
                    wp.from_torch(grad_components[0], dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        if field_fp32.ndim == 2:
            wp.launch(
                kernel=(
                    _rectilinear_gradient_2d_kernel
                    if derivative_order == 1
                    else _rectilinear_second_derivative_2d_kernel
                ),
                dim=field_fp32.shape,
                inputs=[
                    wp.from_torch(field_fp32, dtype=wp.float32),
                    wp.from_torch(coords_tuple[0], dtype=wp.float32),
                    wp.from_torch(coords_tuple[1], dtype=wp.float32),
                    float(period_tuple[0]),
                    float(period_tuple[1]),
                    wp.from_torch(grad_components[0], dtype=wp.float32),
                    wp.from_torch(grad_components[1], dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        wp.launch(
            kernel=(
                _rectilinear_gradient_3d_kernel
                if derivative_order == 1
                else _rectilinear_second_derivative_3d_kernel
            ),
            dim=field_fp32.shape,
            inputs=[
                wp.from_torch(field_fp32, dtype=wp.float32),
                wp.from_torch(coords_tuple[0], dtype=wp.float32),
                wp.from_torch(coords_tuple[1], dtype=wp.float32),
                wp.from_torch(coords_tuple[2], dtype=wp.float32),
                float(period_tuple[0]),
                float(period_tuple[1]),
                float(period_tuple[2]),
                wp.from_torch(grad_components[0], dtype=wp.float32),
                wp.from_torch(grad_components[1], dtype=wp.float32),
                wp.from_torch(grad_components[2], dtype=wp.float32),
            ],
            device=wp_device,
            stream=wp_stream,
        )


def _launch_backward(
    *,
    grad_output_fp32: torch.Tensor,
    coords_tuple: tuple[torch.Tensor, ...],
    period_tuple: tuple[float, ...],
    derivative_order: int,
    grad_field: torch.Tensor,
    wp_device,
    wp_stream,
) -> None:
    ### Launch dimensionality-specific backward kernels.
    with wp.ScopedStream(wp_stream):
        if grad_output_fp32.ndim == 2:
            wp.launch(
                kernel=(
                    _rectilinear_gradient_1d_backward_kernel
                    if derivative_order == 1
                    else _rectilinear_second_derivative_1d_backward_kernel
                ),
                dim=grad_field.shape[0],
                inputs=[
                    wp.from_torch(grad_output_fp32[0], dtype=wp.float32),
                    wp.from_torch(coords_tuple[0], dtype=wp.float32),
                    float(period_tuple[0]),
                    wp.from_torch(grad_field, dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        if grad_output_fp32.ndim == 3:
            wp.launch(
                kernel=(
                    _rectilinear_gradient_2d_backward_kernel
                    if derivative_order == 1
                    else _rectilinear_second_derivative_2d_backward_kernel
                ),
                dim=grad_field.shape,
                inputs=[
                    wp.from_torch(grad_output_fp32[0], dtype=wp.float32),
                    wp.from_torch(grad_output_fp32[1], dtype=wp.float32),
                    wp.from_torch(coords_tuple[0], dtype=wp.float32),
                    wp.from_torch(coords_tuple[1], dtype=wp.float32),
                    float(period_tuple[0]),
                    float(period_tuple[1]),
                    wp.from_torch(grad_field, dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
            return

        wp.launch(
            kernel=(
                _rectilinear_gradient_3d_backward_kernel
                if derivative_order == 1
                else _rectilinear_second_derivative_3d_backward_kernel
            ),
            dim=grad_field.shape,
            inputs=[
                wp.from_torch(grad_output_fp32[0], dtype=wp.float32),
                wp.from_torch(grad_output_fp32[1], dtype=wp.float32),
                wp.from_torch(grad_output_fp32[2], dtype=wp.float32),
                wp.from_torch(coords_tuple[0], dtype=wp.float32),
                wp.from_torch(coords_tuple[1], dtype=wp.float32),
                wp.from_torch(coords_tuple[2], dtype=wp.float32),
                float(period_tuple[0]),
                float(period_tuple[1]),
                float(period_tuple[2]),
                wp.from_torch(grad_field, dtype=wp.float32),
            ],
            device=wp_device,
            stream=wp_stream,
        )


def _rectilinear_forward_common(
    field: torch.Tensor,
    coords_tuple: tuple[torch.Tensor, ...],
    period_tuple: tuple[float, ...],
    derivative_order: int,
    include_mixed: bool,
) -> torch.Tensor:
    """Run rectilinear forward kernels and restore the caller dtype."""
    validate_field(field)
    derivative_order = validate_derivative_request(
        derivative_order=derivative_order,
        include_mixed=include_mixed,
    )
    coords_tuple, period_tuple = validate_and_normalize_coordinates(
        field=field,
        coordinates=coords_tuple,
        periods=period_tuple,
        coordinates_dtype=torch.float32,
        requires_grad_error="coordinate gradients are not supported in warp backend",
    )

    orig_dtype = field.dtype
    field_fp32 = field.to(dtype=torch.float32).contiguous()
    grad_components = [torch.empty_like(field_fp32) for _ in range(field_fp32.ndim)]

    wp_device, wp_stream = FunctionSpec.warp_launch_context(field_fp32)
    _launch_forward(
        field_fp32=field_fp32,
        coords_tuple=coords_tuple,
        period_tuple=period_tuple,
        derivative_order=derivative_order,
        grad_components=grad_components,
        wp_device=wp_device,
        wp_stream=wp_stream,
    )

    output = torch.stack(grad_components, dim=0)
    if output.dtype != orig_dtype:
        output = output.to(dtype=orig_dtype)
    return output


def _rectilinear_setup_common(
    ctx: torch.autograd.function.FunctionCtx,
    field: torch.Tensor,
    coords_tuple: tuple[torch.Tensor, ...],
    period_tuple: tuple[float, ...],
    derivative_order: int,
    include_mixed: bool,
) -> None:
    """Store normalized geometry metadata for rectilinear custom-op backward."""
    derivative_order = validate_derivative_request(
        derivative_order=derivative_order,
        include_mixed=include_mixed,
    )
    _, period_tuple = validate_and_normalize_coordinates(
        field=field,
        coordinates=coords_tuple,
        periods=period_tuple,
        coordinates_dtype=torch.float32,
        requires_grad_error="coordinate gradients are not supported in warp backend",
    )
    ctx.save_for_backward(
        *[coord.to(dtype=torch.float32).contiguous() for coord in coords_tuple]
    )
    ctx.period_tuple = period_tuple
    ctx.derivative_order = derivative_order
    ctx.orig_dtype = field.dtype


def _rectilinear_backward_common(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> torch.Tensor | None:
    """Evaluate the rectilinear backward kernels for one custom-op invocation."""
    if grad_output is None or not ctx.needs_input_grad[0]:
        return None

    coords_tuple = tuple(ctx.saved_tensors)
    period_tuple = tuple(float(v) for v in ctx.period_tuple)
    grad_output_fp32 = grad_output.to(dtype=torch.float32).contiguous()
    derivative_order = int(ctx.derivative_order)

    ### CUDA 1D second-derivative VJP is routed through torch autograd for numerical stability.
    if (
        derivative_order == 2
        and grad_output_fp32.device.type == "cuda"
        and grad_output_fp32.shape[0] == 1
    ):
        with torch.enable_grad():
            probe = torch.zeros_like(grad_output_fp32[0], requires_grad=True)
            probe_out = rectilinear_grid_gradient_torch(
                field=probe,
                coordinates=coords_tuple,
                periods=period_tuple,
                derivative_order=2,
                include_mixed=False,
            )
            grad_field = torch.autograd.grad(
                outputs=probe_out,
                inputs=probe,
                grad_outputs=grad_output_fp32,
                create_graph=False,
                retain_graph=False,
                allow_unused=False,
            )[0]
        if grad_field.dtype != ctx.orig_dtype:
            grad_field = grad_field.to(dtype=ctx.orig_dtype)
        return grad_field

    grad_field = torch.empty_like(grad_output_fp32[0])
    wp_device, wp_stream = FunctionSpec.warp_launch_context(grad_output_fp32)
    _launch_backward(
        grad_output_fp32=grad_output_fp32,
        coords_tuple=coords_tuple,
        period_tuple=period_tuple,
        derivative_order=derivative_order,
        grad_field=grad_field,
        wp_device=wp_device,
        wp_stream=wp_stream,
    )
    if grad_field.dtype != ctx.orig_dtype:
        grad_field = grad_field.to(dtype=ctx.orig_dtype)
    return grad_field


@torch.library.custom_op(
    "physicsnemo::rectilinear_grid_gradient_1d_warp_impl", mutates_args=()
)
def rectilinear_grid_gradient_1d_impl(
    field: torch.Tensor,
    coord0: torch.Tensor,
    period0: float,
    derivative_order: int,
    include_mixed: bool,
) -> torch.Tensor:
    """Compute periodic 1D first or pure second derivatives with Warp kernels."""
    return _rectilinear_forward_common(
        field=field,
        coords_tuple=(coord0,),
        period_tuple=(float(period0),),
        derivative_order=int(derivative_order),
        include_mixed=bool(include_mixed),
    )


@rectilinear_grid_gradient_1d_impl.register_fake
def _rectilinear_grid_gradient_1d_impl_fake(
    field: torch.Tensor,
    coord0: torch.Tensor,
    period0: float,
    derivative_order: int,
    include_mixed: bool,
) -> torch.Tensor:
    """Fake tensor propagation for 1D rectilinear custom op."""
    _ = (coord0, period0, derivative_order, include_mixed)
    return torch.empty((1, *field.shape), device=field.device, dtype=field.dtype)


def setup_rectilinear_grid_gradient_1d_context(
    ctx: torch.autograd.function.FunctionCtx, inputs: tuple, output: torch.Tensor
) -> None:
    """Store backward context for 1D rectilinear custom op."""
    field, coord0, period0, derivative_order, include_mixed = inputs
    _ = output
    _rectilinear_setup_common(
        ctx=ctx,
        field=field,
        coords_tuple=(coord0,),
        period_tuple=(float(period0),),
        derivative_order=int(derivative_order),
        include_mixed=bool(include_mixed),
    )


def backward_rectilinear_grid_gradient_1d(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[torch.Tensor | None, None, None, None, None]:
    """Backward pass for 1D rectilinear custom op (field gradients only)."""
    grad_field = _rectilinear_backward_common(ctx, grad_output)
    return grad_field, None, None, None, None


rectilinear_grid_gradient_1d_impl.register_autograd(
    backward_rectilinear_grid_gradient_1d,
    setup_context=setup_rectilinear_grid_gradient_1d_context,
)


@torch.library.custom_op(
    "physicsnemo::rectilinear_grid_gradient_2d_warp_impl", mutates_args=()
)
def rectilinear_grid_gradient_2d_impl(
    field: torch.Tensor,
    coord0: torch.Tensor,
    coord1: torch.Tensor,
    period0: float,
    period1: float,
    derivative_order: int,
    include_mixed: bool,
) -> torch.Tensor:
    """Compute periodic 2D first or pure second derivatives with Warp kernels."""
    return _rectilinear_forward_common(
        field=field,
        coords_tuple=(coord0, coord1),
        period_tuple=(float(period0), float(period1)),
        derivative_order=int(derivative_order),
        include_mixed=bool(include_mixed),
    )


@rectilinear_grid_gradient_2d_impl.register_fake
def _rectilinear_grid_gradient_2d_impl_fake(
    field: torch.Tensor,
    coord0: torch.Tensor,
    coord1: torch.Tensor,
    period0: float,
    period1: float,
    derivative_order: int,
    include_mixed: bool,
) -> torch.Tensor:
    """Fake tensor propagation for 2D rectilinear custom op."""
    _ = (coord0, coord1, period0, period1, derivative_order, include_mixed)
    return torch.empty((2, *field.shape), device=field.device, dtype=field.dtype)


def setup_rectilinear_grid_gradient_2d_context(
    ctx: torch.autograd.function.FunctionCtx, inputs: tuple, output: torch.Tensor
) -> None:
    """Store backward context for 2D rectilinear custom op."""
    field, coord0, coord1, period0, period1, derivative_order, include_mixed = inputs
    _ = output
    _rectilinear_setup_common(
        ctx=ctx,
        field=field,
        coords_tuple=(coord0, coord1),
        period_tuple=(float(period0), float(period1)),
        derivative_order=int(derivative_order),
        include_mixed=bool(include_mixed),
    )


def backward_rectilinear_grid_gradient_2d(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[torch.Tensor | None, None, None, None, None, None, None]:
    """Backward pass for 2D rectilinear custom op (field gradients only)."""
    grad_field = _rectilinear_backward_common(ctx, grad_output)
    return grad_field, None, None, None, None, None, None


rectilinear_grid_gradient_2d_impl.register_autograd(
    backward_rectilinear_grid_gradient_2d,
    setup_context=setup_rectilinear_grid_gradient_2d_context,
)


@torch.library.custom_op(
    "physicsnemo::rectilinear_grid_gradient_3d_warp_impl", mutates_args=()
)
def rectilinear_grid_gradient_3d_impl(
    field: torch.Tensor,
    coord0: torch.Tensor,
    coord1: torch.Tensor,
    coord2: torch.Tensor,
    period0: float,
    period1: float,
    period2: float,
    derivative_order: int,
    include_mixed: bool,
) -> torch.Tensor:
    """Compute periodic 3D first or pure second derivatives with Warp kernels."""
    return _rectilinear_forward_common(
        field=field,
        coords_tuple=(coord0, coord1, coord2),
        period_tuple=(float(period0), float(period1), float(period2)),
        derivative_order=int(derivative_order),
        include_mixed=bool(include_mixed),
    )


@rectilinear_grid_gradient_3d_impl.register_fake
def _rectilinear_grid_gradient_3d_impl_fake(
    field: torch.Tensor,
    coord0: torch.Tensor,
    coord1: torch.Tensor,
    coord2: torch.Tensor,
    period0: float,
    period1: float,
    period2: float,
    derivative_order: int,
    include_mixed: bool,
) -> torch.Tensor:
    """Fake tensor propagation for 3D rectilinear custom op."""
    _ = (
        coord0,
        coord1,
        coord2,
        period0,
        period1,
        period2,
        derivative_order,
        include_mixed,
    )
    return torch.empty((3, *field.shape), device=field.device, dtype=field.dtype)


def setup_rectilinear_grid_gradient_3d_context(
    ctx: torch.autograd.function.FunctionCtx, inputs: tuple, output: torch.Tensor
) -> None:
    """Store backward context for 3D rectilinear custom op."""
    (
        field,
        coord0,
        coord1,
        coord2,
        period0,
        period1,
        period2,
        derivative_order,
        include_mixed,
    ) = inputs
    _ = output
    _rectilinear_setup_common(
        ctx=ctx,
        field=field,
        coords_tuple=(coord0, coord1, coord2),
        period_tuple=(float(period0), float(period1), float(period2)),
        derivative_order=int(derivative_order),
        include_mixed=bool(include_mixed),
    )


def backward_rectilinear_grid_gradient_3d(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[torch.Tensor | None, None, None, None, None, None, None, None, None]:
    """Backward pass for 3D rectilinear custom op (field gradients only)."""
    grad_field = _rectilinear_backward_common(ctx, grad_output)
    return grad_field, None, None, None, None, None, None, None, None


rectilinear_grid_gradient_3d_impl.register_autograd(
    backward_rectilinear_grid_gradient_3d,
    setup_context=setup_rectilinear_grid_gradient_3d_context,
)


def rectilinear_grid_gradient_warp(
    field: torch.Tensor,
    coordinates: Sequence[torch.Tensor],
    periods: float | Sequence[float] | None = None,
    derivative_order: int = 1,
    include_mixed: bool = False,
) -> torch.Tensor:
    """Compute periodic first or pure second derivatives on rectilinear grids."""
    ### Validate field shape/dtype and normalize coordinates.
    validate_field(field)
    derivative_order = validate_derivative_request(
        derivative_order=derivative_order,
        include_mixed=include_mixed,
    )

    coords_tuple, period_tuple = validate_and_normalize_coordinates(
        field=field,
        coordinates=coordinates,
        periods=periods,
        coordinates_dtype=torch.float32,
        requires_grad_error="coordinate gradients are not supported in warp backend",
    )

    if field.ndim == 1:
        return rectilinear_grid_gradient_1d_impl(
            field,
            coords_tuple[0],
            float(period_tuple[0]),
            int(derivative_order),
            bool(include_mixed),
        )
    if field.ndim == 2:
        return rectilinear_grid_gradient_2d_impl(
            field,
            coords_tuple[0],
            coords_tuple[1],
            float(period_tuple[0]),
            float(period_tuple[1]),
            int(derivative_order),
            bool(include_mixed),
        )
    return rectilinear_grid_gradient_3d_impl(
        field,
        coords_tuple[0],
        coords_tuple[1],
        coords_tuple[2],
        float(period_tuple[0]),
        float(period_tuple[1]),
        float(period_tuple[2]),
        int(derivative_order),
        bool(include_mixed),
    )
