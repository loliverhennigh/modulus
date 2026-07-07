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

from .._rectilinear_grid_utils import validate_and_normalize_coordinates
from .._rectilinear_grid_warp_utils import _axis_coeff, _launch_dim
from .utils import validate_vector_field

wp.init()
wp.config.quiet = True


def _to_fp32_contiguous(tensor: torch.Tensor) -> torch.Tensor:
    if tensor.dtype == torch.float32 and tensor.is_contiguous():
        return tensor
    return tensor.to(dtype=torch.float32).contiguous()


def _restore_dtype(tensor: torch.Tensor, target_dtype: torch.dtype) -> torch.Tensor:
    if tensor.dtype == target_dtype:
        return tensor
    return tensor.to(dtype=target_dtype)


def _normalize_geometry(
    vector_field: torch.Tensor,
    coordinates: Sequence[torch.Tensor],
    periods: float | Sequence[float] | None,
) -> tuple[tuple[torch.Tensor, ...], tuple[float, ...]]:
    return validate_and_normalize_coordinates(
        field=vector_field[0],
        coordinates=coordinates,
        periods=periods,
        coordinates_dtype=torch.float32,
        requires_grad_error="coordinate gradients are not supported in warp backend",
    )


@wp.kernel
def _divergence_1d_kernel(
    vector_field: wp.array2d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    period0: float,
    output: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = output.shape[0]
    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0
    c0 = _axis_coeff(x0, period0, i)
    output[i] = (
        c0[0] * vector_field[0, im]
        + c0[1] * vector_field[0, i]
        + c0[2] * vector_field[0, ip]
    )


@wp.kernel
def _divergence_2d_kernel(
    vector_field: wp.array3d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    x1: wp.array(dtype=wp.float32),
    period0: float,
    period1: float,
    output: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = output.shape[0]
    n1 = output.shape[1]
    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0
    jm = (j + n1 - 1) % n1
    jp = (j + 1) % n1
    c0 = _axis_coeff(x0, period0, i)
    c1 = _axis_coeff(x1, period1, j)
    div_x = (
        c0[0] * vector_field[0, im, j]
        + c0[1] * vector_field[0, i, j]
        + c0[2] * vector_field[0, ip, j]
    )
    div_y = (
        c1[0] * vector_field[1, i, jm]
        + c1[1] * vector_field[1, i, j]
        + c1[2] * vector_field[1, i, jp]
    )
    output[i, j] = div_x + div_y


@wp.kernel
def _divergence_3d_kernel(
    vector_field: wp.array4d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    x1: wp.array(dtype=wp.float32),
    x2: wp.array(dtype=wp.float32),
    period0: float,
    period1: float,
    period2: float,
    output: wp.array3d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = output.shape[0]
    n1 = output.shape[1]
    n2 = output.shape[2]
    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0
    jm = (j + n1 - 1) % n1
    jp = (j + 1) % n1
    km = (k + n2 - 1) % n2
    kp = (k + 1) % n2
    c0 = _axis_coeff(x0, period0, i)
    c1 = _axis_coeff(x1, period1, j)
    c2 = _axis_coeff(x2, period2, k)
    div_x = (
        c0[0] * vector_field[0, im, j, k]
        + c0[1] * vector_field[0, i, j, k]
        + c0[2] * vector_field[0, ip, j, k]
    )
    div_y = (
        c1[0] * vector_field[1, i, jm, k]
        + c1[1] * vector_field[1, i, j, k]
        + c1[2] * vector_field[1, i, jp, k]
    )
    div_z = (
        c2[0] * vector_field[2, i, j, km]
        + c2[1] * vector_field[2, i, j, k]
        + c2[2] * vector_field[2, i, j, kp]
    )
    output[i, j, k] = div_x + div_y + div_z


@wp.kernel
def _divergence_backward_1d_kernel(
    grad_output: wp.array(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    period0: float,
    grad_vector: wp.array2d(dtype=wp.float32),
):
    i = wp.tid()
    n0 = grad_output.shape[0]
    im = (i + n0 - 1) % n0
    ip = (i + 1) % n0
    ci = _axis_coeff(x0, period0, i)
    cip = _axis_coeff(x0, period0, ip)
    cim = _axis_coeff(x0, period0, im)
    grad_vector[0, i] = (
        ci[1] * grad_output[i] + cip[0] * grad_output[ip] + cim[2] * grad_output[im]
    )


@wp.kernel
def _divergence_backward_2d_kernel(
    grad_output: wp.array2d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    x1: wp.array(dtype=wp.float32),
    period0: float,
    period1: float,
    grad_vector: wp.array3d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = grad_output.shape[0]
    n1 = grad_output.shape[1]
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
    grad_vector[0, i, j] = (
        cxi[1] * grad_output[i, j]
        + cxip[0] * grad_output[ip, j]
        + cxim[2] * grad_output[im, j]
    )
    grad_vector[1, i, j] = (
        cyi[1] * grad_output[i, j]
        + cyip[0] * grad_output[i, jp]
        + cyim[2] * grad_output[i, jm]
    )


@wp.kernel
def _divergence_backward_3d_kernel(
    grad_output: wp.array3d(dtype=wp.float32),
    x0: wp.array(dtype=wp.float32),
    x1: wp.array(dtype=wp.float32),
    x2: wp.array(dtype=wp.float32),
    period0: float,
    period1: float,
    period2: float,
    grad_vector: wp.array4d(dtype=wp.float32),
):
    i, j, k = wp.tid()
    n0 = grad_output.shape[0]
    n1 = grad_output.shape[1]
    n2 = grad_output.shape[2]
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
    grad_vector[0, i, j, k] = (
        cxi[1] * grad_output[i, j, k]
        + cxip[0] * grad_output[ip, j, k]
        + cxim[2] * grad_output[im, j, k]
    )
    grad_vector[1, i, j, k] = (
        cyi[1] * grad_output[i, j, k]
        + cyip[0] * grad_output[i, jp, k]
        + cyim[2] * grad_output[i, jm, k]
    )
    grad_vector[2, i, j, k] = (
        czi[1] * grad_output[i, j, k]
        + czip[0] * grad_output[i, j, kp]
        + czim[2] * grad_output[i, j, km]
    )


_FORWARD_KERNELS = {
    1: _divergence_1d_kernel,
    2: _divergence_2d_kernel,
    3: _divergence_3d_kernel,
}
_BACKWARD_KERNELS = {
    1: _divergence_backward_1d_kernel,
    2: _divergence_backward_2d_kernel,
    3: _divergence_backward_3d_kernel,
}


def _launch_forward(
    vector_field_fp32: torch.Tensor,
    coords_tuple: tuple[torch.Tensor, ...],
    period_tuple: tuple[float, ...],
    output_fp32: torch.Tensor,
) -> None:
    ndim = output_fp32.ndim
    wp_device, wp_stream = FunctionSpec.warp_launch_context(vector_field_fp32)
    inputs = [
        wp.from_torch(vector_field_fp32, dtype=wp.float32),
        *[wp.from_torch(coords_tuple[i], dtype=wp.float32) for i in range(ndim)],
        *[float(period_tuple[i]) for i in range(ndim)],
        wp.from_torch(output_fp32, dtype=wp.float32),
    ]
    with FunctionSpec.warp_stream_scope(wp_stream):
        wp.launch(
            kernel=_FORWARD_KERNELS[ndim],
            dim=_launch_dim(output_fp32.shape),
            inputs=inputs,
            device=wp_device,
            stream=wp_stream,
        )


def _launch_backward(
    grad_output_fp32: torch.Tensor,
    coords_tuple: tuple[torch.Tensor, ...],
    period_tuple: tuple[float, ...],
    grad_vector_fp32: torch.Tensor,
) -> None:
    ndim = grad_output_fp32.ndim
    wp_device, wp_stream = FunctionSpec.warp_launch_context(grad_output_fp32)
    inputs = [
        wp.from_torch(grad_output_fp32, dtype=wp.float32),
        *[wp.from_torch(coords_tuple[i], dtype=wp.float32) for i in range(ndim)],
        *[float(period_tuple[i]) for i in range(ndim)],
        wp.from_torch(grad_vector_fp32, dtype=wp.float32),
    ]
    with FunctionSpec.warp_stream_scope(wp_stream):
        wp.launch(
            kernel=_BACKWARD_KERNELS[ndim],
            dim=_launch_dim(grad_output_fp32.shape),
            inputs=inputs,
            device=wp_device,
            stream=wp_stream,
        )


def _forward_common(
    vector_field: torch.Tensor,
    coords_tuple: tuple[torch.Tensor, ...],
    period_tuple: tuple[float, ...],
) -> torch.Tensor:
    grid_ndim = validate_vector_field(vector_field)
    orig_dtype = vector_field.dtype
    vector_field_fp32 = _to_fp32_contiguous(vector_field)
    output_fp32 = torch.empty(
        vector_field_fp32.shape[1:],
        device=vector_field_fp32.device,
        dtype=torch.float32,
    )
    _launch_forward(
        vector_field_fp32,
        coords_tuple[:grid_ndim],
        period_tuple[:grid_ndim],
        output_fp32,
    )
    return _restore_dtype(output_fp32, orig_dtype)


def _setup_context_common(
    ctx: torch.autograd.function.FunctionCtx,
    vector_field: torch.Tensor,
    coords_tuple: tuple[torch.Tensor, ...],
    period_tuple: tuple[float, ...],
) -> None:
    ctx.save_for_backward(*coords_tuple)
    ctx.period_tuple = period_tuple
    ctx.orig_dtype = vector_field.dtype


def _backward_common(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> torch.Tensor | None:
    if grad_output is None or not ctx.needs_input_grad[0]:
        return None
    coords_tuple = tuple(ctx.saved_tensors)
    period_tuple = tuple(float(v) for v in ctx.period_tuple)
    grad_output_fp32 = _to_fp32_contiguous(grad_output)
    grad_vector_fp32 = torch.empty(
        (grad_output_fp32.ndim, *grad_output_fp32.shape),
        device=grad_output_fp32.device,
        dtype=torch.float32,
    )
    _launch_backward(
        grad_output_fp32,
        coords_tuple,
        period_tuple,
        grad_vector_fp32,
    )
    return _restore_dtype(grad_vector_fp32, ctx.orig_dtype)


@torch.library.custom_op(
    "physicsnemo::rectilinear_grid_divergence_1d_warp_impl", mutates_args=()
)
def rectilinear_grid_divergence_1d_impl(
    vector_field: torch.Tensor,
    coord0: torch.Tensor,
    period0: float,
) -> torch.Tensor:
    """Compute 1D rectilinear-grid divergence with a fused Warp custom op."""
    return _forward_common(vector_field, (coord0,), (float(period0),))


@rectilinear_grid_divergence_1d_impl.register_fake
def _rectilinear_grid_divergence_1d_impl_fake(
    vector_field: torch.Tensor,
    coord0: torch.Tensor,
    period0: float,
) -> torch.Tensor:
    _ = (coord0, period0)
    return torch.empty(
        vector_field.shape[1:],
        device=vector_field.device,
        dtype=vector_field.dtype,
    )


def setup_rectilinear_grid_divergence_1d_context(
    ctx: torch.autograd.function.FunctionCtx,
    inputs: tuple,
    output: torch.Tensor,
) -> None:
    """Save 1D divergence tensors required by the custom backward pass."""
    vector_field, coord0, period0 = inputs
    _ = output
    _setup_context_common(ctx, vector_field, (coord0,), (float(period0),))


def backward_rectilinear_grid_divergence_1d(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[torch.Tensor | None, None, None]:
    return _backward_common(ctx, grad_output), None, None


rectilinear_grid_divergence_1d_impl.register_autograd(
    backward_rectilinear_grid_divergence_1d,
    setup_context=setup_rectilinear_grid_divergence_1d_context,
)


@torch.library.custom_op(
    "physicsnemo::rectilinear_grid_divergence_2d_warp_impl", mutates_args=()
)
def rectilinear_grid_divergence_2d_impl(
    vector_field: torch.Tensor,
    coord0: torch.Tensor,
    coord1: torch.Tensor,
    period0: float,
    period1: float,
) -> torch.Tensor:
    """Compute 2D rectilinear-grid divergence with a fused Warp custom op."""
    return _forward_common(
        vector_field,
        (coord0, coord1),
        (float(period0), float(period1)),
    )


@rectilinear_grid_divergence_2d_impl.register_fake
def _rectilinear_grid_divergence_2d_impl_fake(
    vector_field: torch.Tensor,
    coord0: torch.Tensor,
    coord1: torch.Tensor,
    period0: float,
    period1: float,
) -> torch.Tensor:
    _ = (coord0, coord1, period0, period1)
    return torch.empty(
        vector_field.shape[1:],
        device=vector_field.device,
        dtype=vector_field.dtype,
    )


def setup_rectilinear_grid_divergence_2d_context(
    ctx: torch.autograd.function.FunctionCtx,
    inputs: tuple,
    output: torch.Tensor,
) -> None:
    """Save 2D divergence tensors required by the custom backward pass."""
    vector_field, coord0, coord1, period0, period1 = inputs
    _ = output
    _setup_context_common(
        ctx,
        vector_field,
        (coord0, coord1),
        (float(period0), float(period1)),
    )


def backward_rectilinear_grid_divergence_2d(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[torch.Tensor | None, None, None, None, None]:
    return _backward_common(ctx, grad_output), None, None, None, None


rectilinear_grid_divergence_2d_impl.register_autograd(
    backward_rectilinear_grid_divergence_2d,
    setup_context=setup_rectilinear_grid_divergence_2d_context,
)


@torch.library.custom_op(
    "physicsnemo::rectilinear_grid_divergence_3d_warp_impl", mutates_args=()
)
def rectilinear_grid_divergence_3d_impl(
    vector_field: torch.Tensor,
    coord0: torch.Tensor,
    coord1: torch.Tensor,
    coord2: torch.Tensor,
    period0: float,
    period1: float,
    period2: float,
) -> torch.Tensor:
    """Compute 3D rectilinear-grid divergence with a fused Warp custom op."""
    return _forward_common(
        vector_field,
        (coord0, coord1, coord2),
        (float(period0), float(period1), float(period2)),
    )


@rectilinear_grid_divergence_3d_impl.register_fake
def _rectilinear_grid_divergence_3d_impl_fake(
    vector_field: torch.Tensor,
    coord0: torch.Tensor,
    coord1: torch.Tensor,
    coord2: torch.Tensor,
    period0: float,
    period1: float,
    period2: float,
) -> torch.Tensor:
    _ = (coord0, coord1, coord2, period0, period1, period2)
    return torch.empty(
        vector_field.shape[1:],
        device=vector_field.device,
        dtype=vector_field.dtype,
    )


def setup_rectilinear_grid_divergence_3d_context(
    ctx: torch.autograd.function.FunctionCtx,
    inputs: tuple,
    output: torch.Tensor,
) -> None:
    """Save 3D divergence tensors required by the custom backward pass."""
    vector_field, coord0, coord1, coord2, period0, period1, period2 = inputs
    _ = output
    _setup_context_common(
        ctx,
        vector_field,
        (coord0, coord1, coord2),
        (float(period0), float(period1), float(period2)),
    )


def backward_rectilinear_grid_divergence_3d(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[torch.Tensor | None, None, None, None, None, None, None]:
    return _backward_common(ctx, grad_output), None, None, None, None, None, None


rectilinear_grid_divergence_3d_impl.register_autograd(
    backward_rectilinear_grid_divergence_3d,
    setup_context=setup_rectilinear_grid_divergence_3d_context,
)


def rectilinear_grid_divergence_warp(
    vector_field: torch.Tensor,
    coordinates: Sequence[torch.Tensor],
    periods: float | Sequence[float] | None = None,
) -> torch.Tensor:
    """Compute periodic rectilinear-grid divergence with a fused Warp custom op."""
    grid_ndim = validate_vector_field(vector_field)
    coords_tuple, period_tuple = _normalize_geometry(
        vector_field,
        coordinates,
        periods,
    )
    if grid_ndim == 1:
        return rectilinear_grid_divergence_1d_impl(
            vector_field,
            coords_tuple[0],
            period_tuple[0],
        )
    if grid_ndim == 2:
        return rectilinear_grid_divergence_2d_impl(
            vector_field,
            coords_tuple[0],
            coords_tuple[1],
            period_tuple[0],
            period_tuple[1],
        )
    return rectilinear_grid_divergence_3d_impl(
        vector_field,
        coords_tuple[0],
        coords_tuple[1],
        coords_tuple[2],
        period_tuple[0],
        period_tuple[1],
        period_tuple[2],
    )
