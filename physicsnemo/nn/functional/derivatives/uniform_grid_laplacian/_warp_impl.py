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

from ..uniform_grid_gradient._warp_impl.utils import (
    _launch_dim,
    _normalize_spacing,
    _to_wp_tensor,
    _wp_launch,
    _wrap_minus1,
    _wrap_minus2,
    _wrap_plus1,
    _wrap_plus2,
)
from .utils import validate_scalar_field

_SUPPORTED_ORDERS = (2, 4)


def _validate_order(order: int) -> int:
    if not isinstance(order, int):
        raise TypeError(f"order must be an integer, got {type(order)}")
    if order not in _SUPPORTED_ORDERS:
        raise ValueError(
            "uniform_grid_laplacian supports central orders "
            f"{list(_SUPPORTED_ORDERS)}, got order={order}"
        )
    return order


def _validate_positive_spacing(spacing_tuple: tuple[float, ...]) -> None:
    for dx in spacing_tuple:
        if dx <= 0.0:
            raise ValueError("all spacing entries must be strictly positive")


def _to_fp32_contiguous(tensor: torch.Tensor) -> torch.Tensor:
    if tensor.dtype == torch.float32 and tensor.is_contiguous():
        return tensor
    return tensor.to(dtype=torch.float32).contiguous()


def _restore_dtype(tensor: torch.Tensor, target_dtype: torch.dtype) -> torch.Tensor:
    if tensor.dtype == target_dtype:
        return tensor
    return tensor.to(dtype=target_dtype)


@wp.kernel
def _laplacian_1d_order2_kernel(
    field: wp.array(dtype=wp.float32),
    inv_dx0_sq: float,
    output: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = field.shape[0]
    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)
    output[i] = (field[ip] - 2.0 * field[i] + field[im]) * inv_dx0_sq


@wp.kernel
def _laplacian_1d_order4_kernel(
    field: wp.array(dtype=wp.float32),
    inv_dx0_sq: float,
    output: wp.array(dtype=wp.float32),
):
    i = wp.tid()
    n0 = field.shape[0]
    im1 = _wrap_minus1(i, n0)
    ip1 = _wrap_plus1(i, n0)
    im2 = _wrap_minus2(i, n0)
    ip2 = _wrap_plus2(i, n0)
    output[i] = (
        -field[ip2]
        + 16.0 * field[ip1]
        - 30.0 * field[i]
        + 16.0 * field[im1]
        - field[im2]
    ) * (inv_dx0_sq / 12.0)


@wp.kernel
def _laplacian_2d_order2_kernel(
    field: wp.array2d(dtype=wp.float32),
    inv_dx0_sq: float,
    inv_dx1_sq: float,
    output: wp.array2d(dtype=wp.float32),
):
    i, j = wp.tid()
    n0 = field.shape[0]
    n1 = field.shape[1]
    im = _wrap_minus1(i, n0)
    ip = _wrap_plus1(i, n0)
    jm = _wrap_minus1(j, n1)
    jp = _wrap_plus1(j, n1)
    d2x = (field[ip, j] - 2.0 * field[i, j] + field[im, j]) * inv_dx0_sq
    d2y = (field[i, jp] - 2.0 * field[i, j] + field[i, jm]) * inv_dx1_sq
    output[i, j] = d2x + d2y


@wp.kernel
def _laplacian_2d_order4_kernel(
    field: wp.array2d(dtype=wp.float32),
    inv_dx0_sq: float,
    inv_dx1_sq: float,
    output: wp.array2d(dtype=wp.float32),
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
    d2x = (
        -field[ip2, j]
        + 16.0 * field[ip1, j]
        - 30.0 * field[i, j]
        + 16.0 * field[im1, j]
        - field[im2, j]
    ) * (inv_dx0_sq / 12.0)
    d2y = (
        -field[i, jp2]
        + 16.0 * field[i, jp1]
        - 30.0 * field[i, j]
        + 16.0 * field[i, jm1]
        - field[i, jm2]
    ) * (inv_dx1_sq / 12.0)
    output[i, j] = d2x + d2y


@wp.kernel
def _laplacian_3d_order2_kernel(
    field: wp.array3d(dtype=wp.float32),
    inv_dx0_sq: float,
    inv_dx1_sq: float,
    inv_dx2_sq: float,
    output: wp.array3d(dtype=wp.float32),
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
    d2x = (field[ip, j, k] - 2.0 * field[i, j, k] + field[im, j, k]) * inv_dx0_sq
    d2y = (field[i, jp, k] - 2.0 * field[i, j, k] + field[i, jm, k]) * inv_dx1_sq
    d2z = (field[i, j, kp] - 2.0 * field[i, j, k] + field[i, j, km]) * inv_dx2_sq
    output[i, j, k] = d2x + d2y + d2z


@wp.kernel
def _laplacian_3d_order4_kernel(
    field: wp.array3d(dtype=wp.float32),
    inv_dx0_sq: float,
    inv_dx1_sq: float,
    inv_dx2_sq: float,
    output: wp.array3d(dtype=wp.float32),
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
    d2x = (
        -field[ip2, j, k]
        + 16.0 * field[ip1, j, k]
        - 30.0 * field[i, j, k]
        + 16.0 * field[im1, j, k]
        - field[im2, j, k]
    ) * (inv_dx0_sq / 12.0)
    d2y = (
        -field[i, jp2, k]
        + 16.0 * field[i, jp1, k]
        - 30.0 * field[i, j, k]
        + 16.0 * field[i, jm1, k]
        - field[i, jm2, k]
    ) * (inv_dx1_sq / 12.0)
    d2z = (
        -field[i, j, kp2]
        + 16.0 * field[i, j, kp1]
        - 30.0 * field[i, j, k]
        + 16.0 * field[i, j, km1]
        - field[i, j, km2]
    ) * (inv_dx2_sq / 12.0)
    output[i, j, k] = d2x + d2y + d2z


_LAPLACIAN_KERNELS = {
    (1, 2): _laplacian_1d_order2_kernel,
    (1, 4): _laplacian_1d_order4_kernel,
    (2, 2): _laplacian_2d_order2_kernel,
    (2, 4): _laplacian_2d_order4_kernel,
    (3, 2): _laplacian_3d_order2_kernel,
    (3, 4): _laplacian_3d_order4_kernel,
}


def _launch_laplacian(
    *,
    field_fp32: torch.Tensor,
    spacing_tuple: tuple[float, ...],
    order: int,
    output_fp32: torch.Tensor,
) -> None:
    inv_sq = [1.0 / float(dx * dx) for dx in spacing_tuple]
    wp_device, wp_stream = FunctionSpec.warp_launch_context(field_fp32)
    _wp_launch(
        kernel=_LAPLACIAN_KERNELS[(field_fp32.ndim, order)],
        dim=_launch_dim(field_fp32.shape),
        inputs=[
            _to_wp_tensor(field_fp32),
            *inv_sq,
            _to_wp_tensor(output_fp32),
        ],
        device=wp_device,
        stream=wp_stream,
    )


@torch.library.custom_op(
    "physicsnemo::uniform_grid_laplacian_warp_impl", mutates_args=()
)
def uniform_grid_laplacian_impl(
    field: torch.Tensor,
    spacing_meta: torch.Tensor,
    order: int,
) -> torch.Tensor:
    """Evaluate uniform-grid Laplacian with fused Warp kernels."""
    validate_scalar_field(field)
    spacing_tuple = tuple(float(v) for v in spacing_meta.tolist())
    _validate_positive_spacing(spacing_tuple)
    order = _validate_order(int(order))
    orig_dtype = field.dtype
    field_fp32 = _to_fp32_contiguous(field)
    output_fp32 = torch.empty_like(field_fp32)
    _launch_laplacian(
        field_fp32=field_fp32,
        spacing_tuple=spacing_tuple,
        order=order,
        output_fp32=output_fp32,
    )
    return _restore_dtype(output_fp32, orig_dtype)


@uniform_grid_laplacian_impl.register_fake
def _uniform_grid_laplacian_impl_fake(
    field: torch.Tensor,
    spacing_meta: torch.Tensor,
    order: int,
) -> torch.Tensor:
    _ = (spacing_meta, order)
    return torch.empty_like(field)


def setup_uniform_grid_laplacian_context(
    ctx: torch.autograd.function.FunctionCtx,
    inputs: tuple,
    output: torch.Tensor,
) -> None:
    """Save uniform-grid Laplacian metadata for the backward pass."""
    field, spacing_meta, order = inputs
    _ = output
    ctx.spacing_tuple = tuple(float(v) for v in spacing_meta.tolist())
    ctx.order = int(order)
    ctx.orig_dtype = field.dtype


def backward_uniform_grid_laplacian(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[torch.Tensor | None, None, None]:
    if grad_output is None or not ctx.needs_input_grad[0]:
        return None, None, None
    grad_output_fp32 = _to_fp32_contiguous(grad_output)
    grad_field = torch.empty_like(grad_output_fp32)
    _launch_laplacian(
        field_fp32=grad_output_fp32,
        spacing_tuple=ctx.spacing_tuple,
        order=ctx.order,
        output_fp32=grad_field,
    )
    return _restore_dtype(grad_field, ctx.orig_dtype), None, None


uniform_grid_laplacian_impl.register_autograd(
    backward_uniform_grid_laplacian,
    setup_context=setup_uniform_grid_laplacian_context,
)


def uniform_grid_laplacian_warp(
    field: torch.Tensor,
    spacing: float | Sequence[float] = 1.0,
    order: int = 2,
) -> torch.Tensor:
    """Compute periodic uniform-grid Laplacian with a fused Warp custom op."""
    spacing_tuple = _normalize_spacing(spacing, field.ndim)
    spacing_meta = torch.tensor(spacing_tuple, dtype=torch.float32, device="cpu")
    return uniform_grid_laplacian_impl(field, spacing_meta, _validate_order(order))
