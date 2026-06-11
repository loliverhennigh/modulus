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

from .utils import validate_vector_field

try:
    import warp as wp

    from ..uniform_grid_gradient._warp_impl.utils import (
        _launch_dim,
        _normalize_spacing,
        _to_wp_tensor,
        _warp_launch_context,
        _wp_launch,
    )

    _HAS_WARP = True
except ImportError as import_error:
    _HAS_WARP = False
    _WARP_IMPORT_ERROR = import_error


_SUPPORTED_ORDERS = (2, 4)


def _validate_order(order: int) -> int:
    if not isinstance(order, int):
        raise TypeError(f"order must be an integer, got {type(order)}")
    if order not in _SUPPORTED_ORDERS:
        raise ValueError(
            "uniform_grid_curl supports central orders "
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


if _HAS_WARP:

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

    @wp.kernel
    def _curl_2d_order2_kernel(
        vector_field: wp.array3d(dtype=wp.float32),
        inv_dx0: float,
        inv_dx1: float,
        output: wp.array2d(dtype=wp.float32),
    ):
        i, j = wp.tid()
        n0 = output.shape[0]
        n1 = output.shape[1]
        im = _wrap_minus1(i, n0)
        ip = _wrap_plus1(i, n0)
        jm = _wrap_minus1(j, n1)
        jp = _wrap_plus1(j, n1)
        d_v1_dx = (vector_field[1, ip, j] - vector_field[1, im, j]) * (0.5 * inv_dx0)
        d_v0_dy = (vector_field[0, i, jp] - vector_field[0, i, jm]) * (0.5 * inv_dx1)
        output[i, j] = d_v1_dx - d_v0_dy

    @wp.kernel
    def _curl_2d_order4_kernel(
        vector_field: wp.array3d(dtype=wp.float32),
        inv_dx0: float,
        inv_dx1: float,
        output: wp.array2d(dtype=wp.float32),
    ):
        i, j = wp.tid()
        n0 = output.shape[0]
        n1 = output.shape[1]
        im1 = _wrap_minus1(i, n0)
        ip1 = _wrap_plus1(i, n0)
        im2 = _wrap_minus2(i, n0)
        ip2 = _wrap_plus2(i, n0)
        jm1 = _wrap_minus1(j, n1)
        jp1 = _wrap_plus1(j, n1)
        jm2 = _wrap_minus2(j, n1)
        jp2 = _wrap_plus2(j, n1)
        d_v1_dx = (
            -vector_field[1, ip2, j]
            + 8.0 * vector_field[1, ip1, j]
            - 8.0 * vector_field[1, im1, j]
            + vector_field[1, im2, j]
        ) * (inv_dx0 / 12.0)
        d_v0_dy = (
            -vector_field[0, i, jp2]
            + 8.0 * vector_field[0, i, jp1]
            - 8.0 * vector_field[0, i, jm1]
            + vector_field[0, i, jm2]
        ) * (inv_dx1 / 12.0)
        output[i, j] = d_v1_dx - d_v0_dy

    @wp.kernel
    def _curl_3d_order2_kernel(
        vector_field: wp.array4d(dtype=wp.float32),
        inv_dx0: float,
        inv_dx1: float,
        inv_dx2: float,
        output: wp.array4d(dtype=wp.float32),
    ):
        i, j, k = wp.tid()
        n0 = output.shape[1]
        n1 = output.shape[2]
        n2 = output.shape[3]
        im = _wrap_minus1(i, n0)
        ip = _wrap_plus1(i, n0)
        jm = _wrap_minus1(j, n1)
        jp = _wrap_plus1(j, n1)
        km = _wrap_minus1(k, n2)
        kp = _wrap_plus1(k, n2)
        output[0, i, j, k] = (vector_field[2, i, jp, k] - vector_field[2, i, jm, k]) * (
            0.5 * inv_dx1
        ) - (vector_field[1, i, j, kp] - vector_field[1, i, j, km]) * (0.5 * inv_dx2)
        output[1, i, j, k] = (vector_field[0, i, j, kp] - vector_field[0, i, j, km]) * (
            0.5 * inv_dx2
        ) - (vector_field[2, ip, j, k] - vector_field[2, im, j, k]) * (0.5 * inv_dx0)
        output[2, i, j, k] = (vector_field[1, ip, j, k] - vector_field[1, im, j, k]) * (
            0.5 * inv_dx0
        ) - (vector_field[0, i, jp, k] - vector_field[0, i, jm, k]) * (0.5 * inv_dx1)

    @wp.kernel
    def _curl_3d_order4_kernel(
        vector_field: wp.array4d(dtype=wp.float32),
        inv_dx0: float,
        inv_dx1: float,
        inv_dx2: float,
        output: wp.array4d(dtype=wp.float32),
    ):
        i, j, k = wp.tid()
        n0 = output.shape[1]
        n1 = output.shape[2]
        n2 = output.shape[3]
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
        d_v2_dy = (
            -vector_field[2, i, jp2, k]
            + 8.0 * vector_field[2, i, jp1, k]
            - 8.0 * vector_field[2, i, jm1, k]
            + vector_field[2, i, jm2, k]
        ) * (inv_dx1 / 12.0)
        d_v1_dz = (
            -vector_field[1, i, j, kp2]
            + 8.0 * vector_field[1, i, j, kp1]
            - 8.0 * vector_field[1, i, j, km1]
            + vector_field[1, i, j, km2]
        ) * (inv_dx2 / 12.0)
        d_v0_dz = (
            -vector_field[0, i, j, kp2]
            + 8.0 * vector_field[0, i, j, kp1]
            - 8.0 * vector_field[0, i, j, km1]
            + vector_field[0, i, j, km2]
        ) * (inv_dx2 / 12.0)
        d_v2_dx = (
            -vector_field[2, ip2, j, k]
            + 8.0 * vector_field[2, ip1, j, k]
            - 8.0 * vector_field[2, im1, j, k]
            + vector_field[2, im2, j, k]
        ) * (inv_dx0 / 12.0)
        d_v1_dx = (
            -vector_field[1, ip2, j, k]
            + 8.0 * vector_field[1, ip1, j, k]
            - 8.0 * vector_field[1, im1, j, k]
            + vector_field[1, im2, j, k]
        ) * (inv_dx0 / 12.0)
        d_v0_dy = (
            -vector_field[0, i, jp2, k]
            + 8.0 * vector_field[0, i, jp1, k]
            - 8.0 * vector_field[0, i, jm1, k]
            + vector_field[0, i, jm2, k]
        ) * (inv_dx1 / 12.0)
        output[0, i, j, k] = d_v2_dy - d_v1_dz
        output[1, i, j, k] = d_v0_dz - d_v2_dx
        output[2, i, j, k] = d_v1_dx - d_v0_dy

    @wp.kernel
    def _curl_backward_2d_order2_kernel(
        grad_output: wp.array2d(dtype=wp.float32),
        inv_dx0: float,
        inv_dx1: float,
        grad_vector: wp.array3d(dtype=wp.float32),
    ):
        i, j = wp.tid()
        n0 = grad_output.shape[0]
        n1 = grad_output.shape[1]
        im = _wrap_minus1(i, n0)
        ip = _wrap_plus1(i, n0)
        jm = _wrap_minus1(j, n1)
        jp = _wrap_plus1(j, n1)
        grad_vector[0, i, j] = (grad_output[i, jp] - grad_output[i, jm]) * (
            0.5 * inv_dx1
        )
        grad_vector[1, i, j] = (grad_output[im, j] - grad_output[ip, j]) * (
            0.5 * inv_dx0
        )

    @wp.kernel
    def _curl_backward_2d_order4_kernel(
        grad_output: wp.array2d(dtype=wp.float32),
        inv_dx0: float,
        inv_dx1: float,
        grad_vector: wp.array3d(dtype=wp.float32),
    ):
        i, j = wp.tid()
        n0 = grad_output.shape[0]
        n1 = grad_output.shape[1]
        im1 = _wrap_minus1(i, n0)
        ip1 = _wrap_plus1(i, n0)
        im2 = _wrap_minus2(i, n0)
        ip2 = _wrap_plus2(i, n0)
        jm1 = _wrap_minus1(j, n1)
        jp1 = _wrap_plus1(j, n1)
        jm2 = _wrap_minus2(j, n1)
        jp2 = _wrap_plus2(j, n1)
        grad_vector[0, i, j] = (
            -grad_output[i, jp2]
            + 8.0 * grad_output[i, jp1]
            - 8.0 * grad_output[i, jm1]
            + grad_output[i, jm2]
        ) * (inv_dx1 / 12.0)
        grad_vector[1, i, j] = (
            grad_output[ip2, j]
            - 8.0 * grad_output[ip1, j]
            + 8.0 * grad_output[im1, j]
            - grad_output[im2, j]
        ) * (inv_dx0 / 12.0)

    _FORWARD_KERNELS = {
        (2, 2): _curl_2d_order2_kernel,
        (2, 4): _curl_2d_order4_kernel,
        (3, 2): _curl_3d_order2_kernel,
        (3, 4): _curl_3d_order4_kernel,
    }
    _BACKWARD_2D_KERNELS = {
        2: _curl_backward_2d_order2_kernel,
        4: _curl_backward_2d_order4_kernel,
    }

    def _launch_curl_forward(
        *,
        vector_field_fp32: torch.Tensor,
        spacing_tuple: tuple[float, ...],
        order: int,
        output_fp32: torch.Tensor,
    ) -> None:
        wp_device, wp_stream = _warp_launch_context(vector_field_fp32)
        launch_shape = (
            output_fp32.shape if output_fp32.ndim == 2 else output_fp32.shape[1:]
        )
        _wp_launch(
            kernel=_FORWARD_KERNELS[(vector_field_fp32.ndim - 1, order)],
            dim=_launch_dim(launch_shape),
            inputs=[
                _to_wp_tensor(vector_field_fp32),
                *[1.0 / float(dx) for dx in spacing_tuple],
                _to_wp_tensor(output_fp32),
            ],
            device=wp_device,
            stream=wp_stream,
        )

    def _launch_curl_backward_2d(
        *,
        grad_output_fp32: torch.Tensor,
        spacing_tuple: tuple[float, ...],
        order: int,
        grad_vector_fp32: torch.Tensor,
    ) -> None:
        wp_device, wp_stream = _warp_launch_context(grad_output_fp32)
        _wp_launch(
            kernel=_BACKWARD_2D_KERNELS[order],
            dim=_launch_dim(grad_output_fp32.shape),
            inputs=[
                _to_wp_tensor(grad_output_fp32),
                *[1.0 / float(dx) for dx in spacing_tuple],
                _to_wp_tensor(grad_vector_fp32),
            ],
            device=wp_device,
            stream=wp_stream,
        )

    @torch.library.custom_op(
        "physicsnemo::uniform_grid_curl_warp_impl", mutates_args=()
    )
    def uniform_grid_curl_impl(
        vector_field: torch.Tensor,
        spacing_meta: torch.Tensor,
        order: int,
    ) -> torch.Tensor:
        grid_ndim = validate_vector_field(vector_field)
        spacing_tuple = tuple(float(v) for v in spacing_meta.tolist())
        _validate_positive_spacing(spacing_tuple)
        order = _validate_order(int(order))
        orig_dtype = vector_field.dtype
        vector_field_fp32 = _to_fp32_contiguous(vector_field)
        output_shape = (
            vector_field_fp32.shape[1:] if grid_ndim == 2 else vector_field_fp32.shape
        )
        output_fp32 = torch.empty(
            output_shape,
            device=vector_field_fp32.device,
            dtype=torch.float32,
        )
        _launch_curl_forward(
            vector_field_fp32=vector_field_fp32,
            spacing_tuple=spacing_tuple[:grid_ndim],
            order=order,
            output_fp32=output_fp32,
        )
        return _restore_dtype(output_fp32, orig_dtype)

    @uniform_grid_curl_impl.register_fake
    def _uniform_grid_curl_impl_fake(
        vector_field: torch.Tensor,
        spacing_meta: torch.Tensor,
        order: int,
    ) -> torch.Tensor:
        _ = (spacing_meta, order)
        output_shape = (
            vector_field.shape[1:] if vector_field.ndim == 3 else vector_field.shape
        )
        return torch.empty(
            output_shape,
            device=vector_field.device,
            dtype=vector_field.dtype,
        )

    def setup_uniform_grid_curl_context(
        ctx: torch.autograd.function.FunctionCtx,
        inputs: tuple,
        output: torch.Tensor,
    ) -> None:
        vector_field, spacing_meta, order = inputs
        _ = output
        ctx.spacing_tuple = tuple(float(v) for v in spacing_meta.tolist())
        ctx.order = int(order)
        ctx.orig_dtype = vector_field.dtype
        ctx.grid_ndim = vector_field.ndim - 1

    def backward_uniform_grid_curl(
        ctx: torch.autograd.function.FunctionCtx,
        grad_output: torch.Tensor,
    ) -> tuple[torch.Tensor | None, None, None]:
        if grad_output is None or not ctx.needs_input_grad[0]:
            return None, None, None
        grad_output_fp32 = _to_fp32_contiguous(grad_output)
        if ctx.grid_ndim == 2:
            grad_vector_fp32 = torch.empty(
                (2, *grad_output_fp32.shape),
                device=grad_output_fp32.device,
                dtype=torch.float32,
            )
            _launch_curl_backward_2d(
                grad_output_fp32=grad_output_fp32,
                spacing_tuple=ctx.spacing_tuple[:2],
                order=ctx.order,
                grad_vector_fp32=grad_vector_fp32,
            )
        else:
            grad_vector_fp32 = torch.empty_like(grad_output_fp32)
            _launch_curl_forward(
                vector_field_fp32=grad_output_fp32,
                spacing_tuple=ctx.spacing_tuple[:3],
                order=ctx.order,
                output_fp32=grad_vector_fp32,
            )
        return _restore_dtype(grad_vector_fp32, ctx.orig_dtype), None, None

    uniform_grid_curl_impl.register_autograd(
        backward_uniform_grid_curl,
        setup_context=setup_uniform_grid_curl_context,
    )


def uniform_grid_curl_warp(
    vector_field: torch.Tensor,
    spacing: float | Sequence[float] = 1.0,
    order: int = 2,
) -> torch.Tensor:
    """Compute periodic uniform-grid curl with a fused Warp custom op."""
    if not _HAS_WARP:
        raise ImportError(
            "uniform_grid_curl requires the Warp backend; install warp>=0.6.0"
        ) from _WARP_IMPORT_ERROR
    grid_ndim = validate_vector_field(vector_field)
    spacing_tuple = _normalize_spacing(spacing, grid_ndim)
    _validate_positive_spacing(spacing_tuple)
    order = _validate_order(order)
    spacing_meta = torch.tensor(spacing_tuple, dtype=torch.float32, device="cpu")
    return uniform_grid_curl_impl(vector_field, spacing_meta, int(order))
