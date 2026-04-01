# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Sequence

import torch

from physicsnemo.core.function_spec import FunctionSpec

_WARP_AVAILABLE = True
_WARP_IMPORT_ERROR: Exception | None = None

### Optional Warp dependency detection for backend availability.
try:  # pragma: no cover - optional dependency
    import warp as wp
except Exception as exc:  # pragma: no cover - optional dependency
    _WARP_AVAILABLE = False
    _WARP_IMPORT_ERROR = exc

if _WARP_AVAILABLE:
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

        gx = (
            cxi[1] * grad0[i, j, k]
            + cxip[0] * grad0[ip, j, k]
            + cxim[2] * grad0[im, j, k]
        )
        gy = (
            cyi[1] * grad1[i, j, k]
            + cyip[0] * grad1[i, jp, k]
            + cyim[2] * grad1[i, jm, k]
        )
        gz = (
            czi[1] * grad2[i, j, k]
            + czip[0] * grad2[i, j, kp]
            + czim[2] * grad2[i, j, km]
        )
        grad_field[i, j, k] = gx + gy + gz


def _normalize_periods(
    periods: float | Sequence[float] | None,
    coordinates: tuple[torch.Tensor, ...],
) -> tuple[float, ...]:
    ### Normalize explicit/inferred periodic lengths to one value per axis.
    ndim = len(coordinates)
    if periods is None:
        inferred = [
            float((coords[-1] - coords[0] + (coords[1] - coords[0])).item())
            for coords in coordinates
        ]
        return tuple(inferred)
    if isinstance(periods, (float, int)):
        return tuple(float(periods) for _ in range(ndim))
    periods_tuple = tuple(float(v) for v in periods)
    if len(periods_tuple) != ndim:
        raise ValueError(
            f"periods must have {ndim} entries for a {ndim}D field, got {len(periods_tuple)}"
        )
    return periods_tuple


def _validate_and_normalize_coordinates(
    field: torch.Tensor,
    coordinates: Sequence[torch.Tensor],
    periods: float | Sequence[float] | None,
) -> tuple[tuple[torch.Tensor, ...], tuple[float, ...]]:
    ### Validate rectilinear coordinates and periodic lengths.
    if len(coordinates) != field.ndim:
        raise ValueError(
            f"coordinates must contain one axis tensor per field dimension ({field.ndim}), "
            f"got {len(coordinates)}"
        )

    normalized_coords: list[torch.Tensor] = []
    for axis, coords in enumerate(coordinates):
        if not isinstance(coords, torch.Tensor):
            raise TypeError(f"coordinates[{axis}] must be a tensor")
        if coords.ndim != 1:
            raise ValueError(f"coordinates[{axis}] must be rank-1, got shape={tuple(coords.shape)}")
        if coords.shape[0] != field.shape[axis]:
            raise ValueError(
                f"coordinates[{axis}] length must equal field.shape[{axis}] "
                f"({field.shape[axis]}), got {coords.shape[0]}"
            )
        if coords.requires_grad:
            raise ValueError("coordinate gradients are not supported in warp backend")
        if not torch.is_floating_point(coords):
            raise TypeError(f"coordinates[{axis}] must be floating-point")
        if coords.device != field.device:
            raise ValueError("field and coordinates must be on the same device")
        if coords.numel() < 3:
            raise ValueError("each coordinate axis must contain at least 3 points for central differencing")

        coords_fp = coords.to(dtype=torch.float32).contiguous()
        diffs = coords_fp[1:] - coords_fp[:-1]
        if torch.any(diffs <= 0):
            raise ValueError(f"coordinates[{axis}] must be strictly increasing")
        normalized_coords.append(coords_fp)

    period_tuple = _normalize_periods(periods=periods, coordinates=tuple(normalized_coords))
    for axis, period in enumerate(period_tuple):
        if period <= 0.0:
            raise ValueError("all periodic lengths must be strictly positive")
        min_period = float((normalized_coords[axis][-1] - normalized_coords[axis][0]).item())
        if period <= min_period:
            raise ValueError(
                f"periods[{axis}] must be larger than coordinate span ({min_period}), got {period}"
            )
    return tuple(normalized_coords), period_tuple


def _launch_forward(
    *,
    field_fp32: torch.Tensor,
    coords_tuple: tuple[torch.Tensor, ...],
    period_tuple: tuple[float, ...],
    grad_components: list[torch.Tensor],
    wp_device,
    wp_stream,
) -> None:
    ### Launch dimensionality-specific forward kernels.
    with wp.ScopedStream(wp_stream):
        if field_fp32.ndim == 1:
            wp.launch(
                kernel=_rectilinear_gradient_1d_kernel,
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
                kernel=_rectilinear_gradient_2d_kernel,
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
            kernel=_rectilinear_gradient_3d_kernel,
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
    grad_field: torch.Tensor,
    wp_device,
    wp_stream,
) -> None:
    ### Launch dimensionality-specific backward kernels.
    with wp.ScopedStream(wp_stream):
        if grad_output_fp32.ndim == 2:
            wp.launch(
                kernel=_rectilinear_gradient_1d_backward_kernel,
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
                kernel=_rectilinear_gradient_2d_backward_kernel,
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
            kernel=_rectilinear_gradient_3d_backward_kernel,
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


class _RectilinearGridGradientWarpAutograd(torch.autograd.Function):
    ### Wrap warp forward/backward kernels for torch autograd interoperability.
    @staticmethod
    def forward(  # type: ignore[override]
        ctx,
        field: torch.Tensor,
        period_meta: torch.Tensor,
        *coords: torch.Tensor,
    ) -> torch.Tensor:
        period_tuple = tuple(float(v) for v in period_meta.tolist())
        coords_tuple = tuple(coord.contiguous() for coord in coords)

        orig_dtype = field.dtype
        field_fp32 = field.to(dtype=torch.float32).contiguous()
        grad_components = [torch.empty_like(field_fp32) for _ in range(field_fp32.ndim)]

        wp_device, wp_stream = FunctionSpec.warp_launch_context(field_fp32)
        _launch_forward(
            field_fp32=field_fp32,
            coords_tuple=coords_tuple,
            period_tuple=period_tuple,
            grad_components=grad_components,
            wp_device=wp_device,
            wp_stream=wp_stream,
        )

        output = torch.stack(grad_components, dim=0)
        if output.dtype != orig_dtype:
            output = output.to(dtype=orig_dtype)

        ### Save geometry metadata needed for backward adjoint evaluation.
        ctx.save_for_backward(period_meta, *coords_tuple)
        ctx.orig_dtype = orig_dtype
        return output

    @staticmethod
    def backward(ctx, grad_output: torch.Tensor):  # type: ignore[override]
        saved = ctx.saved_tensors
        period_meta = saved[0]
        coords_tuple = tuple(saved[1:])
        period_tuple = tuple(float(v) for v in period_meta.tolist())
        orig_dtype = ctx.orig_dtype

        if grad_output is None:
            return (None,) * (2 + len(coords_tuple))

        grad_output_fp32 = grad_output.to(dtype=torch.float32).contiguous()
        grad_field = torch.empty_like(grad_output_fp32[0])
        wp_device, wp_stream = FunctionSpec.warp_launch_context(grad_output_fp32)
        _launch_backward(
            grad_output_fp32=grad_output_fp32,
            coords_tuple=coords_tuple,
            period_tuple=period_tuple,
            grad_field=grad_field,
            wp_device=wp_device,
            wp_stream=wp_stream,
        )

        if grad_field.dtype != orig_dtype:
            grad_field = grad_field.to(dtype=orig_dtype)

        ### Coordinate/period gradients are intentionally not supported.
        return (grad_field, None, *([None] * len(coords_tuple)))


def rectilinear_grid_gradient_warp(
    field: torch.Tensor,
    coordinates: Sequence[torch.Tensor],
    periods: float | Sequence[float] | None = None,
) -> torch.Tensor:
    ### Ensure Warp backend is available before dispatch.
    if not _WARP_AVAILABLE:
        raise ImportError(
            "rectilinear_grid_gradient warp backend requires warp>=0.6.0"
        ) from _WARP_IMPORT_ERROR

    ### Validate field shape/dtype and normalize coordinates.
    if field.ndim < 1 or field.ndim > 3:
        raise ValueError(f"rectilinear_grid_gradient supports 1D-3D fields, got {field.shape=}")
    if not torch.is_floating_point(field):
        raise TypeError("field must be a floating-point tensor")

    coords_tuple, period_tuple = _validate_and_normalize_coordinates(
        field=field,
        coordinates=coordinates,
        periods=periods,
    )

    period_meta = torch.tensor(period_tuple, dtype=torch.float32, device="cpu")
    return _RectilinearGridGradientWarpAutograd.apply(field, period_meta, *coords_tuple)
