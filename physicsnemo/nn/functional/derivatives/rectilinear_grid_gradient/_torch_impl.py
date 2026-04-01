# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Sequence

import torch


def _normalize_periods(
    periods: float | Sequence[float] | None,
    coordinates: tuple[torch.Tensor, ...],
) -> tuple[float, ...]:
    ### Normalize explicit/inferred periodic lengths to one value per axis.
    ndim = len(coordinates)
    if periods is None:
        inferred: list[float] = []
        for coords in coordinates:
            if coords.numel() < 2:
                raise ValueError("rectilinear_grid_gradient requires at least two coordinates per axis")
            inferred.append(float((coords[-1] - coords[0] + (coords[1] - coords[0])).item()))
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
            raise ValueError("coordinate gradients are not supported; pass detached coordinates")
        if not torch.is_floating_point(coords):
            raise TypeError(f"coordinates[{axis}] must be floating-point")
        if coords.device != field.device:
            raise ValueError("field and coordinates must be on the same device")
        if coords.numel() < 3:
            raise ValueError("each coordinate axis must contain at least 3 points for central differencing")

        coords_fp = coords.to(dtype=field.dtype).contiguous()
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


def _axis_central_weights(
    coords: torch.Tensor,
    period: float,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    ### Build periodic nonuniform second-order central-difference weights.
    diffs = coords[1:] - coords[:-1]

    h_plus = torch.empty_like(coords)
    h_plus[:-1] = diffs
    h_plus[-1] = period - (coords[-1] - coords[0])

    h_minus = torch.empty_like(coords)
    h_minus[1:] = diffs
    h_minus[0] = h_plus[-1]

    if torch.any(h_minus <= 0.0) or torch.any(h_plus <= 0.0):
        raise ValueError("rectilinear coordinates/period produce non-positive periodic spacing")

    denom = h_minus + h_plus
    w_minus = -h_plus / (h_minus * denom)
    w_center = (h_plus - h_minus) / (h_minus * h_plus)
    w_plus = h_minus / (h_plus * denom)
    return w_minus, w_center, w_plus


def rectilinear_grid_gradient_torch(
    field: torch.Tensor,
    coordinates: Sequence[torch.Tensor],
    periods: float | Sequence[float] | None = None,
) -> torch.Tensor:
    ### Validate field and coordinate inputs.
    if field.ndim < 1 or field.ndim > 3:
        raise ValueError(f"rectilinear_grid_gradient supports 1D-3D fields, got {field.shape=}")
    if not torch.is_floating_point(field):
        raise TypeError("field must be a floating-point tensor")

    coords_tuple, period_tuple = _validate_and_normalize_coordinates(
        field=field,
        coordinates=coordinates,
        periods=periods,
    )

    ### Compute per-axis nonuniform periodic central-difference derivatives.
    gradients: list[torch.Tensor] = []
    for axis in range(field.ndim):
        w_minus, w_center, w_plus = _axis_central_weights(
            coords_tuple[axis],
            period_tuple[axis],
        )

        view_shape = [1] * field.ndim
        view_shape[axis] = field.shape[axis]
        w_minus = w_minus.view(view_shape)
        w_center = w_center.view(view_shape)
        w_plus = w_plus.view(view_shape)

        grad_axis = (
            w_minus * torch.roll(field, shifts=1, dims=axis)
            + w_center * field
            + w_plus * torch.roll(field, shifts=-1, dims=axis)
        )
        gradients.append(grad_axis)

    ### Stack axis-wise derivatives into (dims, *field.shape).
    return torch.stack(gradients, dim=0)
