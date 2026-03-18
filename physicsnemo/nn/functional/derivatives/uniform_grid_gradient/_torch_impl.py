# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Sequence

import torch

_SUPPORTED_ORDERS = (2, 4)


def _normalize_spacing(spacing: float | Sequence[float], ndim: int) -> tuple[float, ...]:
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


def _central_derivative_order2(field: torch.Tensor, axis: int, dx: float) -> torch.Tensor:
    ### Second-order periodic central difference.
    return (torch.roll(field, shifts=-1, dims=axis) - torch.roll(field, shifts=1, dims=axis)) / (
        2.0 * dx
    )


def _central_derivative_order4(field: torch.Tensor, axis: int, dx: float) -> torch.Tensor:
    ### Fourth-order periodic central difference.
    # d/dx f_i ≈ (-f_{i+2} + 8 f_{i+1} - 8 f_{i-1} + f_{i-2}) / (12 dx)
    return (
        -torch.roll(field, shifts=-2, dims=axis)
        + 8.0 * torch.roll(field, shifts=-1, dims=axis)
        - 8.0 * torch.roll(field, shifts=1, dims=axis)
        + torch.roll(field, shifts=2, dims=axis)
    ) / (12.0 * dx)


def uniform_grid_gradient_torch(
    field: torch.Tensor,
    spacing: float | Sequence[float] = 1.0,
    order: int = 2,
) -> torch.Tensor:
    ### Validate field shape and dtype.
    if field.ndim < 1 or field.ndim > 3:
        raise ValueError(f"uniform_grid_gradient supports 1D-3D fields, got {field.shape=}")
    if not torch.is_floating_point(field):
        raise TypeError("field must be a floating-point tensor")
    order = _validate_order(order)

    ### Expand spacing to one entry per field axis.
    spacing_tuple = _normalize_spacing(spacing, field.ndim)

    ### Compute periodic central differences independently per axis.
    gradients: list[torch.Tensor] = []
    for axis, dx in enumerate(spacing_tuple):
        if dx <= 0.0:
            raise ValueError("all spacing entries must be strictly positive")
        ### Periodic central difference with configurable accuracy order.
        if order == 2:
            grad_axis = _central_derivative_order2(field, axis=axis, dx=dx)
        else:
            grad_axis = _central_derivative_order4(field, axis=axis, dx=dx)
        gradients.append(grad_axis)

    ### Stack axis-wise derivatives into (dims, *field.shape).
    return torch.stack(gradients, dim=0)
