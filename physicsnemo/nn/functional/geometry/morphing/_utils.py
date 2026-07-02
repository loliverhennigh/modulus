# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared structural validation and normalization for morphing backends."""

from __future__ import annotations

import math
from numbers import Real

import torch


def _validate_points(tensor: torch.Tensor, name: str) -> None:
    """Validate an independent point-coordinate tensor."""

    if tensor.ndim not in (2, 3):
        raise ValueError(
            f"{name} must have shape (N, D) or (B, N, D), got {tuple(tensor.shape)}"
        )
    if tensor.shape[-1] < 1:
        raise ValueError(f"{name} coordinate dimension must be at least 1")
    if tensor.dtype not in (torch.float32, torch.float64):
        raise TypeError(
            f"{name} must have dtype torch.float32 or torch.float64, got {tensor.dtype}"
        )


def _validate_layout(
    tensor: torch.Tensor,
    reference: torch.Tensor,
    names: str,
    same_shape: bool = False,
) -> None:
    """Validate tensor layout constraints that must not broadcast or promote."""

    if same_shape and tensor.shape != reference.shape:
        raise ValueError(
            f"{names} must have identical shapes, got "
            f"{tuple(tensor.shape)} and {tuple(reference.shape)}"
        )
    if tensor.device != reference.device:
        raise ValueError(
            f"{names} must be on the same device, got "
            f"{tensor.device} and {reference.device}"
        )
    if tensor.dtype != reference.dtype:
        raise TypeError(
            f"{names} must have the same dtype, got "
            f"{tensor.dtype} and {reference.dtype}"
        )


def _as_batched(tensor: torch.Tensor) -> tuple[torch.Tensor, bool]:
    """Normalize ``(N, D)`` to ``(1, N, D)`` and retain rank information."""

    if tensor.ndim == 2:
        return tensor.unsqueeze(0), True
    return tensor, False


def _normalize_amount(
    amount: float | torch.Tensor,
    *,
    reference: torch.Tensor,
) -> torch.Tensor:
    """Normalize a scalar amount to the one-element backend representation."""

    if isinstance(amount, torch.Tensor):
        if amount.ndim != 0:
            raise ValueError(
                f"amount must be a Python scalar or 0-D tensor, got {tuple(amount.shape)}"
            )
        _validate_layout(amount, reference, "tensor-valued amount and points")
        return amount.reshape(1)

    if not isinstance(amount, Real) or isinstance(amount, bool):
        raise TypeError(
            "amount must be a finite Python real scalar or a floating-point 0-D tensor"
        )
    if not torch.compiler.is_compiling():
        amount_value = float(amount)
        if not math.isfinite(amount_value):
            raise ValueError("amount must be finite")
        if abs(amount_value) > torch.finfo(reference.dtype).max:
            raise ValueError("amount must be finite in the point dtype")
    return torch.as_tensor(
        amount,
        device=reference.device,
        dtype=reference.dtype,
    ).reshape(1)


def _normalize_weights(
    weights: torch.Tensor | None,
    points: torch.Tensor,
    was_unbatched: bool,
) -> torch.Tensor | None:
    """Normalize optional per-query weights to ``(B, N)`` without copying."""

    if weights is None:
        return None

    batch_size, num_points = points.shape[:2]
    expected = (num_points,) if was_unbatched else (batch_size, num_points)
    if tuple(weights.shape) != expected:
        raise ValueError(
            f"weights must have shape {expected}, got {tuple(weights.shape)}"
        )
    if weights.device != points.device:
        raise ValueError(
            "weights and points must be on the same device, got "
            f"{weights.device} and {points.device}"
        )
    if weights.dtype not in (torch.bool, points.dtype):
        raise TypeError(
            "weights must have bool dtype or the same dtype as points, got "
            f"{weights.dtype} and {points.dtype}"
        )
    return weights.unsqueeze(0) if was_unbatched else weights


def normalize_displace_inputs(
    points: torch.Tensor,
    displacement: torch.Tensor,
    amount: float | torch.Tensor,
    weights: torch.Tensor | None,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor | None,
    bool,
]:
    """Validate and normalize dense-displacement inputs for either backend."""

    _validate_points(points, "points")
    _validate_layout(displacement, points, "points and displacement", True)

    points_b3, was_unbatched = _as_batched(points)
    displacement_b3, _ = _as_batched(displacement)
    return (
        points_b3,
        displacement_b3,
        _normalize_amount(amount, reference=points_b3),
        _normalize_weights(weights, points_b3, was_unbatched),
        was_unbatched,
    )


def _normalize_radius(
    radius: float | torch.Tensor,
    controls: torch.Tensor,
    was_unbatched: bool,
) -> torch.Tensor:
    """Normalize scalar, per-control, or aligned-batch radii to ``(B, C)``."""

    batch_size, num_controls = controls.shape[:2]
    if isinstance(radius, torch.Tensor):
        _validate_layout(radius, controls, "tensor-valued radius and controls")
        if radius.ndim == 0:
            normalized = radius.reshape(1, 1).expand(batch_size, num_controls)
        elif tuple(radius.shape) == (num_controls,):
            normalized = radius.unsqueeze(0).expand(batch_size, num_controls)
        elif not was_unbatched and tuple(radius.shape) == (
            batch_size,
            num_controls,
        ):
            normalized = radius
        else:
            expected = (
                "a scalar or shape (C,)"
                if was_unbatched
                else (
                    "a scalar, shape (C,), or aligned shape "
                    f"(B, C)={(batch_size, num_controls)}"
                )
            )
            raise ValueError(f"radius must be {expected}, got {tuple(radius.shape)}")
    elif isinstance(radius, Real) and not isinstance(radius, bool):
        if not torch.compiler.is_compiling() and num_controls > 0:
            radius_value = float(radius)
            if not math.isfinite(radius_value):
                raise ValueError("radius must be finite")
            if radius_value <= 0:
                raise ValueError("radius must be strictly positive")
            finfo = torch.finfo(controls.dtype)
            if radius_value > finfo.max:
                raise ValueError("radius must be finite in the control dtype")
            if radius_value < finfo.tiny * finfo.eps:
                raise ValueError(
                    "radius must be strictly positive in the control dtype"
                )
        normalized = (
            torch.as_tensor(radius, dtype=controls.dtype, device=controls.device)
            .reshape(1, 1)
            .expand(batch_size, num_controls)
        )
    else:
        raise TypeError(
            "radius must be a positive finite Python real scalar or floating-point tensor"
        )

    return normalized


def normalize_morph_inputs(
    points: torch.Tensor,
    control_points: torch.Tensor,
    control_displacements: torch.Tensor,
    radius: float | torch.Tensor,
    amount: float | torch.Tensor,
    weights: torch.Tensor | None,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor | None,
    bool,
]:
    """Validate and normalize compact-Shepard morphing inputs."""

    _validate_points(points, "points")
    _validate_points(control_points, "control_points")
    _validate_layout(
        control_displacements,
        control_points,
        "control_points and control_displacements",
        True,
    )
    if points.ndim != control_points.ndim:
        raise ValueError(
            "points and controls must both be unbatched or both be batched; got ranks "
            f"{points.ndim} and {control_points.ndim}"
        )
    if points.shape[-1] != control_points.shape[-1]:
        raise ValueError(
            "points and control_points must have the same coordinate dimension, got "
            f"{points.shape[-1]} and {control_points.shape[-1]}"
        )
    if points.ndim == 3 and points.shape[0] != control_points.shape[0]:
        raise ValueError(
            "batched points and controls must have aligned batch sizes, got "
            f"{points.shape[0]} and {control_points.shape[0]}"
        )
    _validate_layout(control_points, points, "points and control_points")

    points_b3, was_unbatched = _as_batched(points)
    controls_b3, _ = _as_batched(control_points)
    control_displacements_b3, _ = _as_batched(control_displacements)
    return (
        points_b3,
        controls_b3,
        control_displacements_b3,
        _normalize_radius(radius, controls_b3, was_unbatched),
        _normalize_amount(amount, reference=points_b3),
        _normalize_weights(weights, points_b3, was_unbatched),
        was_unbatched,
    )


def restore_point_rank(points: torch.Tensor, was_unbatched: bool) -> torch.Tensor:
    """Restore an originally unbatched output to rank two."""

    return points.squeeze(0) if was_unbatched else points


__all__ = [
    "normalize_displace_inputs",
    "normalize_morph_inputs",
    "restore_point_rank",
]
