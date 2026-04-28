# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from .._pml_common import (
    _as_spacing_tensor,
    _normalize_material_field,
    _normalize_offset,
    _validate_pml_layer_tensor,
    _validate_vector_field,
)


def _validate_inputs(
    electric_field: torch.Tensor,
    pml_layer: torch.Tensor,
) -> None:
    _validate_vector_field("electric_field", electric_field)
    _validate_pml_layer_tensor(pml_layer)

    if electric_field.device != pml_layer.device:
        raise ValueError("electric_field and pml_layer must be on the same device")


def _prepare_inputs(
    electric_field: torch.Tensor,
    pml_layer: torch.Tensor,
    eps: float | torch.Tensor,
    spacing: torch.Tensor | Sequence[float],
    pml_layer_offset: torch.Tensor | Sequence[int],
    inplace: bool,
) -> tuple[torch.Tensor, tuple[int, int, int], torch.Tensor]:
    _validate_inputs(electric_field, pml_layer)

    spacing_tensor = _as_spacing_tensor(
        spacing,
        device=electric_field.device,
        dtype=electric_field.dtype,
    )
    offset = _normalize_offset(pml_layer_offset)

    spatial_shape = tuple(electric_field.shape[1:])
    eps_field = _normalize_material_field(
        eps,
        "eps",
        spatial_shape,
        electric_field.device,
        field_name="electric_field",
    )

    if inplace and (electric_field.requires_grad or eps_field.requires_grad):
        raise ValueError(
            "inplace=True is not supported when electric_field or eps requires gradients"
        )

    return spacing_tensor, offset, eps_field


def _prepare_inputs_warp(
    electric_field: torch.Tensor,
    pml_layer: torch.Tensor,
    eps: float | torch.Tensor,
    spacing: torch.Tensor | Sequence[float],
    pml_layer_offset: torch.Tensor | Sequence[int],
    inplace: bool,
) -> tuple[torch.Tensor, tuple[int, int, int], torch.Tensor | None, float, bool]:
    _validate_inputs(electric_field, pml_layer)

    spacing_tensor = _as_spacing_tensor(
        spacing,
        device=electric_field.device,
        dtype=electric_field.dtype,
    )
    offset = _normalize_offset(pml_layer_offset)

    eps_is_scalar = isinstance(eps, (int, float))
    eps_field: torch.Tensor | None = None
    eps_scalar = 0.0
    eps_requires_grad = False
    if eps_is_scalar:
        eps_scalar = float(eps)
    else:
        spatial_shape = tuple(electric_field.shape[1:])
        eps_field = _normalize_material_field(
            eps,
            "eps",
            spatial_shape,
            electric_field.device,
            field_name="electric_field",
        )
        eps_requires_grad = eps_field.requires_grad

    if inplace and (electric_field.requires_grad or eps_requires_grad):
        raise ValueError(
            "inplace=True is not supported when electric_field or eps requires gradients"
        )

    return spacing_tensor, offset, eps_field, eps_scalar, eps_is_scalar
