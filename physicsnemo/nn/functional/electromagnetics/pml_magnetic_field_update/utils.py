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
    magnetic_field: torch.Tensor,
    pml_layer: torch.Tensor,
) -> None:
    _validate_vector_field("magnetic_field", magnetic_field)
    _validate_pml_layer_tensor(pml_layer)

    if magnetic_field.device != pml_layer.device:
        raise ValueError("magnetic_field and pml_layer must be on the same device")


def _prepare_inputs(
    magnetic_field: torch.Tensor,
    pml_layer: torch.Tensor,
    mu: float | torch.Tensor,
    spacing: torch.Tensor | Sequence[float],
    pml_layer_offset: torch.Tensor | Sequence[int],
    inplace: bool,
) -> tuple[torch.Tensor, tuple[int, int, int], torch.Tensor]:
    _validate_inputs(magnetic_field, pml_layer)

    spacing_tensor = _as_spacing_tensor(
        spacing,
        device=magnetic_field.device,
        dtype=magnetic_field.dtype,
    )
    offset = _normalize_offset(pml_layer_offset)

    spatial_shape = tuple(magnetic_field.shape[1:])
    mu_field = _normalize_material_field(
        mu,
        "mu",
        spatial_shape,
        magnetic_field.device,
        field_name="magnetic_field",
    )

    if inplace and (magnetic_field.requires_grad or mu_field.requires_grad):
        raise ValueError(
            "inplace=True is not supported when magnetic_field or mu requires gradients"
        )

    return spacing_tensor, offset, mu_field


def _prepare_inputs_warp(
    magnetic_field: torch.Tensor,
    pml_layer: torch.Tensor,
    mu: float | torch.Tensor,
    spacing: torch.Tensor | Sequence[float],
    pml_layer_offset: torch.Tensor | Sequence[int],
    inplace: bool,
) -> tuple[torch.Tensor, tuple[int, int, int], torch.Tensor | None, float, bool]:
    _validate_inputs(magnetic_field, pml_layer)

    spacing_tensor = _as_spacing_tensor(
        spacing,
        device=magnetic_field.device,
        dtype=magnetic_field.dtype,
    )
    offset = _normalize_offset(pml_layer_offset)

    mu_is_scalar = isinstance(mu, (int, float))
    mu_field: torch.Tensor | None = None
    mu_scalar = 0.0
    mu_requires_grad = False
    if mu_is_scalar:
        mu_scalar = float(mu)
    else:
        spatial_shape = tuple(magnetic_field.shape[1:])
        mu_field = _normalize_material_field(
            mu,
            "mu",
            spatial_shape,
            magnetic_field.device,
            field_name="magnetic_field",
        )
        mu_requires_grad = mu_field.requires_grad

    if inplace and (magnetic_field.requires_grad or mu_requires_grad):
        raise ValueError(
            "inplace=True is not supported when magnetic_field or mu requires gradients"
        )

    return spacing_tensor, offset, mu_field, mu_scalar, mu_is_scalar
