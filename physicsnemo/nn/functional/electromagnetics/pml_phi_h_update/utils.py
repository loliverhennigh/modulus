# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from .._pml_common import (
    _normalize_offset,
    _validate_pml_layer_tensor,
    _validate_vector_field,
)


def _validate_inputs(
    electric_field: torch.Tensor,
    pml_layer: torch.Tensor,
    pml_layer_offset: torch.Tensor | Sequence[int],
    inplace: bool,
) -> None:
    _validate_vector_field("electric_field", electric_field)
    _validate_pml_layer_tensor(pml_layer)

    if electric_field.device != pml_layer.device:
        raise ValueError("electric_field and pml_layer must be on the same device")

    _ = _normalize_offset(pml_layer_offset)

    if inplace and pml_layer.requires_grad:
        raise ValueError(
            "inplace=True is not supported when pml_layer requires gradients"
        )
