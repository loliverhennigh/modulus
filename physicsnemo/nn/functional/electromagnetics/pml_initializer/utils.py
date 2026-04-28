# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from .._pml_common import _normalize_direction, _validate_pml_layer_tensor


# Validate the public API contract for pml_initializer.
def _validate_inputs(
    pml_layer: torch.Tensor,
    direction: torch.Tensor | Sequence[float],
    thickness: int,
    courant_number: float,
    kappa: float,
    a: float,
    inplace: bool,
) -> None:
    _validate_pml_layer_tensor(pml_layer)

    direction_tensor = _normalize_direction(direction, device=pml_layer.device)
    axis = int(torch.argmax(direction_tensor.abs()).item())

    if thickness <= 0:
        raise ValueError("thickness must be > 0")
    if int(pml_layer.shape[axis + 1]) != int(thickness):
        raise ValueError(
            "thickness must match pml_layer extent along the selected direction axis"
        )

    _ = float(courant_number)
    _ = float(kappa)
    _ = float(a)

    if inplace and pml_layer.requires_grad:
        raise ValueError(
            "inplace=True is not supported when pml_layer requires gradients"
        )
