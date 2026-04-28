# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from ..._pml_common import _normalize_direction, _validate_no_autograd_warp
from ..utils import _validate_inputs
from .launch_forward import _launch_warp_forward


def pml_initializer_warp(
    pml_layer: torch.Tensor,
    direction: torch.Tensor | Sequence[float],
    thickness: int,
    courant_number: float,
    kappa: float = 1.0,
    a: float = 1.0e-8,
    inplace: bool = False,
) -> torch.Tensor:
    _validate_inputs(
        pml_layer,
        direction,
        thickness,
        courant_number,
        kappa,
        a,
        inplace,
    )
    _validate_no_autograd_warp(pml_layer)

    if not pml_layer.is_contiguous():
        raise ValueError("pml_layer must be contiguous for the warp implementation")

    output = pml_layer if inplace else pml_layer.clone()
    direction_tensor = _normalize_direction(direction, device=output.device)

    _launch_warp_forward(
        pml_layer=output,
        direction=direction_tensor,
        thickness=thickness,
        courant_number=courant_number,
        kappa=kappa,
        a=a,
    )
    return output
