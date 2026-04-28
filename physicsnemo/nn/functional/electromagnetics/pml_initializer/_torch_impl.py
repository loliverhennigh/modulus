# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from .._pml_common import PML_BE, PML_BH, PML_CE, PML_CH, _normalize_direction
from .utils import _validate_inputs


# Initialize PML coefficients for one boundary slab.
def pml_initializer_torch(
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

    output = pml_layer if inplace else pml_layer.clone()

    direction_tensor = _normalize_direction(direction, device=output.device)
    axis = int(torch.argmax(direction_tensor.abs()).item())
    positive_direction = bool(
        torch.isclose(direction_tensor[axis], torch.tensor(1.0, device=output.device))
    )

    px, py, pz = output.shape[1:]
    i = torch.arange(px, device=output.device, dtype=torch.float32).view(px, 1, 1)
    j = torch.arange(py, device=output.device, dtype=torch.float32).view(1, py, 1)
    k = torch.arange(pz, device=output.device, dtype=torch.float32).view(1, 1, pz)

    dot = i * direction_tensor[0] + j * direction_tensor[1] + k * direction_tensor[2]

    thickness_f = float(thickness)
    if positive_direction:
        step_e = thickness_f - dot - 0.5
        step_h = thickness_f - dot - 1.0
    else:
        step_e = -dot + 0.5
        step_h = -dot + 1.0

    norm = (thickness_f + 1.0) ** 4.0
    sigma_e = (40.0 * step_e**3.0) / norm
    sigma_h = (40.0 * step_h**3.0) / norm

    vec_sigma_e = torch.zeros(
        (3, px, py, pz), device=output.device, dtype=torch.float32
    )
    vec_sigma_h = torch.zeros(
        (3, px, py, pz), device=output.device, dtype=torch.float32
    )
    vec_sigma_e[axis] = sigma_e
    vec_sigma_h[axis] = sigma_h

    be = torch.exp(-((vec_sigma_e / float(kappa)) + float(a)) * float(courant_number))
    ce = ((be - 1.0) * vec_sigma_e) / (
        vec_sigma_e * float(kappa) + float(a) * (float(kappa) ** 2.0)
    )

    bh = torch.exp(-((vec_sigma_h / float(kappa)) + float(a)) * float(courant_number))
    ch = ((bh - 1.0) * vec_sigma_h) / (
        vec_sigma_h * float(kappa) + float(a) * (float(kappa) ** 2.0)
    )

    output[PML_BE] = be
    output[PML_CE] = ce
    output[PML_BH] = bh
    output[PML_CH] = ch
    return output
