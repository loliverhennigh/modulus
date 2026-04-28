# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from .._pml_common import (
    PML_PHI_H,
    _periodic_gather,
    _periodic_scatter_add_,
    _pml_region_indices,
)
from .utils import _prepare_inputs


def _harmonic_average(a: torch.Tensor, b: torch.Tensor) -> torch.Tensor:
    denom = a + b
    return torch.where(denom != 0.0, (2.0 * a * b) / denom, torch.zeros_like(denom))


def pml_magnetic_field_update_torch(
    magnetic_field: torch.Tensor,
    pml_layer: torch.Tensor,
    mu: float | torch.Tensor,
    spacing: torch.Tensor | Sequence[float],
    pml_layer_offset: torch.Tensor | Sequence[int],
    dt: float,
    inplace: bool = False,
) -> torch.Tensor:
    spacing_tensor, offset, mu_field = _prepare_inputs(
        magnetic_field,
        pml_layer,
        mu,
        spacing,
        pml_layer_offset,
        inplace,
    )

    output = magnetic_field if inplace else magnetic_field.clone()

    pml_shape = tuple(pml_layer.shape[1:])
    i, j, k = _pml_region_indices(pml_shape, offset, output.device)

    mu_0_1_1 = _periodic_gather(mu_field, i - 1, j, k)
    mu_1_0_1 = _periodic_gather(mu_field, i, j - 1, k)
    mu_1_1_0 = _periodic_gather(mu_field, i, j, k - 1)
    mu_1_1_1 = _periodic_gather(mu_field, i, j, k)

    mu_x = _harmonic_average(mu_1_1_1, mu_0_1_1)
    mu_y = _harmonic_average(mu_1_1_1, mu_1_0_1)
    mu_z = _harmonic_average(mu_1_1_1, mu_1_1_0)

    phi_h = pml_layer[PML_PHI_H]

    two_dt = 2.0 * float(dt)
    c_he_x = torch.where(
        mu_x != 0.0,
        two_dt / (spacing_tensor[0] * (2.0 * mu_x)),
        torch.zeros_like(mu_x),
    )
    c_he_y = torch.where(
        mu_y != 0.0,
        two_dt / (spacing_tensor[1] * (2.0 * mu_y)),
        torch.zeros_like(mu_y),
    )
    c_he_z = torch.where(
        mu_z != 0.0,
        two_dt / (spacing_tensor[2] * (2.0 * mu_z)),
        torch.zeros_like(mu_z),
    )

    h_add_x = c_he_x * phi_h[0]
    h_add_y = c_he_y * phi_h[1]
    h_add_z = c_he_z * phi_h[2]

    _periodic_scatter_add_(output[0], i, j, k, -h_add_x)
    _periodic_scatter_add_(output[1], i, j, k, -h_add_y)
    _periodic_scatter_add_(output[2], i, j, k, -h_add_z)

    return output
