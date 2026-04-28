# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from .._pml_common import (
    PML_PHI_E,
    _periodic_gather,
    _periodic_scatter_add_,
    _pml_region_indices,
)
from .utils import _prepare_inputs


def pml_electric_field_update_torch(
    electric_field: torch.Tensor,
    pml_layer: torch.Tensor,
    eps: float | torch.Tensor,
    spacing: torch.Tensor | Sequence[float],
    pml_layer_offset: torch.Tensor | Sequence[int],
    dt: float,
    inplace: bool = False,
) -> torch.Tensor:
    spacing_tensor, offset, eps_field = _prepare_inputs(
        electric_field,
        pml_layer,
        eps,
        spacing,
        pml_layer_offset,
        inplace,
    )

    output = electric_field if inplace else electric_field.clone()

    pml_shape = tuple(pml_layer.shape[1:])
    i, j, k = _pml_region_indices(pml_shape, offset, output.device)

    eps_0_0_1 = _periodic_gather(eps_field, i - 1, j - 1, k)
    eps_0_1_0 = _periodic_gather(eps_field, i - 1, j, k - 1)
    eps_0_1_1 = _periodic_gather(eps_field, i - 1, j, k)
    eps_1_0_0 = _periodic_gather(eps_field, i, j - 1, k - 1)
    eps_1_0_1 = _periodic_gather(eps_field, i, j - 1, k)
    eps_1_1_0 = _periodic_gather(eps_field, i, j, k - 1)
    eps_1_1_1 = _periodic_gather(eps_field, i, j, k)

    eps_x = 0.25 * (eps_1_1_1 + eps_1_1_0 + eps_1_0_1 + eps_1_0_0)
    eps_y = 0.25 * (eps_1_1_1 + eps_1_1_0 + eps_0_1_1 + eps_0_1_0)
    eps_z = 0.25 * (eps_1_1_1 + eps_1_0_1 + eps_0_1_1 + eps_0_0_1)

    phi_e = pml_layer[PML_PHI_E]

    two_dt = 2.0 * float(dt)
    c_eh_x = torch.where(
        eps_x != 0.0,
        two_dt / (spacing_tensor[0] * (2.0 * eps_x)),
        torch.zeros_like(eps_x),
    )
    c_eh_y = torch.where(
        eps_y != 0.0,
        two_dt / (spacing_tensor[1] * (2.0 * eps_y)),
        torch.zeros_like(eps_y),
    )
    c_eh_z = torch.where(
        eps_z != 0.0,
        two_dt / (spacing_tensor[2] * (2.0 * eps_z)),
        torch.zeros_like(eps_z),
    )

    e_add_x = c_eh_x * phi_e[0]
    e_add_y = c_eh_y * phi_e[1]
    e_add_z = c_eh_z * phi_e[2]

    _periodic_scatter_add_(output[0], i, j, k, e_add_x)
    _periodic_scatter_add_(output[1], i, j, k, e_add_y)
    _periodic_scatter_add_(output[2], i, j, k, e_add_z)

    return output
