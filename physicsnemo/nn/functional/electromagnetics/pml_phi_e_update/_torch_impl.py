# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from .._pml_common import (
    PML_BE,
    PML_CE,
    PML_PHI_E,
    PML_PSI_E_X,
    PML_PSI_E_Y,
    PML_PSI_E_Z,
    _normalize_offset,
    _periodic_gather,
    _pml_region_indices,
)
from .utils import _validate_inputs


def pml_phi_e_update_torch(
    magnetic_field: torch.Tensor,
    pml_layer: torch.Tensor,
    pml_layer_offset: torch.Tensor | Sequence[int] = (0, 0, 0),
    inplace: bool = False,
) -> torch.Tensor:
    _validate_inputs(
        magnetic_field,
        pml_layer,
        pml_layer_offset,
        inplace,
    )

    offset = _normalize_offset(pml_layer_offset)

    px, py, pz = pml_layer.shape[1:]
    i, j, k = _pml_region_indices((px, py, pz), offset, pml_layer.device)

    h_x = magnetic_field[0]
    h_y = magnetic_field[1]
    h_z = magnetic_field[2]

    h_x_1_1_1 = _periodic_gather(h_x, i, j, k)
    h_x_1_0_1 = _periodic_gather(h_x, i, j - 1, k)
    h_x_1_1_0 = _periodic_gather(h_x, i, j, k - 1)

    h_y_1_1_1 = _periodic_gather(h_y, i, j, k)
    h_y_0_1_1 = _periodic_gather(h_y, i - 1, j, k)
    h_y_1_1_0 = _periodic_gather(h_y, i, j, k - 1)

    h_z_1_1_1 = _periodic_gather(h_z, i, j, k)
    h_z_0_1_1 = _periodic_gather(h_z, i - 1, j, k)
    h_z_1_0_1 = _periodic_gather(h_z, i, j - 1, k)

    be = pml_layer[PML_BE]
    ce = pml_layer[PML_CE]

    psi_ex_base = be * pml_layer[PML_PSI_E_X]
    psi_ey_base = be * pml_layer[PML_PSI_E_Y]
    psi_ez_base = be * pml_layer[PML_PSI_E_Z]

    mask_i = (
        torch.arange(px, device=pml_layer.device, dtype=torch.int64).view(px, 1, 1)
        != 0
    ).to(dtype=pml_layer.dtype)
    mask_j = (
        torch.arange(py, device=pml_layer.device, dtype=torch.int64).view(1, py, 1)
        != 0
    ).to(dtype=pml_layer.dtype)
    mask_k = (
        torch.arange(pz, device=pml_layer.device, dtype=torch.int64).view(1, 1, pz)
        != 0
    ).to(dtype=pml_layer.dtype)

    psi_ex = torch.stack(
        (
            psi_ex_base[0],
            psi_ex_base[1] + ((h_z_1_1_1 - h_z_1_0_1) * ce[1]) * mask_j,
            psi_ex_base[2] + ((h_y_1_1_1 - h_y_1_1_0) * ce[2]) * mask_k,
        ),
        dim=0,
    )
    psi_ey = torch.stack(
        (
            psi_ey_base[0] + ((h_z_1_1_1 - h_z_0_1_1) * ce[0]) * mask_i,
            psi_ey_base[1],
            psi_ey_base[2] + ((h_x_1_1_1 - h_x_1_1_0) * ce[2]) * mask_k,
        ),
        dim=0,
    )
    psi_ez = torch.stack(
        (
            psi_ez_base[0] + ((h_y_1_1_1 - h_y_0_1_1) * ce[0]) * mask_i,
            psi_ez_base[1] + ((h_x_1_1_1 - h_x_1_0_1) * ce[1]) * mask_j,
            psi_ez_base[2],
        ),
        dim=0,
    )

    phi_e = torch.stack(
        (
            psi_ex[1] - psi_ex[2],
            psi_ey[2] - psi_ey[0],
            psi_ez[0] - psi_ez[1],
        ),
        dim=0,
    )

    output = pml_layer if inplace else pml_layer.clone()
    output[PML_PHI_E] = phi_e
    output[PML_PSI_E_X] = psi_ex
    output[PML_PSI_E_Y] = psi_ey
    output[PML_PSI_E_Z] = psi_ez

    return output
