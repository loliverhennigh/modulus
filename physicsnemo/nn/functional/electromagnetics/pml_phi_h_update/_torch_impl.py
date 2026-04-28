# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from .._pml_common import (
    PML_BH,
    PML_CH,
    PML_PHI_H,
    PML_PSI_H_X,
    PML_PSI_H_Y,
    PML_PSI_H_Z,
    _normalize_offset,
    _periodic_gather,
    _pml_region_indices,
)
from .utils import _validate_inputs


def pml_phi_h_update_torch(
    electric_field: torch.Tensor,
    pml_layer: torch.Tensor,
    pml_layer_offset: torch.Tensor | Sequence[int] = (0, 0, 0),
    inplace: bool = False,
) -> torch.Tensor:
    _validate_inputs(
        electric_field,
        pml_layer,
        pml_layer_offset,
        inplace,
    )

    offset = _normalize_offset(pml_layer_offset)

    px, py, pz = pml_layer.shape[1:]
    i, j, k = _pml_region_indices((px, py, pz), offset, pml_layer.device)

    e_x = electric_field[0]
    e_y = electric_field[1]
    e_z = electric_field[2]

    e_x_0_0_0 = _periodic_gather(e_x, i, j, k)
    e_x_0_1_0 = _periodic_gather(e_x, i, j + 1, k)
    e_x_0_0_1 = _periodic_gather(e_x, i, j, k + 1)

    e_y_0_0_0 = _periodic_gather(e_y, i, j, k)
    e_y_1_0_0 = _periodic_gather(e_y, i + 1, j, k)
    e_y_0_0_1 = _periodic_gather(e_y, i, j, k + 1)

    e_z_0_0_0 = _periodic_gather(e_z, i, j, k)
    e_z_1_0_0 = _periodic_gather(e_z, i + 1, j, k)
    e_z_0_1_0 = _periodic_gather(e_z, i, j + 1, k)

    bh = pml_layer[PML_BH]
    ch = pml_layer[PML_CH]

    psi_hx_base = bh * pml_layer[PML_PSI_H_X]
    psi_hy_base = bh * pml_layer[PML_PSI_H_Y]
    psi_hz_base = bh * pml_layer[PML_PSI_H_Z]

    mask_i = (
        torch.arange(px, device=pml_layer.device, dtype=torch.int64).view(px, 1, 1)
        != (px - 1)
    ).to(dtype=pml_layer.dtype)
    mask_j = (
        torch.arange(py, device=pml_layer.device, dtype=torch.int64).view(1, py, 1)
        != (py - 1)
    ).to(dtype=pml_layer.dtype)
    mask_k = (
        torch.arange(pz, device=pml_layer.device, dtype=torch.int64).view(1, 1, pz)
        != (pz - 1)
    ).to(dtype=pml_layer.dtype)

    psi_hx = torch.stack(
        (
            psi_hx_base[0],
            psi_hx_base[1] + ((e_z_0_1_0 - e_z_0_0_0) * ch[1]) * mask_j,
            psi_hx_base[2] + ((e_y_0_0_1 - e_y_0_0_0) * ch[2]) * mask_k,
        ),
        dim=0,
    )
    psi_hy = torch.stack(
        (
            psi_hy_base[0] + ((e_z_1_0_0 - e_z_0_0_0) * ch[0]) * mask_i,
            psi_hy_base[1],
            psi_hy_base[2] + ((e_x_0_0_1 - e_x_0_0_0) * ch[2]) * mask_k,
        ),
        dim=0,
    )
    psi_hz = torch.stack(
        (
            psi_hz_base[0] + ((e_y_1_0_0 - e_y_0_0_0) * ch[0]) * mask_i,
            psi_hz_base[1] + ((e_x_0_1_0 - e_x_0_0_0) * ch[1]) * mask_j,
            psi_hz_base[2],
        ),
        dim=0,
    )

    phi_h = torch.stack(
        (
            psi_hx[1] - psi_hx[2],
            psi_hy[2] - psi_hy[0],
            psi_hz[0] - psi_hz[1],
        ),
        dim=0,
    )

    output = pml_layer if inplace else pml_layer.clone()
    output[PML_PHI_H] = phi_h
    output[PML_PSI_H_X] = psi_hx
    output[PML_PSI_H_Y] = psi_hy
    output[PML_PSI_H_Z] = psi_hz

    return output
