# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch

from .utils import build_geometry, validate_inputs


def mesh_green_gauss_gradient_torch(
    points: torch.Tensor,
    cells: torch.Tensor,
    values: torch.Tensor,
) -> torch.Tensor:
    ### Validate mesh/value tensors and geometry compatibility.
    validate_inputs(points=points, cells=cells, values=values)

    ### Build geometry coefficients and cell-neighbor adjacency.
    coeff, neighbors = build_geometry(points=points, cells=cells)

    n_cells = cells.shape[0]
    dims = points.shape[1]
    value_shape = values.shape[1:]
    values_flat = values.reshape(n_cells, -1)
    n_components = values_flat.shape[1]

    coeff_cast = coeff.to(dtype=values.dtype)
    grad_flat = torch.zeros(
        (n_cells, dims, n_components),
        device=values.device,
        dtype=values.dtype,
    )

    ### Accumulate Green-Gauss face fluxes into per-cell gradients.
    for face_idx in range(coeff_cast.shape[1]):
        neigh = neighbors[:, face_idx]
        face_values = values_flat
        interior = neigh >= 0
        if torch.any(interior):
            face_values = values_flat.clone()
            face_values[interior] = 0.5 * (
                values_flat[interior] + values_flat[neigh[interior]]
            )

        grad_flat = grad_flat + coeff_cast[:, face_idx, :].unsqueeze(-1) * face_values.unsqueeze(1)

    ### Restore gradient output layout.
    return grad_flat.reshape(n_cells, dims, *value_shape)
