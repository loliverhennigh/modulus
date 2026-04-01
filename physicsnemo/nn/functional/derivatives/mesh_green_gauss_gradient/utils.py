# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch


def validate_inputs(
    points: torch.Tensor,
    cells: torch.Tensor,
    values: torch.Tensor,
) -> None:
    ### Validate mesh point coordinates and supported spatial dimensionality.
    if points.ndim != 2:
        raise ValueError(f"points must have shape (n_points, dims), got points.shape={points.shape}")
    if points.shape[1] not in (2, 3):
        raise ValueError(f"mesh_green_gauss_gradient supports dims in {{2, 3}}, got {points.shape[1]}")
    if not torch.is_floating_point(points):
        raise TypeError("points must be a floating-point tensor")

    ### Validate simplicial connectivity and compatibility with spatial dims.
    if cells.ndim != 2:
        raise ValueError(f"cells must have shape (n_cells, n_vertices), got cells.shape={cells.shape}")
    expected_vertices = points.shape[1] + 1
    if cells.shape[1] != expected_vertices:
        raise ValueError(
            f"cells must contain {expected_vertices} vertices per simplex for dims={points.shape[1]}, "
            f"got {cells.shape[1]}"
        )
    if cells.dtype not in (torch.int32, torch.int64):
        raise TypeError("cells must be int32 or int64")

    ### Validate cell-centered values tensor.
    if values.ndim < 1:
        raise ValueError(f"values must have shape (n_cells, ...), got values.shape={values.shape}")
    if values.shape[0] != cells.shape[0]:
        raise ValueError(
            f"values leading dimension must match n_cells: {values.shape[0]} != {cells.shape[0]}"
        )
    if not torch.is_floating_point(values):
        raise TypeError("values must be a floating-point tensor")

    ### Validate co-located tensors and index range invariants.
    if points.device != cells.device or points.device != values.device:
        raise ValueError("points, cells, and values must be on the same device")
    if cells.numel() > 0:
        idx_min = int(cells.min().item())
        idx_max = int(cells.max().item())
        if idx_min < 0 or idx_max >= points.shape[0]:
            raise ValueError(
                f"cells indices must satisfy 0 <= index < n_points ({points.shape[0]})"
            )


def build_geometry(
    points: torch.Tensor,
    cells: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Build Green-Gauss geometry tensors from simplicial connectivity.

    Returns
    -------
    tuple[torch.Tensor, torch.Tensor]
        ``(coefficients, neighbors)`` where:
        - ``coefficients`` has shape ``(n_cells, n_faces, dims)`` and stores
          outward face-area vectors divided by cell volume/area.
        - ``neighbors`` has shape ``(n_cells, n_faces)`` with adjacent cell id
          or ``-1`` for boundary faces.
    """

    n_cells = cells.shape[0]
    dims = points.shape[1]
    n_faces = dims + 1

    ### Build per-cell face-vertex tables and interior neighbor map.
    if dims == 2:
        local_faces = ((0, 1), (1, 2), (2, 0))
    else:
        local_faces = ((1, 2, 3), (0, 3, 2), (0, 1, 3), (0, 2, 1))

    cells_i64 = cells.to(dtype=torch.int64)
    cells_cpu = cells_i64.detach().cpu().tolist()
    neighbors = torch.full((n_cells, n_faces), -1, device=points.device, dtype=torch.int64)
    face_vertices = torch.empty(
        (n_cells, n_faces, dims),
        device=points.device,
        dtype=torch.int64,
    )

    open_faces: dict[tuple[int, ...], tuple[int, int]] = {}
    for cell_idx, cell in enumerate(cells_cpu):
        for face_idx, local_face in enumerate(local_faces):
            verts = tuple(int(cell[v]) for v in local_face)
            face_vertices[cell_idx, face_idx] = torch.tensor(
                verts,
                device=points.device,
                dtype=torch.int64,
            )
            key = tuple(sorted(verts))
            if key in open_faces:
                other_cell, other_face = open_faces.pop(key)
                neighbors[cell_idx, face_idx] = other_cell
                neighbors[other_cell, other_face] = cell_idx
            else:
                open_faces[key] = (cell_idx, face_idx)

    ### Compute cell centroids and volume/area metrics.
    cell_points = points[cells_i64]
    centroids = cell_points.mean(dim=1)
    if dims == 2:
        p0, p1, p2 = cell_points[:, 0], cell_points[:, 1], cell_points[:, 2]
        cell_volume = 0.5 * torch.abs(
            (p1[:, 0] - p0[:, 0]) * (p2[:, 1] - p0[:, 1])
            - (p1[:, 1] - p0[:, 1]) * (p2[:, 0] - p0[:, 0])
        )
    else:
        p0, p1, p2, p3 = (
            cell_points[:, 0],
            cell_points[:, 1],
            cell_points[:, 2],
            cell_points[:, 3],
        )
        cell_volume = torch.abs(
            torch.einsum("bi,bi->b", p1 - p0, torch.cross(p2 - p0, p3 - p0, dim=-1))
        ) / 6.0
    cell_volume = torch.clamp(cell_volume, min=1.0e-12)

    ### Compute outward face-area vectors divided by cell volume/area.
    coeff = torch.empty(
        (n_cells, n_faces, dims),
        device=points.device,
        dtype=points.dtype,
    )
    for face_idx in range(n_faces):
        verts = face_vertices[:, face_idx]
        if dims == 2:
            va = points[verts[:, 0]]
            vb = points[verts[:, 1]]
            edge = vb - va
            normal = torch.stack((edge[:, 1], -edge[:, 0]), dim=-1)
            face_center = 0.5 * (va + vb)
            to_face = face_center - centroids
            sign = torch.where(
                torch.einsum("bi,bi->b", normal, to_face) >= 0.0,
                1.0,
                -1.0,
            ).unsqueeze(-1)
            area_vec = sign * normal
        else:
            va = points[verts[:, 0]]
            vb = points[verts[:, 1]]
            vc = points[verts[:, 2]]
            normal = 0.5 * torch.cross(vb - va, vc - va, dim=-1)
            face_center = (va + vb + vc) / 3.0
            to_face = face_center - centroids
            sign = torch.where(
                torch.einsum("bi,bi->b", normal, to_face) >= 0.0,
                1.0,
                -1.0,
            ).unsqueeze(-1)
            area_vec = sign * normal

        coeff[:, face_idx, :] = area_vec / cell_volume.unsqueeze(-1)

    return coeff, neighbors
