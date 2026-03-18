# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch


def _validate_inputs(
    points: torch.Tensor,
    values: torch.Tensor,
    neighbor_offsets: torch.Tensor,
    neighbor_indices: torch.Tensor,
    *,
    min_neighbors: int,
) -> None:
    ### Validate core tensor shapes and dimensions.
    if points.ndim != 2:
        raise ValueError(f"points must have shape (n_entities, dims), got {points.shape=}")
    if points.shape[1] < 1 or points.shape[1] > 3:
        raise ValueError(f"points must be 1D/2D/3D, got dims={points.shape[1]}")
    if values.ndim < 1:
        raise ValueError(f"values must have shape (n_entities, ...), got {values.shape=}")
    if values.shape[0] != points.shape[0]:
        raise ValueError(
            f"values leading dimension must match points: {values.shape[0]} != {points.shape[0]}"
        )
    if neighbor_offsets.ndim != 1:
        raise ValueError("neighbor_offsets must be rank-1")
    if neighbor_offsets.shape[0] != points.shape[0] + 1:
        raise ValueError(
            "neighbor_offsets must have shape (n_entities + 1,), "
            f"got {neighbor_offsets.shape} for n_entities={points.shape[0]}"
        )
    if neighbor_indices.ndim != 1:
        raise ValueError("neighbor_indices must be rank-1")
    if min_neighbors < 0:
        raise ValueError("min_neighbors must be non-negative")

    ### Validate all inputs are co-located on the same device.
    if not (
        points.device == values.device
        and points.device == neighbor_offsets.device
        and points.device == neighbor_indices.device
    ):
        raise ValueError(
            "points, values, neighbor_offsets, and neighbor_indices must be on the same device"
        )

    ### Validate floating-point and index dtypes.
    if not torch.is_floating_point(points):
        raise TypeError("points must be floating-point")
    if not torch.is_floating_point(values):
        raise TypeError("values must be floating-point")

    if neighbor_offsets.dtype not in (torch.int32, torch.int64):
        raise TypeError("neighbor_offsets must be int32 or int64")
    if neighbor_indices.dtype not in (torch.int32, torch.int64):
        raise TypeError("neighbor_indices must be int32 or int64")

    ### Validate CSR range invariants.
    if int(neighbor_offsets[0].item()) != 0:
        raise ValueError("neighbor_offsets must start at 0")
    if int(neighbor_offsets[-1].item()) != neighbor_indices.shape[0]:
        raise ValueError("neighbor_offsets[-1] must equal len(neighbor_indices)")
    if torch.any(neighbor_offsets[1:] < neighbor_offsets[:-1]):
        raise ValueError("neighbor_offsets must be non-decreasing")

    if neighbor_indices.numel() > 0:
        idx_min = int(neighbor_indices.min().item())
        idx_max = int(neighbor_indices.max().item())
        if idx_min < 0 or idx_max >= points.shape[0]:
            raise ValueError(
                f"neighbor_indices must satisfy 0 <= index < n_entities ({points.shape[0]})"
            )


### Convert CSR adjacency to a padded (N, K_max) matrix with -1 sentinels.
def _csr_to_padded(neighbor_offsets: torch.Tensor, neighbor_indices: torch.Tensor) -> torch.Tensor:
    n_entities = neighbor_offsets.shape[0] - 1
    counts = neighbor_offsets[1:] - neighbor_offsets[:-1]
    max_count = int(counts.max().item()) if n_entities > 0 else 0

    padded = torch.full(
        (n_entities, max_count),
        -1,
        dtype=torch.int64,
        device=neighbor_offsets.device,
    )
    if max_count == 0:
        return padded

    col_idx = torch.arange(max_count, device=neighbor_offsets.device).unsqueeze(0)
    valid = col_idx < counts.unsqueeze(1)
    gather_idx = neighbor_offsets[:-1].unsqueeze(1) + col_idx
    padded[valid] = neighbor_indices[gather_idx[valid]]
    return padded


def mesh_lsq_gradient_torch(
    points: torch.Tensor,
    values: torch.Tensor,
    neighbor_offsets: torch.Tensor,
    neighbor_indices: torch.Tensor,
    weight_power: float = 2.0,
    min_neighbors: int = 0,
) -> torch.Tensor:
    ### Validate inputs before building LSQ systems.
    _validate_inputs(
        points=points,
        values=values,
        neighbor_offsets=neighbor_offsets,
        neighbor_indices=neighbor_indices,
        min_neighbors=min_neighbors,
    )

    ### Normalize dtypes/layout for stable downstream linear algebra.
    points = points.contiguous()
    values = values.contiguous()
    neighbor_offsets = neighbor_offsets.to(dtype=torch.int64, device=points.device).contiguous()
    neighbor_indices = neighbor_indices.to(dtype=torch.int64, device=points.device).contiguous()

    n_entities = points.shape[0]
    n_dims = points.shape[1]
    value_shape = values.shape[1:]

    ### Expand CSR adjacency into padded gather format.
    neighbors = _csr_to_padded(neighbor_offsets, neighbor_indices)
    counts = neighbor_offsets[1:] - neighbor_offsets[:-1]

    ### Build neighbor gathers with mask for padded entries.
    idx = neighbors.clamp_min(0).to(torch.long)
    valid = neighbors >= 0

    ### Build weighted LSQ matrices A and b for all entities/components.
    points_cast = points.to(dtype=values.dtype)
    center = points_cast.unsqueeze(1)
    neigh_points = points_cast[idx]
    dx = neigh_points - center

    values_flat = values.reshape(n_entities, -1)
    dphi_flat = values_flat[idx] - values_flat.unsqueeze(1)

    dist2 = (dx * dx).sum(dim=-1).clamp_min(1.0e-20)
    weights = dist2.pow(-0.5 * weight_power)
    weights = torch.where(valid, weights, torch.zeros_like(weights))

    sqrt_w = weights.sqrt().unsqueeze(-1)
    A_weighted = sqrt_w * dx

    b_weighted = sqrt_w * dphi_flat
    ### Solve batched weighted least-squares systems.
    solution = torch.linalg.lstsq(
        A_weighted,
        b_weighted,
        rcond=None,
    ).solution

    ### Restore gradient output shape.
    gradients = solution.reshape(n_entities, n_dims, *value_shape)

    ### Enforce explicit zero gradients for rows below min-neighbor threshold.
    if min_neighbors > 0:
        valid_row = counts >= min_neighbors
        gradients = gradients * valid_row.view(-1, *([1] * (gradients.ndim - 1)))

    return gradients
