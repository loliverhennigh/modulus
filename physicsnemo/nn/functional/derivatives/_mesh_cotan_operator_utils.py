# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import torch

from physicsnemo.mesh.utilities._tolerances import safe_eps


def validate_cotan_geometry(
    *,
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    n_points: int,
    function_name: str,
) -> None:
    """Validate common cotangent edge operator geometry tensors."""
    if edges.ndim != 2 or edges.shape[1] != 2:
        raise ValueError(
            f"{function_name}: edges must have shape (n_edges, 2), got {edges.shape=}"
        )
    if edges.dtype not in (torch.int32, torch.int64):
        raise TypeError(f"{function_name}: edges must be int32 or int64")
    if cotan_weights.ndim != 1:
        raise ValueError(
            f"{function_name}: cotan_weights must have shape (n_edges,), "
            f"got {cotan_weights.shape=}"
        )
    if cotan_weights.shape[0] != edges.shape[0]:
        raise ValueError(
            f"{function_name}: cotan_weights length must match edges: "
            f"{cotan_weights.shape[0]} != {edges.shape[0]}"
        )
    if not torch.is_floating_point(cotan_weights):
        raise TypeError(f"{function_name}: cotan_weights must be floating-point")
    if dual_volumes.ndim != 1:
        raise ValueError(
            f"{function_name}: dual_volumes must have shape (n_points,), "
            f"got {dual_volumes.shape=}"
        )
    if dual_volumes.shape[0] != n_points:
        raise ValueError(
            f"{function_name}: dual_volumes length must match n_points: "
            f"{dual_volumes.shape[0]} != {n_points}"
        )
    if not torch.is_floating_point(dual_volumes):
        raise TypeError(f"{function_name}: dual_volumes must be floating-point")
    if edges.device != cotan_weights.device or edges.device != dual_volumes.device:
        raise ValueError(
            f"{function_name}: edges, cotan_weights, and dual_volumes "
            "must be on the same device"
        )
    if edges.numel() > 0:
        idx_min = int(edges.min().item())
        idx_max = int(edges.max().item())
        if idx_min < 0 or idx_max >= n_points:
            raise ValueError(
                f"{function_name}: edges must satisfy 0 <= index < n_points ({n_points})"
            )


def validate_cotan_laplacian_inputs(
    *,
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    values: torch.Tensor,
    function_name: str,
) -> None:
    """Validate cotangent Laplacian inputs."""
    if values.ndim < 1:
        raise ValueError(
            f"{function_name}: values must have shape (n_points, ...), "
            f"got {values.shape=}"
        )
    if not torch.is_floating_point(values):
        raise TypeError(f"{function_name}: values must be floating-point")
    validate_cotan_geometry(
        edges=edges,
        cotan_weights=cotan_weights,
        dual_volumes=dual_volumes,
        n_points=values.shape[0],
        function_name=function_name,
    )
    if values.device != edges.device:
        raise ValueError(f"{function_name}: values and geometry must be on same device")


def validate_cotan_divergence_inputs(
    *,
    points: torch.Tensor,
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    vector_field: torch.Tensor,
    function_name: str,
) -> None:
    """Validate cotangent divergence inputs."""
    if points.ndim != 2:
        raise ValueError(
            f"{function_name}: points must have shape (n_points, dims), "
            f"got {points.shape=}"
        )
    if not torch.is_floating_point(points):
        raise TypeError(f"{function_name}: points must be floating-point")
    if vector_field.shape != points.shape:
        raise ValueError(
            f"{function_name}: vector_field shape must match points shape, "
            f"got {vector_field.shape} and {points.shape}"
        )
    if not torch.is_floating_point(vector_field):
        raise TypeError(f"{function_name}: vector_field must be floating-point")
    validate_cotan_geometry(
        edges=edges,
        cotan_weights=cotan_weights,
        dual_volumes=dual_volumes,
        n_points=points.shape[0],
        function_name=function_name,
    )
    if vector_field.device != points.device or edges.device != points.device:
        raise ValueError(
            f"{function_name}: points, vector_field, and geometry must be on same device"
        )


def normalize_cotan_accumulation(
    accumulation: torch.Tensor,
    dual_volumes: torch.Tensor,
) -> torch.Tensor:
    """Normalize vertex accumulations by dual volumes with dtype-aware floors."""
    volumes = dual_volumes.clamp(min=safe_eps(dual_volumes.dtype))
    if accumulation.ndim == 1:
        return accumulation / volumes
    return accumulation / volumes.view(-1, *([1] * (accumulation.ndim - 1)))


def make_cotan_edge_case(
    *,
    device: torch.device | str,
    n_points: int,
    n_dims: int,
    seed: int,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    """Build deterministic cotangent-style edge inputs for tests and benchmarks."""
    device = torch.device(device)
    generator = torch.Generator(device=device)
    generator.manual_seed(seed)
    points = torch.rand((n_points, n_dims), generator=generator, device=device)

    edge_start = torch.arange(n_points - 1, device=device, dtype=torch.int64)
    chain_edges = torch.stack((edge_start, edge_start + 1), dim=-1)
    skip_edges = torch.stack((edge_start[:-1], edge_start[:-1] + 2), dim=-1)
    edges = torch.cat((chain_edges, skip_edges), dim=0)

    edge_vectors = points[edges[:, 1]] - points[edges[:, 0]]
    lengths = edge_vectors.norm(dim=-1).clamp(min=safe_eps(points.dtype))
    cotan_weights = lengths.reciprocal().to(torch.float32)
    dual_volumes = torch.ones((n_points,), dtype=torch.float32, device=device)

    return points.to(torch.float32), edges, cotan_weights, dual_volumes
