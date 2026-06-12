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


def validate_lsq_geometry(
    *,
    points: torch.Tensor,
    neighbor_offsets: torch.Tensor,
    neighbor_indices: torch.Tensor,
    min_neighbors: int,
    function_name: str,
) -> None:
    """Validate common CSR geometry inputs for LSQ mesh operators."""
    if points.ndim != 2:
        raise ValueError(
            f"{function_name}: points must have shape (n_entities, dims), "
            f"got {points.shape=}"
        )
    if points.shape[1] < 1 or points.shape[1] > 3:
        raise ValueError(
            f"{function_name}: points must be 1D/2D/3D, got dims={points.shape[1]}"
        )
    if not torch.is_floating_point(points):
        raise TypeError(f"{function_name}: points must be floating-point")
    if neighbor_offsets.ndim != 1:
        raise ValueError(f"{function_name}: neighbor_offsets must be rank-1")
    if neighbor_offsets.shape[0] != points.shape[0] + 1:
        raise ValueError(
            f"{function_name}: neighbor_offsets must have shape (n_entities + 1,), "
            f"got {neighbor_offsets.shape} for n_entities={points.shape[0]}"
        )
    if neighbor_indices.ndim != 1:
        raise ValueError(f"{function_name}: neighbor_indices must be rank-1")
    if neighbor_offsets.dtype not in (torch.int32, torch.int64):
        raise TypeError(f"{function_name}: neighbor_offsets must be int32 or int64")
    if neighbor_indices.dtype not in (torch.int32, torch.int64):
        raise TypeError(f"{function_name}: neighbor_indices must be int32 or int64")
    if min_neighbors < 0:
        raise ValueError(f"{function_name}: min_neighbors must be non-negative")
    if (
        points.device != neighbor_offsets.device
        or points.device != neighbor_indices.device
    ):
        raise ValueError(
            f"{function_name}: points, neighbor_offsets, and neighbor_indices "
            "must be on the same device"
        )
    if int(neighbor_offsets[0].item()) != 0:
        raise ValueError(f"{function_name}: neighbor_offsets must start at 0")
    if int(neighbor_offsets[-1].item()) != neighbor_indices.shape[0]:
        raise ValueError(
            f"{function_name}: neighbor_offsets[-1] must equal len(neighbor_indices)"
        )
    if torch.any(neighbor_offsets[1:] < neighbor_offsets[:-1]):
        raise ValueError(f"{function_name}: neighbor_offsets must be non-decreasing")
    if neighbor_indices.numel() > 0:
        idx_min = int(neighbor_indices.min().item())
        idx_max = int(neighbor_indices.max().item())
        if idx_min < 0 or idx_max >= points.shape[0]:
            raise ValueError(
                f"{function_name}: neighbor_indices must satisfy "
                f"0 <= index < n_entities ({points.shape[0]})"
            )


def validate_lsq_scalar_field(
    *,
    points: torch.Tensor,
    values: torch.Tensor,
    neighbor_offsets: torch.Tensor,
    neighbor_indices: torch.Tensor,
    min_neighbors: int,
    function_name: str,
) -> None:
    """Validate scalar-valued inputs for LSQ mesh scalar operators."""
    validate_lsq_geometry(
        points=points,
        neighbor_offsets=neighbor_offsets,
        neighbor_indices=neighbor_indices,
        min_neighbors=min_neighbors,
        function_name=function_name,
    )
    if values.ndim != 1:
        raise ValueError(
            f"{function_name}: values must have shape (n_entities,), "
            f"got {values.shape=}"
        )
    if values.shape[0] != points.shape[0]:
        raise ValueError(
            f"{function_name}: values leading dimension must match points: "
            f"{values.shape[0]} != {points.shape[0]}"
        )
    if not torch.is_floating_point(values):
        raise TypeError(f"{function_name}: values must be floating-point")
    if values.device != points.device:
        raise ValueError(
            f"{function_name}: values and points must be on the same device"
        )


def validate_lsq_vector_field(
    *,
    points: torch.Tensor,
    vector_field: torch.Tensor,
    neighbor_offsets: torch.Tensor,
    neighbor_indices: torch.Tensor,
    min_neighbors: int,
    function_name: str,
    required_dims: tuple[int, ...] | None = None,
) -> None:
    """Validate vector-valued inputs for LSQ mesh vector operators."""
    validate_lsq_geometry(
        points=points,
        neighbor_offsets=neighbor_offsets,
        neighbor_indices=neighbor_indices,
        min_neighbors=min_neighbors,
        function_name=function_name,
    )
    if vector_field.ndim != 2:
        raise ValueError(
            f"{function_name}: vector_field must have shape (n_entities, dims), "
            f"got {vector_field.shape=}"
        )
    if vector_field.shape != points.shape:
        raise ValueError(
            f"{function_name}: vector_field shape must match points shape, "
            f"got {vector_field.shape} and {points.shape}"
        )
    if required_dims is not None and points.shape[1] not in required_dims:
        raise ValueError(
            f"{function_name}: supported dims are {required_dims}, got {points.shape[1]}"
        )
    if not torch.is_floating_point(vector_field):
        raise TypeError(f"{function_name}: vector_field must be floating-point")
    if vector_field.device != points.device:
        raise ValueError(
            f"{function_name}: vector_field and points must be on the same device"
        )


def make_knn_csr_case(
    *,
    device: torch.device | str,
    n_entities: int,
    n_dims: int,
    k_neighbors: int,
    seed: int,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Build deterministic point-cloud KNN CSR geometry for tests and benchmarks."""
    device = torch.device(device)
    generator = torch.Generator(device=device)
    generator.manual_seed(seed)
    points = torch.rand((n_entities, n_dims), generator=generator, device=device)
    dists = torch.cdist(points, points)
    knn = torch.topk(dists, k=k_neighbors + 1, largest=False, dim=1).indices[:, 1:]
    offsets = torch.arange(
        0,
        n_entities * k_neighbors + 1,
        k_neighbors,
        device=device,
        dtype=torch.int64,
    )
    indices = knn.reshape(-1).to(torch.int64)
    return points.to(torch.float32), offsets, indices
