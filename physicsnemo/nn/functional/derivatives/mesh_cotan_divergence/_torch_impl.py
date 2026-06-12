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

from physicsnemo.nn.functional.derivatives._mesh_cotan_operator_utils import (
    normalize_cotan_accumulation,
    validate_cotan_divergence_inputs,
)


def mesh_cotan_divergence_torch(
    points: torch.Tensor,
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    vector_field: torch.Tensor,
) -> torch.Tensor:
    """Compute cotangent/DEC mesh divergence with eager PyTorch."""
    validate_cotan_divergence_inputs(
        points=points,
        edges=edges,
        cotan_weights=cotan_weights,
        dual_volumes=dual_volumes,
        vector_field=vector_field,
        function_name="mesh_cotan_divergence",
    )

    edge_indices = edges.to(dtype=torch.int64, device=points.device)
    v0 = edge_indices[:, 0]
    v1 = edge_indices[:, 1]

    edge_vectors = points[v1] - points[v0]
    edge_average = 0.5 * (vector_field[v0] + vector_field[v1])
    flat_edge_flux = (edge_average * edge_vectors).sum(dim=-1)
    weighted_flux = cotan_weights.to(dtype=points.dtype) * flat_edge_flux

    divergence = torch.zeros(
        (points.shape[0],),
        dtype=vector_field.dtype,
        device=points.device,
    )
    divergence.index_add_(0, v0, weighted_flux.to(dtype=vector_field.dtype))
    divergence.index_add_(0, v1, -weighted_flux.to(dtype=vector_field.dtype))
    return normalize_cotan_accumulation(divergence, dual_volumes)
