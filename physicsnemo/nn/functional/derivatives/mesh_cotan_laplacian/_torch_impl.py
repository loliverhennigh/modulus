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
    validate_cotan_laplacian_inputs,
)


def mesh_cotan_laplacian_torch(
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    values: torch.Tensor,
) -> torch.Tensor:
    """Apply the normalized cotangent Laplacian with eager PyTorch."""
    validate_cotan_laplacian_inputs(
        edges=edges,
        cotan_weights=cotan_weights,
        dual_volumes=dual_volumes,
        values=values,
        function_name="mesh_cotan_laplacian",
    )

    n_points = values.shape[0]
    value_shape = values.shape[1:]
    values_flat = values.reshape(n_points, -1)
    accumulation_flat = torch.zeros_like(values_flat)

    edge_indices = edges.to(dtype=torch.int64, device=values.device)
    v0 = edge_indices[:, 0]
    v1 = edge_indices[:, 1]
    weights = cotan_weights.to(dtype=values.dtype, device=values.device).view(-1, 1)

    delta = values_flat[v1] - values_flat[v0]
    contrib = weights * delta
    accumulation_flat.index_add_(0, v0, contrib)
    accumulation_flat.index_add_(0, v1, -contrib)

    accumulation = accumulation_flat.reshape(n_points, *value_shape)
    return normalize_cotan_accumulation(accumulation, dual_volumes)
