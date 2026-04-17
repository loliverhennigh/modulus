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

"""Shared synthetic-mesh utilities for geometry functional benchmarks."""

from __future__ import annotations

import torch


def make_uv_sphere_mesh(
    *,
    device: torch.device,
    subdivisions: int,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Build a UV-sphere mesh and return ``(vertices, flattened_indices)``.

    Parameters
    ----------
    device : torch.device
        Target device for the generated tensors.
    subdivisions : int
        Resolution level controlling the number of latitude/longitude samples.

    Returns
    -------
    tuple[torch.Tensor, torch.Tensor]
        ``mesh_vertices`` with shape ``(n_vertices, 3)`` and dtype
        ``torch.float32``, and flattened triangle indices with shape
        ``(3 * n_faces,)`` and dtype ``torch.int32``.
    """

    n_rings = 4 * (2**subdivisions)
    n_segments = 8 * (2**subdivisions)

    phi = torch.linspace(0.0, torch.pi, n_rings + 2, device=device)[1:-1]
    theta = torch.linspace(0.0, 2.0 * torch.pi, n_segments + 1, device=device)[:-1]
    phi_g, theta_g = torch.meshgrid(phi, theta, indexing="ij")

    sin_phi = phi_g.sin()
    ring_points = torch.stack(
        [sin_phi * theta_g.cos(), sin_phi * theta_g.sin(), phi_g.cos()],
        dim=-1,
    ).reshape(-1, 3)

    mesh_vertices = torch.cat(
        [
            torch.tensor([[0.0, 0.0, 1.0]], device=device),
            ring_points,
            torch.tensor([[0.0, 0.0, -1.0]], device=device),
        ]
    ).to(torch.float32)

    south_idx = n_rings * n_segments + 1
    j = torch.arange(n_segments, device=device)
    j_next = (j + 1) % n_segments

    north_fan = torch.stack([torch.zeros_like(j), 1 + j, 1 + j_next], dim=1)

    r = torch.arange(n_rings - 1, device=device).unsqueeze(1)
    base = 1 + r * n_segments
    p00 = base + j
    p01 = base + j_next
    p10 = base + n_segments + j
    p11 = base + n_segments + j_next
    body_tris = torch.stack(
        [
            torch.stack([p00, p10, p11], dim=-1),
            torch.stack([p00, p11, p01], dim=-1),
        ],
        dim=2,
    ).reshape(-1, 3)

    last = south_idx - n_segments
    south_fan = torch.stack(
        [last + j, torch.full_like(j, south_idx), last + j_next], dim=1
    )

    mesh_indices = (
        torch.cat([north_fan, body_tris, south_fan]).to(torch.int32).reshape(-1)
    )

    return mesh_vertices.contiguous(), mesh_indices.contiguous()
