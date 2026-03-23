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

import math
from collections.abc import Sequence

import torch

from physicsnemo.core.function_spec import FunctionSpec

from ._warp_impl import point_cloud_ball_pivoting_warp


def _sample_oriented_sphere_points(
    num_points: int,
    *,
    device: torch.device,
    seed: int,
) -> tuple[torch.Tensor, torch.Tensor]:
    generator = torch.Generator(device=device)
    generator.manual_seed(seed)

    # Generate approximately uniform points on a unit sphere with oriented normals.
    u = torch.rand((num_points,), generator=generator, device=device, dtype=torch.float32)
    v = torch.rand((num_points,), generator=generator, device=device, dtype=torch.float32)
    theta = 2.0 * math.pi * u
    z = 2.0 * v - 1.0
    radial = torch.sqrt(torch.clamp(1.0 - z * z, min=0.0))

    x = radial * torch.cos(theta)
    y = radial * torch.sin(theta)
    points = torch.stack((x, y, z), dim=1).contiguous()

    # Add a small smooth perturbation to avoid overly regular neighborhoods.
    points = points + 0.05 * torch.stack(
        (
            torch.sin(3.0 * theta) * radial,
            torch.cos(2.0 * theta) * radial,
            0.5 * torch.sin(2.0 * theta),
        ),
        dim=1,
    )
    normals = torch.nn.functional.normalize(points, dim=1).contiguous()
    return points.contiguous(), normals.contiguous()


class PointCloudBallPivoting(FunctionSpec):
    r"""Reconstruct a mesh from oriented points with ball pivoting.

    This functional takes oriented point-cloud tensors and returns mesh tensors.
    The implementation follows Open3D-style ball-pivoting front expansion.

    Kernel summary
    --------------
    +-------------------------------------------+------------------------------------------+
    | Kernel                                    | Purpose                                  |
    +===========================================+==========================================+
    | ``_count_neighbors_within_radius``        | Counts per-point neighbors in hash grid  |
    +-------------------------------------------+------------------------------------------+
    | ``_write_neighbors_within_radius``        | Writes neighbor CSR adjacency            |
    +-------------------------------------------+------------------------------------------+

    Parameters
    ----------
    points : torch.Tensor
        Point positions with shape ``(n_points, 3)``.
    normals : torch.Tensor
        Point normals with shape ``(n_points, 3)``.
    radii : Sequence[float] | torch.Tensor
        Positive pivot radii, typically in increasing order.
    max_neighbors : int, optional
        Maximum stored neighbors per point for local candidate search.
    hash_grid_dim : int | Sequence[int] | None, optional
        Warp hash-grid dimensions. ``None`` chooses dimensions from bounds.
    max_triangles : int | None, optional
        Optional cap on the number of output triangles.
    front_mode : str, optional
        Front-expansion strategy:
        - ``"serial"``: Open3D-style near-serial edge-front expansion.
        - ``"batched"``: batched front-edge proposals with deterministic conflict resolution.
    front_batch_size : int, optional
        Number of front edges processed per batch when ``front_mode="batched"``.
    implementation : str | None, optional
        Explicit backend selection. Defaults to dispatch behavior.

    Returns
    -------
    tuple[torch.Tensor, torch.Tensor]
        ``(vertices, triangles)`` where:
        - ``vertices`` has shape ``(n_points, 3)`` and dtype ``torch.float32``
        - ``triangles`` has shape ``(n_triangles, 3)`` and dtype ``torch.int32``
    """

    _BENCHMARK_CASES = (
        ("small-n2048-k96", 2048, 96, (0.08, 0.12)),
        ("medium-n4096-k96", 4096, 96, (0.06, 0.10, 0.14)),
        ("large-n8192-k128", 8192, 128, (0.05, 0.08, 0.12)),
    )

    @FunctionSpec.register(
        name="warp",
        required_imports=("warp>=0.6.0",),
        rank=0,
        baseline=True,
    )
    def warp_forward(
        points: torch.Tensor,
        normals: torch.Tensor,
        radii: Sequence[float] | torch.Tensor,
        max_neighbors: int = 128,
        hash_grid_dim: int | Sequence[int] | None = None,
        max_triangles: int | None = None,
        front_mode: str = "serial",
        front_batch_size: int = 16,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        return point_cloud_ball_pivoting_warp(
            points=points,
            normals=normals,
            radii=radii,
            max_neighbors=max_neighbors,
            hash_grid_dim=hash_grid_dim,
            max_triangles=max_triangles,
            front_mode=front_mode,
            front_batch_size=front_batch_size,
        )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)

        # Build benchmark cases with increasing point count/workload.
        for case_idx, (label, num_points, max_neighbors, radii) in enumerate(cls._BENCHMARK_CASES):
            points, normals = _sample_oriented_sphere_points(
                num_points,
                device=device,
                seed=2026 + case_idx,
            )
            yield (
                label,
                (points, normals, radii),
                {
                    "max_neighbors": max_neighbors,
                    "hash_grid_dim": None,
                    "max_triangles": None,
                    "front_mode": "serial",
                    "front_batch_size": 16,
                },
            )


point_cloud_ball_pivoting = PointCloudBallPivoting.make_function("point_cloud_ball_pivoting")


__all__ = ["PointCloudBallPivoting", "point_cloud_ball_pivoting"]
