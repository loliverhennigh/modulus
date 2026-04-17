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

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec
from physicsnemo.nn.functional.geometry._benchmark_utils import make_uv_sphere_mesh

# Warp is a required dependency in v2.0+.

wp.config.quiet = True


@wp.kernel
def _bvh_query_distance(
    mesh_id: wp.uint64,
    points: wp.array(dtype=wp.vec3f),
    max_dist: wp.float32,
    sdf: wp.array(dtype=wp.float32),
    sdf_hit_point: wp.array(dtype=wp.vec3f),
    use_sign_winding_number: bool = False,
):
    """Compute signed distance and closest hit-point for each query point.

    Parameters
    ----------
    mesh_id : wp.uint64
        Identifier of the query mesh.
    points : wp.array
        Array of query points with dtype ``wp.vec3f``.
    max_dist : wp.float32
        Maximum closest-point search distance.
    sdf : wp.array
        Output signed-distance array.
    sdf_hit_point : wp.array
        Output closest-point array.
    use_sign_winding_number : bool, optional
        Whether to query sign via winding-number or normal-sign convention.
    """
    tid = wp.tid()

    if use_sign_winding_number:
        res = wp.mesh_query_point_sign_winding_number(mesh_id, points[tid], max_dist)
    else:
        res = wp.mesh_query_point_sign_normal(mesh_id, points[tid], max_dist)

    # No hit inside max_dist: return a clipped positive distance and self hit-point.
    if not res.result:
        sdf[tid] = max_dist
        sdf_hit_point[tid] = points[tid]
        return

    mesh = wp.mesh_get(mesh_id)

    p0 = mesh.points[mesh.indices[3 * res.face + 0]]
    p1 = mesh.points[mesh.indices[3 * res.face + 1]]
    p2 = mesh.points[mesh.indices[3 * res.face + 2]]

    p_closest = res.u * p0 + res.v * p1 + (1.0 - res.u - res.v) * p2

    sdf[tid] = res.sign * wp.abs(wp.length(points[tid] - p_closest))
    sdf_hit_point[tid] = p_closest


@torch.library.custom_op("physicsnemo::signed_distance_field", mutates_args=())
def signed_distance_field_impl(
    mesh_vertices: torch.Tensor,
    mesh_indices: torch.Tensor,
    input_points: torch.Tensor,
    max_dist: float = 1e8,
    use_sign_winding_number: bool = False,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Compute signed distances and closest points for query points on a mesh.

    Parameters
    ----------
    mesh_vertices : torch.Tensor
        Mesh vertices with shape ``(n_vertices, 3)``.
    mesh_indices : torch.Tensor
        Triangle connectivity in shape ``(n_faces, 3)`` or flattened shape
        ``(3 * n_faces,)``.
    input_points : torch.Tensor
        Query points with shape ``(..., 3)``.
    max_dist : float, optional
        Maximum closest-point search distance. No-hit queries return ``max_dist``.
    use_sign_winding_number : bool, optional
        Whether to use winding-number sign queries instead of normal-sign queries.

    Returns
    -------
    tuple[torch.Tensor, torch.Tensor]
        ``(sdf, hit_points)`` where ``sdf`` has shape ``input_points.shape[:-1]``
        and ``hit_points`` has shape ``input_points.shape``.
    """

    if input_points.shape[-1] != 3:
        raise ValueError("input_points must have last dimension of size 3")
    if not torch.is_floating_point(input_points):
        raise TypeError("input_points must be floating-point")

    # Accept either flattened indices or face-triplet connectivity.
    if mesh_indices.ndim == 2:
        if mesh_indices.shape[-1] != 3:
            raise ValueError(
                "mesh_indices with 2 dimensions must have shape (n_faces, 3)"
            )
        mesh_indices = mesh_indices.reshape(-1)
    elif mesh_indices.ndim != 1:
        raise ValueError(
            "mesh_indices must be either 1D flattened indices or 2D (n_faces, 3)"
        )

    input_shape = input_points.shape
    input_dtype = input_points.dtype

    # Flatten the input points:
    input_points = input_points.reshape(-1, 3)

    N = len(input_points)

    # Allocate output tensors with torch:
    sdf = torch.zeros(N, dtype=torch.float32, device=input_points.device)
    sdf_hit_point = torch.zeros(N, 3, dtype=torch.float32, device=input_points.device)

    wp_launch_device, wp_launch_stream = FunctionSpec.warp_launch_context(input_points)

    with wp.ScopedStream(wp_launch_stream):
        wp.init()

        # zero copy the vertices, indices, and input points to warp:
        wp_vertices = wp.from_torch(mesh_vertices.to(torch.float32), dtype=wp.vec3)
        wp_indices = wp.from_torch(
            mesh_indices.to(torch.int32).contiguous(), dtype=wp.int32
        )
        wp_input_points = wp.from_torch(input_points.to(torch.float32), dtype=wp.vec3)

        # Convert output points:
        wp_sdf = wp.from_torch(sdf, dtype=wp.float32)
        wp_sdf_hit_point = wp.from_torch(sdf_hit_point, dtype=wp.vec3f)

        mesh = wp.Mesh(
            points=wp_vertices,
            indices=wp_indices,
            support_winding_number=use_sign_winding_number,
        )

        wp.launch(
            kernel=_bvh_query_distance,
            dim=N,
            inputs=[
                mesh.id,
                wp_input_points,
                max_dist,
                wp_sdf,
                wp_sdf_hit_point,
                use_sign_winding_number,
            ],
            device=wp_launch_device,
            stream=wp_launch_stream,
        )

    # Unflatten the output to be like the input:
    sdf = sdf.reshape(input_shape[:-1])
    sdf_hit_point = sdf_hit_point.reshape(input_shape)

    return sdf.to(input_dtype), sdf_hit_point.to(input_dtype)


@signed_distance_field_impl.register_fake
def signed_distance_field_impl_fake(
    mesh_vertices: torch.Tensor,
    mesh_indices: torch.Tensor,
    input_points: torch.Tensor,
    max_dist: float = 1e8,
    use_sign_winding_number: bool = False,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Return meta tensors for tracing/compilation of the SDF custom op."""
    if mesh_vertices.device != input_points.device:
        raise RuntimeError("mesh_vertices and input_points must be on the same device")

    if mesh_vertices.device != mesh_indices.device:
        raise RuntimeError("mesh_vertices and mesh_indices must be on the same device")

    if input_points.shape[-1] != 3:
        raise ValueError("input_points must have last dimension of size 3")

    output_shape = input_points.shape[:-1]
    sdf_output = torch.empty(
        output_shape, device=input_points.device, dtype=input_points.dtype
    )
    sdf_hit_point_output = torch.empty(
        input_points.shape, device=input_points.device, dtype=input_points.dtype
    )

    return sdf_output, sdf_hit_point_output


class SignedDistanceField(FunctionSpec):
    """Compute the signed distance field (SDF) for a mesh and query points.

    The mesh must be a surface mesh consisting of triangles. This functional
    uses a Warp-backed implementation for accelerated execution.

    Parameters
    ----------
    mesh_vertices : torch.Tensor
        Coordinates of mesh vertices with shape ``(n_vertices, 3)``.
    mesh_indices : torch.Tensor
        Triangle connectivity indexing into ``mesh_vertices``. Expected shape is
        ``(n_faces, 3)`` or a flattened equivalent.
    input_points : torch.Tensor
        Query points at which to evaluate the signed distance, with shape
        ``(..., 3)``.
    max_dist : float, optional
        Maximum search distance for closest-point queries. Default is ``1e8``.
    use_sign_winding_number : bool, optional
        Whether to use winding-number-based sign computation. Default is
        ``False``. When ``False``, the mesh should be watertight for reliable
        signs.
    implementation : str, optional
        Explicit implementation name. Defaults to ``None``, which uses normal
        dispatch (currently the Warp implementation).

    Returns
    -------
    tuple[torch.Tensor, torch.Tensor]
        A tuple ``(sdf, hit_points)`` where:
        - ``sdf`` contains signed distances at each query point.
        - ``hit_points`` contains the closest point on the mesh for each query.

    Examples
    --------
    >>> mesh_vertices = torch.tensor(
    ...     [(0.0, 0.0, 0.0), (1.0, 0.0, 0.0), (0.0, 1.0, 0.0)]
    ... )
    >>> mesh_indices = torch.tensor([(0, 1, 2)])
    >>> input_points = torch.tensor([(0.5, 0.5, 0.5)])
    >>> sdf, hit_points = signed_distance_field(
    ...     mesh_vertices, mesh_indices, input_points
    ... )
    """

    _BENCHMARK_CASES = (
        ("small-subdiv2-q4096", 2, 4096),
        ("medium-subdiv3-q16384", 3, 16384),
        ("large-subdiv4-q65536", 4, 65536),
    )

    @FunctionSpec.register(
        name="warp", required_imports=("warp>=0.6.0",), rank=0, baseline=True
    )
    def warp_forward(
        mesh_vertices: torch.Tensor,
        mesh_indices: torch.Tensor,
        input_points: torch.Tensor,
        max_dist: float = 1e8,
        use_sign_winding_number: bool = False,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """Dispatch the Warp-backed SDF custom op."""
        return signed_distance_field_impl(
            mesh_vertices,
            mesh_indices,
            input_points,
            max_dist=max_dist,
            use_sign_winding_number=use_sign_winding_number,
        )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        """Build representative benchmark inputs across mesh/query scales."""

        device = torch.device(device)
        # Build benchmark cases with increasing sphere mesh resolution.
        for label, subdivisions, num_points in cls._BENCHMARK_CASES:
            mesh_vertices, mesh_indices = make_uv_sphere_mesh(
                device=device,
                subdivisions=subdivisions,
            )

            # Sample query points in a padded axis-aligned box around the surface.
            bbox_min = mesh_vertices.min(dim=0).values
            bbox_max = mesh_vertices.max(dim=0).values
            span = bbox_max - bbox_min
            padding = 0.25 * span.max()
            box_min = bbox_min - padding
            box_max = bbox_max + padding
            input_points = (
                torch.rand(num_points, 3, device=device) * (box_max - box_min) + box_min
            )

            yield (
                label,
                (mesh_vertices, mesh_indices, input_points),
                {"max_dist": 10.0, "use_sign_winding_number": False},
            )


signed_distance_field = SignedDistanceField.make_function("signed_distance_field")


__all__ = ["SignedDistanceField", "signed_distance_field"]
