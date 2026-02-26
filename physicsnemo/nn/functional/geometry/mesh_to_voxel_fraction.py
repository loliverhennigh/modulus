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

from collections.abc import Sequence

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

wp.init()
wp.config.quiet = True


# Kernel for closed/watertight meshes using sign-normal point queries.
@wp.kernel
def _voxel_mesh_intersection_kernel(
    mesh_id: wp.uint64,
    origin: wp.vec3f,
    voxel_size: wp.float32,
    nx: int,
    ny: int,
    nz: int,
    n_samples: int,
    seed_offset: int,
    output: wp.array(dtype=wp.float32),
):
    # Compute voxel coordinates from launch index.
    i, j, k = wp.tid()
    if i >= nx or j >= ny or k >= nz:
        return

    # Compute flattened output index for this voxel.
    output_index = i + j * nx + k * nx * ny

    # Build voxel bounds and center in world coordinates.
    low = origin + wp.vec3f(
        wp.float32(i) * voxel_size,
        wp.float32(j) * voxel_size,
        wp.float32(k) * voxel_size,
    )
    high = low + wp.vec3f(voxel_size, voxel_size, voxel_size)
    center = (low + high) * wp.float32(0.5)

    # Query whether any triangles overlap this voxel AABB.
    query = wp.mesh_query_aabb(mesh_id, low, high)
    tri_index = wp.int32(0)

    # Use grid extent as a conservative distance scale for inside/outside queries.
    max_dim = wp.max(nx, wp.max(ny, nz))
    max_dist = voxel_size * wp.float32(max_dim)

    # Fast path: no triangle overlap -> classify voxel center only.
    if not wp.mesh_query_aabb_next(query, tri_index):
        hit = wp.mesh_query_point_sign_normal(mesh_id, center, max_dist, 1.0e-6)
        output[output_index] = wp.float32(1.0) if hit.result and hit.sign < 0.0 else 0.0
        return

    # Overlap path: estimate volume fraction with Monte Carlo samples.
    inside_count = wp.int32(0)
    rng_state = wp.rand_init(seed_offset + output_index)

    for _ in range(n_samples):
        rx = wp.randf(rng_state)
        ry = wp.randf(rng_state)
        rz = wp.randf(rng_state)
        sample = low + wp.vec3f(rx, ry, rz) * voxel_size
        hit = wp.mesh_query_point_sign_normal(mesh_id, sample, max_dist, 1.0e-6)
        if hit.result and hit.sign < 0.0:
            inside_count += 1

    output[output_index] = wp.float32(inside_count) / wp.float32(n_samples)


# Kernel for open meshes using sign-winding-number point queries.
@wp.kernel
def _voxel_open_mesh_intersection_kernel(
    mesh_id: wp.uint64,
    origin: wp.vec3f,
    voxel_size: wp.float32,
    nx: int,
    ny: int,
    nz: int,
    n_samples: int,
    seed_offset: int,
    winding_number_threshold: wp.float32,
    winding_number_accuracy: wp.float32,
    output: wp.array(dtype=wp.float32),
):
    # Compute voxel coordinates from launch index.
    i, j, k = wp.tid()
    if i >= nx or j >= ny or k >= nz:
        return

    # Compute flattened output index for this voxel.
    output_index = i + j * nx + k * nx * ny

    # Build voxel bounds and center in world coordinates.
    low = origin + wp.vec3f(
        wp.float32(i) * voxel_size,
        wp.float32(j) * voxel_size,
        wp.float32(k) * voxel_size,
    )
    high = low + wp.vec3f(voxel_size, voxel_size, voxel_size)
    center = (low + high) * wp.float32(0.5)

    # Query whether any triangles overlap this voxel AABB.
    query = wp.mesh_query_aabb(mesh_id, low, high)
    tri_index = wp.int32(0)

    # Use grid extent as a conservative distance scale for inside/outside queries.
    max_dim = wp.max(nx, wp.max(ny, nz))
    max_dist = voxel_size * wp.float32(max_dim)

    # Fast path: no triangle overlap -> classify voxel center only.
    if not wp.mesh_query_aabb_next(query, tri_index):
        hit = wp.mesh_query_point_sign_winding_number(
            mesh_id,
            center,
            max_dist,
            winding_number_accuracy,
            winding_number_threshold,
        )
        output[output_index] = wp.float32(1.0) if hit.result and hit.sign < 0.0 else 0.0
        return

    # Overlap path: estimate volume fraction with Monte Carlo samples.
    inside_count = wp.int32(0)
    rng_state = wp.rand_init(seed_offset + output_index)

    for _ in range(n_samples):
        rx = wp.randf(rng_state)
        ry = wp.randf(rng_state)
        rz = wp.randf(rng_state)
        sample = low + wp.vec3f(rx, ry, rz) * voxel_size
        hit = wp.mesh_query_point_sign_winding_number(
            mesh_id,
            sample,
            max_dist,
            winding_number_accuracy,
            winding_number_threshold,
        )
        if hit.result and hit.sign < 0.0:
            inside_count += 1

    output[output_index] = wp.float32(inside_count) / wp.float32(n_samples)


def _normalize_mesh_indices(
    mesh_indices: torch.Tensor,
    *,
    n_vertices: int | None = None,
) -> torch.Tensor:
    # Mesh connectivity must use an integer dtype.
    if mesh_indices.dtype not in {
        torch.int8,
        torch.int16,
        torch.int32,
        torch.int64,
        torch.uint8,
    }:
        raise TypeError("mesh_indices must use an integer dtype")

    # Accept either flattened indices or (n_faces, 3) connectivity.
    if mesh_indices.ndim == 2:
        if mesh_indices.shape[-1] != 3:
            raise ValueError("mesh_indices with rank 2 must have shape (n_faces, 3)")
        mesh_indices = mesh_indices.reshape(-1)
    elif mesh_indices.ndim != 1:
        raise ValueError(
            "mesh_indices must be either rank-1 flattened indices or rank-2 (n_faces, 3)"
        )

    # Flattened connectivity must contain complete triangle triplets.
    if mesh_indices.numel() == 0 or mesh_indices.numel() % 3 != 0:
        raise ValueError(
            "mesh_indices must contain a positive number of triangle-triplet indices"
        )

    # Validate index bounds when vertex count is provided.
    if n_vertices is not None:
        min_index = int(mesh_indices.min().item())
        max_index = int(mesh_indices.max().item())
        if min_index < 0 or max_index >= n_vertices:
            raise ValueError("mesh_indices values must satisfy 0 <= index < n_vertices")
    return mesh_indices


def _normalize_origin(
    origin: torch.Tensor | Sequence[float],
    *,
    device: torch.device,
) -> torch.Tensor:
    # Convert origin input to a float32 tensor on the target device.
    if torch.is_tensor(origin):
        origin_tensor = origin.to(device=device, dtype=torch.float32)
    else:
        origin_tensor = torch.tensor(origin, device=device, dtype=torch.float32)

    # Origin must be a length-3 coordinate vector.
    if origin_tensor.ndim != 1 or origin_tensor.numel() != 3:
        raise ValueError("origin must be a length-3 vector")
    return origin_tensor


def _normalize_grid_dims(
    grid_dims: torch.Tensor | Sequence[int],
) -> tuple[int, int, int]:
    # Convert grid dimensions to a Python integer triplet.
    if torch.is_tensor(grid_dims):
        if grid_dims.ndim != 1 or grid_dims.numel() != 3:
            raise ValueError("grid_dims must contain exactly three values")
        dims = (
            int(grid_dims[0].item()),
            int(grid_dims[1].item()),
            int(grid_dims[2].item()),
        )
    else:
        if len(grid_dims) != 3:
            raise ValueError("grid_dims must contain exactly three values")
        dims = (int(grid_dims[0]), int(grid_dims[1]), int(grid_dims[2]))

    # Grid resolution in each axis must be positive.
    if dims[0] <= 0 or dims[1] <= 0 or dims[2] <= 0:
        raise ValueError("grid_dims values must be strictly positive")
    return dims


@torch.library.custom_op("physicsnemo::mesh_to_voxel_fraction_warp", mutates_args=())
def mesh_to_voxel_fraction_impl(
    mesh_vertices: torch.Tensor,
    mesh_indices: torch.Tensor,
    origin: torch.Tensor,
    voxel_size: float,
    nx: int,
    ny: int,
    nz: int,
    n_samples: int = 64,
    seed: int = 42,
    open_mesh: bool = False,
    winding_number_threshold: float = 0.5,
    winding_number_accuracy: float = 2.0,
) -> torch.Tensor:
    # Validate mesh and parameter inputs.
    if mesh_vertices.device != mesh_indices.device:
        raise ValueError("mesh_vertices and mesh_indices must be on the same device")
    if mesh_vertices.device != origin.device:
        raise ValueError("mesh_vertices and origin must be on the same device")
    if mesh_vertices.ndim != 2 or mesh_vertices.shape[-1] != 3:
        raise ValueError("mesh_vertices must have shape (n_vertices, 3)")
    if mesh_indices.ndim != 1:
        raise ValueError("mesh_indices must be flattened (rank-1) in the custom op")
    if mesh_indices.numel() == 0 or mesh_indices.numel() % 3 != 0:
        raise ValueError(
            "mesh_indices must contain a positive number of triangle-triplet indices"
        )
    min_index = int(mesh_indices.min().item())
    max_index = int(mesh_indices.max().item())
    if min_index < 0 or max_index >= mesh_vertices.shape[0]:
        raise ValueError("mesh_indices values must satisfy 0 <= index < n_vertices")
    if origin.ndim != 1 or origin.numel() != 3:
        raise ValueError("origin must be a length-3 tensor")
    if voxel_size <= 0.0:
        raise ValueError("voxel_size must be strictly positive")
    if nx <= 0 or ny <= 0 or nz <= 0:
        raise ValueError("nx, ny, and nz must be strictly positive")
    if n_samples <= 0:
        raise ValueError("n_samples must be strictly positive")
    if winding_number_accuracy <= 0.0:
        raise ValueError("winding_number_accuracy must be strictly positive")

    # Normalize dtype/layout for Warp mesh and kernel launches.
    mesh_vertices = mesh_vertices.to(dtype=torch.float32).contiguous()
    mesh_indices = mesh_indices.to(dtype=torch.int32).contiguous()
    origin = origin.to(dtype=torch.float32).contiguous()

    # Allocate flattened output buffer and launch the appropriate kernel.
    output = torch.empty(nx * ny * nz, device=mesh_vertices.device, dtype=torch.float32)
    wp_launch_device, wp_launch_stream = FunctionSpec.warp_launch_context(mesh_vertices)

    with wp.ScopedStream(wp_launch_stream):
        wp_vertices = wp.from_torch(mesh_vertices, dtype=wp.vec3)
        wp_indices = wp.from_torch(mesh_indices, dtype=wp.int32)
        wp_output = wp.from_torch(output, return_ctype=True)

        mesh = wp.Mesh(
            points=wp_vertices,
            indices=wp_indices,
            support_winding_number=open_mesh,
        )
        origin_vec = wp.vec3f(
            float(origin[0].item()),
            float(origin[1].item()),
            float(origin[2].item()),
        )

        if open_mesh:
            wp.launch(
                kernel=_voxel_open_mesh_intersection_kernel,
                dim=(nx, ny, nz),
                inputs=[
                    mesh.id,
                    origin_vec,
                    float(voxel_size),
                    nx,
                    ny,
                    nz,
                    n_samples,
                    seed,
                    float(winding_number_threshold),
                    float(winding_number_accuracy),
                    wp_output,
                ],
                device=wp_launch_device,
                stream=wp_launch_stream,
            )
        else:
            wp.launch(
                kernel=_voxel_mesh_intersection_kernel,
                dim=(nx, ny, nz),
                inputs=[
                    mesh.id,
                    origin_vec,
                    float(voxel_size),
                    nx,
                    ny,
                    nz,
                    n_samples,
                    seed,
                    wp_output,
                ],
                device=wp_launch_device,
                stream=wp_launch_stream,
            )

    # Match the original voxelizer output convention: (nz, ny, nx).
    return output.reshape(nz, ny, nx)


@mesh_to_voxel_fraction_impl.register_fake
def mesh_to_voxel_fraction_impl_fake(
    mesh_vertices: torch.Tensor,
    mesh_indices: torch.Tensor,
    origin: torch.Tensor,
    voxel_size: float,
    nx: int,
    ny: int,
    nz: int,
    n_samples: int = 64,
    seed: int = 42,
    open_mesh: bool = False,
    winding_number_threshold: float = 0.5,
    winding_number_accuracy: float = 2.0,
) -> torch.Tensor:
    if mesh_vertices.device != mesh_indices.device:
        raise ValueError("mesh_vertices and mesh_indices must be on the same device")
    if mesh_vertices.device != origin.device:
        raise ValueError("mesh_vertices and origin must be on the same device")
    return torch.empty((nz, ny, nx), device=mesh_vertices.device, dtype=torch.float32)


class MeshToVoxelFraction(FunctionSpec):
    r"""Compute mesh-voxel volume fractions on a regular 3D grid.

    This functional estimates the fraction of each voxel that lies inside a
    triangle mesh using Warp kernels and Monte Carlo sampling.

    For each voxel, it first performs an AABB-overlap query with mesh triangles.
    If no triangles overlap the voxel, it classifies only the voxel center as
    inside or outside. If triangles overlap, it uniformly samples points inside
    the voxel and estimates the occupancy fraction:

    .. math::

       f_{ijk} \approx \frac{1}{N_s}\sum_{s=1}^{N_s}\mathbb{1}\left(x_s \in \Omega\right),

    where :math:`N_s` is ``n_samples`` and :math:`\Omega` is the mesh interior.

    Parameters
    ----------
    mesh_vertices : torch.Tensor
        Vertex positions with shape ``(n_vertices, 3)``.
    mesh_indices : torch.Tensor
        Triangle connectivity as shape ``(n_faces, 3)`` or flattened shape
        ``(3 * n_faces,)``.
    origin : torch.Tensor | Sequence[float]
        Lower corner of the voxel grid as a length-3 vector.
    voxel_size : float
        Edge length of each cubic voxel.
    grid_dims : Sequence[int]
        Grid resolution ``(nx, ny, nz)``.
    n_samples : int, optional
        Number of Monte Carlo samples per overlapping voxel. Default is ``64``.
    seed : int, optional
        Random seed offset used per voxel. Default is ``42``.
    open_mesh : bool, optional
        If ``True``, uses winding-number sign queries for open meshes.
        Default is ``False``.
    winding_number_threshold : float, optional
        Winding-number threshold used when ``open_mesh=True``.
    winding_number_accuracy : float, optional
        Winding-number query accuracy used when ``open_mesh=True``.
    implementation : str | None, optional
        Explicit backend selection. Defaults to dispatch behavior.

    Returns
    -------
    torch.Tensor
        Volume fractions in ``[0, 1]`` with shape ``(nz, ny, nx)`` and dtype
        ``torch.float32``.

    Notes
    -----
    - This functional provides a Warp implementation.
    - The operation is stochastic over overlapping voxels; use ``seed`` for
      reproducible runs.
    """

    _BENCHMARK_CASES = (
        ("small-subdiv2-64^3-s16", 2, 64, 16, False),
        ("medium-subdiv3-96^3-s32", 3, 96, 32, False),
        ("large-subdiv3-128^3-s64-open", 3, 128, 64, True),
    )

    @FunctionSpec.register(
        name="warp",
        required_imports=("warp>=0.6.0",),
        rank=0,
        baseline=True,
    )
    def warp_forward(
        mesh_vertices: torch.Tensor,
        mesh_indices: torch.Tensor,
        origin: torch.Tensor | Sequence[float],
        voxel_size: float,
        grid_dims: Sequence[int] | torch.Tensor,
        n_samples: int = 64,
        seed: int = 42,
        open_mesh: bool = False,
        winding_number_threshold: float = 0.5,
        winding_number_accuracy: float = 2.0,
    ) -> torch.Tensor:
        mesh_indices = _normalize_mesh_indices(
            mesh_indices,
            n_vertices=mesh_vertices.shape[0],
        )
        origin_tensor = _normalize_origin(origin, device=mesh_vertices.device)
        nx, ny, nz = _normalize_grid_dims(grid_dims)

        return mesh_to_voxel_fraction_impl(
            mesh_vertices=mesh_vertices,
            mesh_indices=mesh_indices,
            origin=origin_tensor,
            voxel_size=float(voxel_size),
            nx=nx,
            ny=ny,
            nz=nz,
            n_samples=int(n_samples),
            seed=int(seed),
            open_mesh=bool(open_mesh),
            winding_number_threshold=float(winding_number_threshold),
            winding_number_accuracy=float(winding_number_accuracy),
        )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        from physicsnemo.mesh.primitives.procedural.lumpy_sphere import (
            load as load_lumpy_sphere,
        )

        device = torch.device(device)

        # Build benchmark cases with increasing grid resolution/workload.
        for seed, (label, subdivisions, grid_n, n_samples, open_mesh) in enumerate(
            cls._BENCHMARK_CASES
        ):
            mesh = load_lumpy_sphere(subdivisions=subdivisions, device=str(device))
            mesh_vertices = mesh.points.to(torch.float32).contiguous()
            mesh_indices = mesh.cells.to(torch.int32).reshape(-1).contiguous()

            # Define a padded cubic domain around the mesh bounds.
            bbox_min = mesh_vertices.min(dim=0).values
            bbox_max = mesh_vertices.max(dim=0).values
            extent_value = float((bbox_max - bbox_min).amax().detach().cpu().item())
            extent_value = extent_value if extent_value > 0.0 else 1.0
            padding = 0.1 * extent_value
            origin = (bbox_min - padding).to(torch.float32).contiguous()
            voxel_size = (extent_value + 2.0 * padding) / float(grid_n)

            yield (
                label,
                (
                    mesh_vertices,
                    mesh_indices,
                    origin,
                    voxel_size,
                    (grid_n, grid_n, grid_n),
                ),
                {
                    "n_samples": n_samples,
                    "seed": 2026 + seed,
                    "open_mesh": open_mesh,
                    "winding_number_threshold": 0.5,
                    "winding_number_accuracy": 2.0,
                },
            )


mesh_to_voxel_fraction = MeshToVoxelFraction.make_function("mesh_to_voxel_fraction")


__all__ = ["MeshToVoxelFraction", "mesh_to_voxel_fraction"]
