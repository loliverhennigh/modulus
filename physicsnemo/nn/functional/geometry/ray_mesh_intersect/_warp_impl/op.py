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
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

wp.init()
wp.config.log_level = wp.LOG_WARNING


# ----------------------------------------------------------------------------
# Warp Kernels
# ----------------------------------------------------------------------------
@wp.func
def _normalize_or_zero(v: wp.vec3):
    length = wp.length(v)
    if length > 0.0:
        return v / length
    return wp.vec3(0.0, 0.0, 0.0)


@wp.kernel
def _ray_mesh_intersect_kernel(
    mesh_id: wp.uint64,
    ray_origins: wp.array(dtype=wp.vec3),
    ray_directions: wp.array(dtype=wp.vec3),
    max_distance: wp.float32,
    hit_mask: wp.array(dtype=wp.int32),
    hit_distance: wp.array(dtype=wp.float32),
    hit_points: wp.array(dtype=wp.vec3),
    face_ids: wp.array(dtype=wp.int32),
    hit_normals: wp.array(dtype=wp.vec3),
):
    tid = wp.tid()
    origin = ray_origins[tid]
    direction = ray_directions[tid]
    direction_length = wp.length(direction)

    if direction_length <= 0.0:
        hit_mask[tid] = 0
        hit_distance[tid] = 3.402823e38
        hit_points[tid] = wp.vec3(0.0, 0.0, 0.0)
        face_ids[tid] = -1
        hit_normals[tid] = wp.vec3(0.0, 0.0, 0.0)
        return

    ray_direction = direction / direction_length
    query = wp.mesh_query_ray(mesh_id, origin, ray_direction, max_distance)

    if not query.result:
        hit_mask[tid] = 0
        hit_distance[tid] = 3.402823e38
        hit_points[tid] = wp.vec3(0.0, 0.0, 0.0)
        face_ids[tid] = -1
        hit_normals[tid] = wp.vec3(0.0, 0.0, 0.0)
        return

    hit_mask[tid] = 1
    hit_distance[tid] = query.t
    hit_points[tid] = origin + query.t * ray_direction
    face_ids[tid] = query.face
    hit_normals[tid] = _normalize_or_zero(query.normal)


# ----------------------------------------------------------------------------
# Input Validation Helpers
# ----------------------------------------------------------------------------
def _check_floating_tensor(name: str, tensor: torch.Tensor) -> None:
    if tensor.dtype not in {
        torch.float16,
        torch.bfloat16,
        torch.float32,
        torch.float64,
    }:
        raise TypeError(f"{name} must use a floating dtype")


def _validate_and_flatten_mesh_indices(
    mesh_indices: torch.Tensor,
    *,
    n_vertices: int,
) -> torch.Tensor:
    if mesh_indices.dtype not in {
        torch.int8,
        torch.int16,
        torch.int32,
        torch.int64,
        torch.uint8,
    }:
        raise TypeError("mesh_indices must use an integer dtype")

    if mesh_indices.ndim == 2:
        if mesh_indices.shape[-1] != 3:
            raise ValueError("mesh_indices with rank 2 must have shape (n_faces, 3)")
        mesh_indices = mesh_indices.reshape(-1)
    elif mesh_indices.ndim != 1:
        raise ValueError(
            "mesh_indices must be either rank-1 flattened indices or rank-2 (n_faces, 3)"
        )

    if mesh_indices.numel() == 0 or mesh_indices.numel() % 3 != 0:
        raise ValueError(
            "mesh_indices must contain a positive number of triangle-triplet indices"
        )

    min_index = int(mesh_indices.min().item())
    max_index = int(mesh_indices.max().item())
    if min_index < 0 or max_index >= n_vertices:
        raise ValueError("mesh_indices values must satisfy 0 <= index < n_vertices")

    return mesh_indices


# ----------------------------------------------------------------------------
# Warp Custom Operator
# ----------------------------------------------------------------------------
@torch.library.custom_op("physicsnemo::ray_mesh_intersect_warp", mutates_args=())
def ray_mesh_intersect_impl(
    mesh_vertices: torch.Tensor,
    mesh_indices: torch.Tensor,
    ray_origins: torch.Tensor,
    ray_directions: torch.Tensor,
    max_distance: float = 1.0e8,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    """Launch Warp ray/mesh intersection queries."""
    if mesh_vertices.ndim != 2 or mesh_vertices.shape[-1] != 3:
        raise ValueError(
            "mesh_vertices must have shape (num_vertices, 3), got "
            f"{tuple(mesh_vertices.shape)}"
        )
    if mesh_vertices.shape[0] == 0:
        raise ValueError("mesh_vertices must contain at least one vertex")
    _check_floating_tensor("mesh_vertices", mesh_vertices)
    _check_floating_tensor("ray_origins", ray_origins)
    _check_floating_tensor("ray_directions", ray_directions)

    if ray_origins.ndim != 2 or ray_origins.shape[-1] != 3:
        raise ValueError("ray_origins must have shape (num_rays, 3)")
    if ray_directions.shape != ray_origins.shape:
        raise ValueError("ray_directions must have the same shape as ray_origins")
    if max_distance <= 0.0:
        raise ValueError("max_distance must be strictly positive")
    if mesh_indices.device != mesh_vertices.device:
        raise RuntimeError("mesh_vertices and mesh_indices must be on the same device")
    if ray_origins.device != mesh_vertices.device:
        raise RuntimeError("mesh_vertices and ray_origins must be on the same device")
    if ray_directions.device != ray_origins.device:
        raise RuntimeError("ray_origins and ray_directions must be on the same device")

    device = ray_origins.device
    mesh_vertices_fp32 = mesh_vertices.to(dtype=torch.float32).contiguous()
    mesh_indices_i32 = (
        _validate_and_flatten_mesh_indices(
            mesh_indices,
            n_vertices=mesh_vertices.shape[0],
        )
        .to(device=device, dtype=torch.int32)
        .contiguous()
    )
    ray_origins_fp32 = ray_origins.to(dtype=torch.float32).contiguous()
    ray_directions_fp32 = ray_directions.to(dtype=torch.float32).contiguous()

    num_rays = ray_origins_fp32.shape[0]
    hit_mask = torch.empty((num_rays,), device=device, dtype=torch.int32)
    hit_distance = torch.empty((num_rays,), device=device, dtype=torch.float32)
    hit_points = torch.empty((num_rays, 3), device=device, dtype=torch.float32)
    face_ids = torch.empty((num_rays,), device=device, dtype=torch.int32)
    hit_normals = torch.empty((num_rays, 3), device=device, dtype=torch.float32)

    if num_rays == 0:
        return hit_mask, hit_distance, hit_points, face_ids, hit_normals

    wp_device, wp_stream = FunctionSpec.warp_launch_context(ray_origins_fp32)
    with wp.ScopedStream(wp_stream):
        wp_vertices = wp.from_torch(mesh_vertices_fp32, dtype=wp.vec3)
        wp_indices = wp.from_torch(mesh_indices_i32, dtype=wp.int32)
        mesh = wp.Mesh(points=wp_vertices, indices=wp_indices)
        wp.launch(
            _ray_mesh_intersect_kernel,
            dim=num_rays,
            inputs=[
                mesh.id,
                wp.from_torch(ray_origins_fp32, dtype=wp.vec3),
                wp.from_torch(ray_directions_fp32, dtype=wp.vec3),
                float(max_distance),
            ],
            outputs=[
                wp.from_torch(hit_mask, dtype=wp.int32),
                wp.from_torch(hit_distance, dtype=wp.float32),
                wp.from_torch(hit_points, dtype=wp.vec3),
                wp.from_torch(face_ids, dtype=wp.int32),
                wp.from_torch(hit_normals, dtype=wp.vec3),
            ],
            device=wp_device,
            stream=wp_stream,
        )

    return hit_mask, hit_distance, hit_points, face_ids, hit_normals


@ray_mesh_intersect_impl.register_fake
def _(
    mesh_vertices: torch.Tensor,
    mesh_indices: torch.Tensor,
    ray_origins: torch.Tensor,
    ray_directions: torch.Tensor,
    max_distance: float = 1.0e8,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    num_rays = ray_origins.shape[0]
    device = ray_origins.device
    return (
        torch.empty((num_rays,), device=device, dtype=torch.int32),
        torch.empty((num_rays,), device=device, dtype=torch.float32),
        torch.empty((num_rays, 3), device=device, dtype=torch.float32),
        torch.empty((num_rays,), device=device, dtype=torch.int32),
        torch.empty((num_rays, 3), device=device, dtype=torch.float32),
    )


def ray_mesh_intersect_warp(
    mesh_vertices: torch.Tensor,
    mesh_indices: torch.Tensor,
    ray_origins: torch.Tensor,
    ray_directions: torch.Tensor,
    max_distance: float = 1.0e8,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    """Normalize inputs and execute the Warp ray/mesh intersection operator."""
    if ray_origins.shape != ray_directions.shape:
        raise ValueError("ray_directions must have the same shape as ray_origins")
    if ray_origins.ndim == 0 or ray_origins.shape[-1] != 3:
        raise ValueError("ray_origins must have shape (..., 3)")

    input_shape = ray_origins.shape
    output_shape = input_shape[:-1]
    output_dtype = ray_origins.dtype

    ray_origins_flat = ray_origins.reshape(-1, 3)
    ray_directions_flat = ray_directions.reshape(-1, 3)

    hit_mask_i32, hit_distance, hit_points, face_ids_i32, hit_normals = (
        ray_mesh_intersect_impl(
            mesh_vertices,
            mesh_indices,
            ray_origins_flat,
            ray_directions_flat,
            max_distance,
        )
    )

    hit_mask = hit_mask_i32.reshape(output_shape).to(torch.bool)
    hit_distance = hit_distance.reshape(output_shape)
    hit_distance = torch.where(
        hit_mask,
        hit_distance,
        torch.full_like(hit_distance, torch.inf),
    ).to(output_dtype)
    hit_points = hit_points.reshape(input_shape).to(output_dtype)
    face_ids = face_ids_i32.reshape(output_shape).to(torch.int64)
    hit_normals = hit_normals.reshape(input_shape).to(output_dtype)

    return hit_mask, hit_distance, hit_points, face_ids, hit_normals


__all__ = ["ray_mesh_intersect_warp"]
