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
import warnings
from collections.abc import Sequence

import numpy as np
import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

wp.init()
wp.config.quiet = True

_DART_THROWING_MODE = "dart_throwing"
_WEIGHTED_SAMPLE_ELIMINATION_MODE = "weighted_sample_elimination"
_VALID_MODES = {
    _DART_THROWING_MODE,
    _WEIGHTED_SAMPLE_ELIMINATION_MODE,
}
_WSE_DELETE_BATCH_SIZE_MIN = 8
_WSE_DELETE_BATCH_SIZE_MAX = 128
_WSE_DELETE_CANDIDATE_POOL_MIN = 1024
_WSE_DELETE_CANDIDATE_POOL_PER_DELETE = 64


# Search a sorted CDF to map a random value to a triangle index.
@wp.func
def _binary_search_cdf(cdf: wp.array(dtype=wp.float32), value: wp.float32) -> int:
    left = int(0)
    right = int(cdf.shape[0] - 1)

    while left < right:
        mid = (left + right) >> 1
        if cdf[mid] < value:
            left = mid + 1
        else:
            right = mid
    return left


# Generate uniform random barycentric coordinates via Turk's method.
@wp.func
def _uniform_barycentric_sample(u1: wp.float32, u2: wp.float32) -> wp.vec2f:
    sqrt_u1 = wp.sqrt(u1)
    return wp.vec2f(1.0 - sqrt_u1, u2 * sqrt_u1)


# Compute squared distance between two 3D points.
@wp.func
def _distance_squared(p1: wp.vec3f, p2: wp.vec3f) -> wp.float32:
    diff = p1 - p2
    return wp.dot(diff, diff)


# Check minimum-distance constraint between two points.
@wp.func
def _points_too_close(
    p1: wp.vec3f,
    p2: wp.vec3f,
    min_distance: wp.float32,
) -> bool:
    return _distance_squared(p1, p2) < (min_distance * min_distance)


# Generate candidate points from area-weighted random triangle samples.
@wp.kernel
def _generate_surface_candidates(
    triangle_vertices: wp.array(dtype=wp.vec3f),
    triangle_edge1: wp.array(dtype=wp.vec3f),
    triangle_edge2: wp.array(dtype=wp.vec3f),
    triangle_vertex_indices: wp.array(dtype=wp.int32),
    area_cdf: wp.array(dtype=wp.float32),
    per_vertex_radius: wp.array(dtype=wp.float32),
    constant_radius: wp.float32,
    seed_base: int,
    output_positions: wp.array(dtype=wp.vec3f),
    output_radii: wp.array(dtype=wp.float32),
    output_priorities: wp.array(dtype=wp.float32),
):
    candidate_idx = wp.tid()
    rng_state = wp.rand_init(seed_base, candidate_idx)

    # Sample one triangle from area CDF.
    random_value = wp.randf(rng_state)
    triangle_idx = _binary_search_cdf(area_cdf, random_value)

    # Sample one point uniformly over that triangle.
    u1 = wp.randf(rng_state)
    u2 = wp.randf(rng_state)
    bary = _uniform_barycentric_sample(u1, u2)
    bary_u = bary[0]
    bary_v = bary[1]
    bary_w = 1.0 - bary_u - bary_v

    point = (
        triangle_vertices[triangle_idx]
        + triangle_edge1[triangle_idx] * bary_v
        + triangle_edge2[triangle_idx] * bary_w
    )
    output_positions[candidate_idx] = point
    output_priorities[candidate_idx] = wp.randf(rng_state)

    # Use either constant radius or barycentric interpolation from vertices.
    if per_vertex_radius.shape[0] > 0:
        i0 = triangle_vertex_indices[triangle_idx * 3 + 0]
        i1 = triangle_vertex_indices[triangle_idx * 3 + 1]
        i2 = triangle_vertex_indices[triangle_idx * 3 + 2]
        radius = (
            bary_u * per_vertex_radius[i0]
            + bary_v * per_vertex_radius[i1]
            + bary_w * per_vertex_radius[i2]
        )
        output_radii[candidate_idx] = radius
    else:
        output_radii[candidate_idx] = constant_radius


# Reject candidates that conflict with already-accepted samples.
@wp.kernel
def _reject_candidates_vs_accepted(
    hashgrid_id: wp.uint64,
    candidate_positions: wp.array(dtype=wp.vec3f),
    candidate_radii: wp.array(dtype=wp.float32),
    candidate_alive: wp.array(dtype=wp.int32),
    accepted_positions: wp.array(dtype=wp.vec3f),
):
    candidate_idx = wp.tid()
    if candidate_alive[candidate_idx] == 0:
        return

    candidate_position = candidate_positions[candidate_idx]
    candidate_radius = candidate_radii[candidate_idx]

    neighbor_idx = int(0)
    query = wp.hash_grid_query(hashgrid_id, candidate_position, candidate_radius)
    while wp.hash_grid_query_next(query, neighbor_idx):
        if neighbor_idx < accepted_positions.shape[0]:
            accepted_position = accepted_positions[neighbor_idx]
            if _points_too_close(
                candidate_position, accepted_position, candidate_radius
            ):
                candidate_alive[candidate_idx] = 0
                return


# Resolve conflicts between candidate points with random-priority MIS.
@wp.kernel
def _resolve_candidate_conflicts(
    hashgrid_id: wp.uint64,
    candidate_positions: wp.array(dtype=wp.vec3f),
    candidate_radii: wp.array(dtype=wp.float32),
    candidate_priorities: wp.array(dtype=wp.float32),
    candidate_alive: wp.array(dtype=wp.int32),
):
    candidate_idx = wp.tid()
    if candidate_alive[candidate_idx] == 0:
        return

    candidate_position = candidate_positions[candidate_idx]
    candidate_radius = candidate_radii[candidate_idx]
    candidate_priority = candidate_priorities[candidate_idx]

    neighbor_idx = int(0)
    query = wp.hash_grid_query(hashgrid_id, candidate_position, candidate_radius)
    while wp.hash_grid_query_next(query, neighbor_idx):
        if neighbor_idx == candidate_idx:
            continue
        if neighbor_idx >= candidate_positions.shape[0]:
            continue
        if candidate_alive[neighbor_idx] == 0:
            continue

        neighbor_position = candidate_positions[neighbor_idx]
        neighbor_priority = candidate_priorities[neighbor_idx]
        min_radius = wp.min(candidate_radius, candidate_radii[neighbor_idx])

        # Keep the candidate with higher random priority (stable tiebreak).
        if _points_too_close(candidate_position, neighbor_position, min_radius):
            if neighbor_priority > candidate_priority or (
                neighbor_priority == candidate_priority and neighbor_idx > candidate_idx
            ):
                candidate_alive[candidate_idx] = 0
                return


# Commit surviving candidates into the accepted-sample arrays.
@wp.kernel
def _commit_accepted_candidates(
    candidate_positions: wp.array(dtype=wp.vec3f),
    candidate_radii: wp.array(dtype=wp.float32),
    candidate_alive: wp.array(dtype=wp.int32),
    accepted_positions: wp.array(dtype=wp.vec3f),
    accepted_radii: wp.array(dtype=wp.float32),
    accepted_count: wp.array(dtype=wp.int32),
):
    candidate_idx = wp.tid()
    if candidate_alive[candidate_idx] == 0:
        return

    accepted_idx = wp.atomic_add(accepted_count, 0, 1)
    if accepted_idx >= accepted_positions.shape[0]:
        return

    accepted_positions[accepted_idx] = candidate_positions[candidate_idx]
    accepted_radii[accepted_idx] = candidate_radii[candidate_idx]


# Compute Yuksel sample-elimination contribution for one pairwise distance.
@wp.func
def _wse_pair_weight(
    distance_squared: wp.float32,
    r_min: wp.float32,
    r_max: wp.float32,
    alpha: wp.float32,
) -> wp.float32:
    distance = wp.sqrt(distance_squared)
    if distance < r_min:
        distance = r_min
    value = 1.0 - distance / r_max
    if value <= 0.0:
        return wp.float32(0.0)
    return wp.pow(value, alpha)


# Count valid weighted-elimination neighbors for each sample (CSR row lengths).
@wp.kernel
def _count_wse_neighbors(
    hashgrid_id: wp.uint64,
    sample_positions: wp.array(dtype=wp.vec3f),
    r_max: wp.float32,
    output_counts: wp.array(dtype=wp.int32),
):
    sample_idx = wp.tid()
    center = sample_positions[sample_idx]
    radius_sq = r_max * r_max
    neighbor_count = int(0)
    neighbor_idx = int(0)
    query = wp.hash_grid_query(hashgrid_id, center, r_max)
    while wp.hash_grid_query_next(query, neighbor_idx):
        if neighbor_idx == sample_idx:
            continue
        if neighbor_idx >= sample_positions.shape[0]:
            continue
        d2 = _distance_squared(center, sample_positions[neighbor_idx])
        if d2 >= radius_sq:
            continue
        neighbor_count += 1
    output_counts[sample_idx] = neighbor_count


# Fill CSR adjacency and pair weights for weighted sample elimination.
@wp.kernel
def _write_wse_csr(
    hashgrid_id: wp.uint64,
    sample_positions: wp.array(dtype=wp.vec3f),
    row_ptr: wp.array(dtype=wp.int32),
    r_min: wp.float32,
    r_max: wp.float32,
    alpha: wp.float32,
    col_idx: wp.array(dtype=wp.int32),
    pair_weights: wp.array(dtype=wp.float32),
):
    sample_idx = wp.tid()
    center = sample_positions[sample_idx]
    radius_sq = r_max * r_max
    write_cursor = row_ptr[sample_idx]

    neighbor_idx = int(0)
    query = wp.hash_grid_query(hashgrid_id, center, r_max)
    while wp.hash_grid_query_next(query, neighbor_idx):
        if neighbor_idx == sample_idx:
            continue
        if neighbor_idx >= sample_positions.shape[0]:
            continue
        d2 = _distance_squared(center, sample_positions[neighbor_idx])
        if d2 >= radius_sq:
            continue

        col_idx[write_cursor] = neighbor_idx
        pair_weights[write_cursor] = _wse_pair_weight(d2, r_min, r_max, alpha)
        write_cursor += 1


# Initialize weighted-elimination scores from CSR pair weights.
@wp.kernel
def _initialize_wse_weights_from_csr(
    row_ptr: wp.array(dtype=wp.int32),
    pair_weights: wp.array(dtype=wp.float32),
    deleted: wp.array(dtype=wp.int32),
    output_weights: wp.array(dtype=wp.float32),
):
    sample_idx = wp.tid()
    if deleted[sample_idx] != 0:
        output_weights[sample_idx] = -1.0e30
        return

    row_start = row_ptr[sample_idx]
    row_end = row_ptr[sample_idx + 1]
    weight = wp.float32(0.0)
    for edge_idx in range(row_start, row_end):
        weight = weight + pair_weights[edge_idx]
    output_weights[sample_idx] = weight


# Subtract deleted-sample contributions for a whole batch from CSR neighbors.
@wp.kernel
def _subtract_deleted_wse_contribution_batch_csr(
    row_ptr: wp.array(dtype=wp.int32),
    col_idx: wp.array(dtype=wp.int32),
    pair_weights: wp.array(dtype=wp.float32),
    deleted_batch: wp.array(dtype=wp.int32),
    batch_count: int,
    max_row_size: int,
    deleted: wp.array(dtype=wp.int32),
    weights: wp.array(dtype=wp.float32),
):
    tid = wp.tid()
    if max_row_size <= 0:
        return

    batch_slot = tid // max_row_size
    local_edge_idx = tid - batch_slot * max_row_size
    if batch_slot >= batch_count:
        return

    deleted_index = deleted_batch[batch_slot]
    row_start = row_ptr[deleted_index]
    row_end = row_ptr[deleted_index + 1]
    edge_idx = row_start + local_edge_idx
    if edge_idx >= row_end:
        return

    neighbor_idx = col_idx[edge_idx]
    if deleted[neighbor_idx] != 0:
        return
    # Multiple deleted nodes can share neighbors, so use atomic accumulation.
    wp.atomic_add(weights, neighbor_idx, -pair_weights[edge_idx])


# Mark a batch of samples as deleted and set their weights to -inf.
@wp.kernel
def _mark_wse_deleted_batch(
    deleted_batch: wp.array(dtype=wp.int32),
    batch_count: int,
    deleted: wp.array(dtype=wp.int32),
    weights: wp.array(dtype=wp.float32),
    neg_inf: wp.float32,
):
    tid = wp.tid()
    if tid >= batch_count:
        return
    sample_idx = deleted_batch[tid]
    deleted[sample_idx] = 1
    weights[sample_idx] = neg_inf


def _normalize_mesh_indices(
    mesh_indices: torch.Tensor,
    *,
    n_vertices: int | None = None,
) -> torch.Tensor:
    # Mesh connectivity must use integer dtype.
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


def _normalize_hash_grid_resolution(
    hash_grid_resolution: int | Sequence[int] | torch.Tensor,
) -> tuple[int, int, int]:
    # Accept scalar or explicit 3D grid resolution.
    if isinstance(hash_grid_resolution, int):
        resolution = (
            int(hash_grid_resolution),
            int(hash_grid_resolution),
            int(hash_grid_resolution),
        )
    elif torch.is_tensor(hash_grid_resolution):
        if hash_grid_resolution.ndim != 1 or hash_grid_resolution.numel() != 3:
            raise ValueError("hash_grid_resolution tensor must have exactly 3 elements")
        resolution = (
            int(hash_grid_resolution[0].item()),
            int(hash_grid_resolution[1].item()),
            int(hash_grid_resolution[2].item()),
        )
    else:
        if len(hash_grid_resolution) != 3:
            raise ValueError("hash_grid_resolution must contain exactly 3 values")
        resolution = (
            int(hash_grid_resolution[0]),
            int(hash_grid_resolution[1]),
            int(hash_grid_resolution[2]),
        )

    # Resolution values must be positive.
    if resolution[0] <= 0 or resolution[1] <= 0 or resolution[2] <= 0:
        raise ValueError("hash_grid_resolution values must be strictly positive")
    return resolution


# Compute an adaptive batch target for weighted sample elimination.
def _wse_target_batch_size(*, delete_count: int, steps_done: int) -> int:
    remaining = max(delete_count - steps_done, 0)
    if remaining <= 0:
        return 0

    # Start with larger batches and taper down as the pool shrinks.
    remaining_fraction = float(remaining) / float(delete_count)
    scheduled = _WSE_DELETE_BATCH_SIZE_MIN + int(
        math.ceil(
            (_WSE_DELETE_BATCH_SIZE_MAX - _WSE_DELETE_BATCH_SIZE_MIN)
            * remaining_fraction
        )
    )
    return max(
        1,
        min(
            remaining,
            max(_WSE_DELETE_BATCH_SIZE_MIN, scheduled),
        ),
    )


def _normalize_per_vertex_radius(
    per_vertex_radius: torch.Tensor | None,
    *,
    n_vertices: int,
    device: torch.device,
) -> torch.Tensor:
    # Missing adaptive radius input means constant-radius mode.
    if per_vertex_radius is None:
        return torch.empty(0, device=device, dtype=torch.float32)

    # Validate shape and dtype for adaptive radii.
    if per_vertex_radius.ndim != 1:
        raise ValueError("per_vertex_radius must be rank-1 with shape (n_vertices,)")
    if per_vertex_radius.shape[0] != n_vertices:
        raise ValueError("per_vertex_radius must have shape (n_vertices,)")
    if per_vertex_radius.dtype not in {
        torch.float16,
        torch.bfloat16,
        torch.float32,
        torch.float64,
    }:
        raise TypeError("per_vertex_radius must use a floating dtype")

    per_vertex_radius = per_vertex_radius.to(device=device, dtype=torch.float32)
    if float(per_vertex_radius.min().item()) <= 0.0:
        raise ValueError("per_vertex_radius values must be strictly positive")
    return per_vertex_radius.contiguous()


def _create_hash_grid(
    *,
    points: torch.Tensor,
    search_radius: float,
    resolution: tuple[int, int, int],
    device: str,
) -> wp.HashGrid:
    # Build a hash grid for neighbor queries on the provided points.
    hash_grid = wp.HashGrid(
        dim_x=resolution[0],
        dim_y=resolution[1],
        dim_z=resolution[2],
        device=device,
    )
    if points.shape[0] > 0:
        hash_grid.reserve(points.shape[0])
        # Match Warp guidance: use a build cell size close to query radius.
        hash_grid.build(
            points=wp.from_torch(points, dtype=wp.vec3f), radius=search_radius
        )
    return hash_grid


def _generate_uniform_surface_samples_warp(
    *,
    tri_vertices: torch.Tensor,
    tri_edge1: torch.Tensor,
    tri_edge2: torch.Tensor,
    mesh_indices: torch.Tensor,
    area_cdf: torch.Tensor,
    num_samples: int,
    random_seed: int,
) -> torch.Tensor:
    # Generate one oversampled uniform point set on the mesh surface.
    sample_positions = torch.empty(
        (num_samples, 3),
        device=tri_vertices.device,
        dtype=torch.float32,
    )
    sample_radii = torch.empty(
        (num_samples,),
        device=tri_vertices.device,
        dtype=torch.float32,
    )
    sample_priorities = torch.empty(
        (num_samples,),
        device=tri_vertices.device,
        dtype=torch.float32,
    )
    empty_radius = torch.empty(
        (0,),
        device=tri_vertices.device,
        dtype=torch.float32,
    )

    wp_launch_device, wp_launch_stream = FunctionSpec.warp_launch_context(tri_vertices)
    with wp.ScopedStream(wp_launch_stream):
        wp.launch(
            kernel=_generate_surface_candidates,
            dim=num_samples,
            inputs=[
                wp.from_torch(tri_vertices, dtype=wp.vec3f, return_ctype=True),
                wp.from_torch(tri_edge1, dtype=wp.vec3f, return_ctype=True),
                wp.from_torch(tri_edge2, dtype=wp.vec3f, return_ctype=True),
                wp.from_torch(mesh_indices, dtype=wp.int32, return_ctype=True),
                wp.from_torch(area_cdf, dtype=wp.float32, return_ctype=True),
                wp.from_torch(empty_radius, dtype=wp.float32, return_ctype=True),
                1.0,
                int(random_seed),
                wp.from_torch(sample_positions, dtype=wp.vec3f, return_ctype=True),
                wp.from_torch(sample_radii, dtype=wp.float32, return_ctype=True),
                wp.from_torch(sample_priorities, dtype=wp.float32, return_ctype=True),
            ],
            device=wp_launch_device,
            stream=wp_launch_stream,
        )
    return sample_positions.contiguous()


def _weighted_sample_elimination_warp(
    *,
    sample_positions: torch.Tensor,
    target_num_points: int,
    surface_area: float,
    hash_grid_resolution: tuple[int, int, int],
    verbose: bool = False,
) -> torch.Tensor:
    # Early return when no elimination is required.
    num_samples = sample_positions.shape[0]
    if target_num_points >= num_samples:
        return sample_positions.contiguous()

    # Match Open3D's Yuksel elimination constants.
    alpha = 8.0
    beta = 0.65
    gamma = 1.5
    ratio = float(target_num_points) / float(num_samples)
    r_max = 2.0 * math.sqrt(
        (surface_area / float(target_num_points)) / (2.0 * math.sqrt(3.0))
    )
    r_min = max(r_max * beta * (1.0 - math.pow(ratio, gamma)), 0.0)

    deleted = torch.zeros(
        (num_samples,),
        device=sample_positions.device,
        dtype=torch.int32,
    )
    weights = torch.empty(
        (num_samples,),
        device=sample_positions.device,
        dtype=torch.float32,
    )

    # Build one static neighborhood structure for elimination updates.
    sample_grid = _create_hash_grid(
        points=sample_positions,
        search_radius=r_max,
        resolution=hash_grid_resolution,
        device=str(sample_positions.device),
    )

    wp_launch_device, wp_launch_stream = FunctionSpec.warp_launch_context(
        sample_positions
    )
    with wp.ScopedStream(wp_launch_stream):
        wp_sample_positions = wp.from_torch(
            sample_positions,
            dtype=wp.vec3f,
            return_ctype=True,
        )
        wp_deleted = wp.from_torch(
            deleted,
            dtype=wp.int32,
            return_ctype=True,
        )
        wp_weights = wp.from_torch(
            weights,
            dtype=wp.float32,
            return_ctype=True,
        )

        # Build weighted-elimination CSR neighbors once.
        neighbor_counts = torch.empty(
            (num_samples,),
            device=sample_positions.device,
            dtype=torch.int32,
        )
        row_ptr = torch.empty(
            (num_samples + 1,),
            device=sample_positions.device,
            dtype=torch.int32,
        )
        wp_neighbor_counts = wp.from_torch(
            neighbor_counts,
            dtype=wp.int32,
            return_ctype=True,
        )
        wp.launch(
            kernel=_count_wse_neighbors,
            dim=num_samples,
            inputs=[
                sample_grid.id,
                wp_sample_positions,
                float(r_max),
                wp_neighbor_counts,
            ],
            device=wp_launch_device,
            stream=wp_launch_stream,
        )
        row_ptr[0] = 0
        torch.cumsum(neighbor_counts, dim=0, out=row_ptr[1:])
        total_edges = int(row_ptr[-1].item())
        max_row_size = int(neighbor_counts.max().item()) if num_samples > 0 else 0
        row_ptr_cpu = row_ptr.detach().cpu().numpy()

        col_idx = torch.empty(
            (total_edges,),
            device=sample_positions.device,
            dtype=torch.int32,
        )
        pair_weights = torch.empty(
            (total_edges,),
            device=sample_positions.device,
            dtype=torch.float32,
        )
        wp_row_ptr = wp.from_torch(
            row_ptr,
            dtype=wp.int32,
            return_ctype=True,
        )
        wp_col_idx = wp.from_torch(
            col_idx,
            dtype=wp.int32,
            return_ctype=True,
        )
        wp_pair_weights = wp.from_torch(
            pair_weights,
            dtype=wp.float32,
            return_ctype=True,
        )
        if total_edges > 0:
            wp.launch(
                kernel=_write_wse_csr,
                dim=num_samples,
                inputs=[
                    sample_grid.id,
                    wp_sample_positions,
                    wp_row_ptr,
                    float(r_min),
                    float(r_max),
                    float(alpha),
                    wp_col_idx,
                    wp_pair_weights,
                ],
                device=wp_launch_device,
                stream=wp_launch_stream,
            )
        col_idx_cpu = col_idx.detach().cpu().numpy()

        # Initialize all sample weights from CSR pair sums.
        wp.launch(
            kernel=_initialize_wse_weights_from_csr,
            dim=num_samples,
            inputs=[
                wp_row_ptr,
                wp_pair_weights,
                wp_deleted,
                wp_weights,
            ],
            device=wp_launch_device,
            stream=wp_launch_stream,
        )

        # Remove highest-weight samples until target size is reached.
        delete_count = num_samples - target_num_points
        deleted_batch = torch.empty(
            (_WSE_DELETE_BATCH_SIZE_MAX,),
            device=sample_positions.device,
            dtype=torch.int32,
        )
        wp_deleted_batch = wp.from_torch(
            deleted_batch,
            dtype=wp.int32,
            return_ctype=True,
        )
        selected_batch_host = torch.empty(
            (_WSE_DELETE_BATCH_SIZE_MAX,),
            dtype=torch.int32,
            pin_memory=sample_positions.is_cuda,
        )
        selected_batch_host_np = selected_batch_host.numpy()
        deleted_cpu = np.zeros((num_samples,), dtype=np.uint8)
        blocked_epoch = np.zeros((num_samples,), dtype=np.int32)
        current_epoch = 1
        neg_inf = -1.0e30
        steps_done = 0
        while steps_done < delete_count:
            # Select a high-weight candidate pool, then greedily form an independent batch.
            target_batch = _wse_target_batch_size(
                delete_count=delete_count,
                steps_done=steps_done,
            )
            pool_k = min(
                num_samples,
                max(
                    _WSE_DELETE_CANDIDATE_POOL_MIN,
                    target_batch * _WSE_DELETE_CANDIDATE_POOL_PER_DELETE,
                ),
            )
            candidate_indices = (
                torch.topk(weights, k=pool_k, largest=True, sorted=True)
                .indices.detach()
                .cpu()
                .numpy()
            )

            current_epoch += 1
            if current_epoch >= np.iinfo(np.int32).max:
                blocked_epoch.fill(0)
                current_epoch = 1

            batch_count = 0
            for candidate_idx in candidate_indices:
                candidate_idx = int(candidate_idx)
                if deleted_cpu[candidate_idx] != 0:
                    continue
                if blocked_epoch[candidate_idx] == current_epoch:
                    continue

                selected_batch_host_np[batch_count] = candidate_idx
                batch_count += 1
                if batch_count >= target_batch:
                    break

                blocked_epoch[candidate_idx] = current_epoch
                start = int(row_ptr_cpu[candidate_idx])
                end = int(row_ptr_cpu[candidate_idx + 1])
                blocked_epoch[col_idx_cpu[start:end]] = current_epoch

            # Fallback guard: always delete at least one point.
            if batch_count == 0:
                selected_batch_host_np[0] = int(candidate_indices[0])
                batch_count = 1

            deleted_cpu[selected_batch_host_np[:batch_count]] = 1
            deleted_batch[:batch_count].copy_(
                selected_batch_host[:batch_count],
                non_blocking=sample_positions.is_cuda,
            )
            wp.launch(
                kernel=_mark_wse_deleted_batch,
                dim=batch_count,
                inputs=[
                    wp_deleted_batch,
                    int(batch_count),
                    wp_deleted,
                    wp_weights,
                    float(neg_inf),
                ],
                device=wp_launch_device,
                stream=wp_launch_stream,
            )

            if max_row_size > 0:
                wp.launch(
                    kernel=_subtract_deleted_wse_contribution_batch_csr,
                    dim=batch_count * max_row_size,
                    inputs=[
                        wp_row_ptr,
                        wp_col_idx,
                        wp_pair_weights,
                        wp_deleted_batch,
                        int(batch_count),
                        int(max_row_size),
                        wp_deleted,
                        wp_weights,
                    ],
                    device=wp_launch_device,
                    stream=wp_launch_stream,
                )

            steps_done += batch_count
            if verbose and (steps_done % 256 == 0 or steps_done >= delete_count):
                print(
                    f"weighted elimination progress: {steps_done}/{delete_count} deletions"
                )

    kept_indices = torch.nonzero(deleted == 0, as_tuple=False).squeeze(1)
    if kept_indices.numel() > target_num_points:
        kept_indices = kept_indices[:target_num_points]
    return sample_positions.index_select(0, kept_indices).contiguous()


def _mesh_poisson_disk_sample_warp(
    mesh_vertices: torch.Tensor,
    mesh_indices: torch.Tensor,
    min_distance: float,
    per_vertex_radius: torch.Tensor | None = None,
    batch_size: int = 131072,
    max_points: int = 2_000_000,
    max_iterations: int = 64,
    verbose: bool = False,
    random_seed: int = 42,
    open3d_init_factor: int = 5,
    hash_grid_resolution: int | Sequence[int] | torch.Tensor = 128,
    mode: str = _DART_THROWING_MODE,
    target_num_points: int | None = None,
) -> torch.Tensor:
    # Validate mesh and scalar arguments.
    if mesh_vertices.ndim != 2 or mesh_vertices.shape[-1] != 3:
        raise ValueError("mesh_vertices must have shape (n_vertices, 3)")
    if min_distance <= 0.0:
        raise ValueError("min_distance must be strictly positive")
    if batch_size <= 0:
        raise ValueError("batch_size must be strictly positive")
    if max_points <= 0:
        raise ValueError("max_points must be strictly positive")
    if max_iterations <= 0:
        raise ValueError("max_iterations must be strictly positive")
    if open3d_init_factor <= 0:
        raise ValueError("open3d_init_factor must be strictly positive")

    # Normalize the Poisson sampling mode.
    if mode not in _VALID_MODES:
        raise ValueError(
            "mode must be one of {'dart_throwing', 'weighted_sample_elimination'}"
        )

    mesh_vertices = mesh_vertices.to(dtype=torch.float32).contiguous()
    mesh_indices = _normalize_mesh_indices(
        mesh_indices,
        n_vertices=mesh_vertices.shape[0],
    ).to(device=mesh_vertices.device, dtype=torch.int32, copy=False)
    per_vertex_radius = _normalize_per_vertex_radius(
        per_vertex_radius,
        n_vertices=mesh_vertices.shape[0],
        device=mesh_vertices.device,
    )
    grid_resolution = _normalize_hash_grid_resolution(hash_grid_resolution)

    # Build area-weighted triangle CDF and triangle geometry tensors.
    tri = mesh_indices.reshape(-1, 3).to(torch.long)
    p0 = mesh_vertices[tri[:, 0]]
    p1 = mesh_vertices[tri[:, 1]]
    p2 = mesh_vertices[tri[:, 2]]
    tri_edge1 = (p1 - p0).contiguous()
    tri_edge2 = (p2 - p0).contiguous()
    tri_vertices = p0.contiguous()

    areas = 0.5 * torch.linalg.norm(torch.cross(tri_edge1, tri_edge2, dim=1), dim=1)
    total_area = float(areas.sum().item())
    if total_area <= 0.0:
        raise ValueError("mesh triangle areas must sum to a positive value")
    area_cdf = (torch.cumsum(areas, dim=0) / total_area).to(torch.float32).contiguous()

    # Weighted elimination mode: oversample uniformly then run Warp elimination.
    if mode == _WEIGHTED_SAMPLE_ELIMINATION_MODE:
        if target_num_points is None:
            target_num_points = max_points
        if target_num_points <= 0:
            raise ValueError("target_num_points must be strictly positive")

        # Match Open3D's initialization behavior: init_factor * target samples.
        pool_target = max(
            target_num_points + 1,
            int(round(target_num_points * open3d_init_factor)),
        )

        if per_vertex_radius.numel() > 0:
            warnings.warn(
                "per_vertex_radius is ignored in weighted_sample_elimination mode",
                stacklevel=2,
            )

        pool_positions = _generate_uniform_surface_samples_warp(
            tri_vertices=tri_vertices,
            tri_edge1=tri_edge1,
            tri_edge2=tri_edge2,
            mesh_indices=mesh_indices,
            area_cdf=area_cdf,
            num_samples=pool_target,
            random_seed=random_seed,
        )
        output = _weighted_sample_elimination_warp(
            sample_positions=pool_positions,
            target_num_points=target_num_points,
            surface_area=total_area,
            hash_grid_resolution=grid_resolution,
            verbose=verbose,
        )
        if verbose:
            print(
                f"Weighted sample elimination selected {output.shape[0]} points "
                f"from {pool_target} uniform candidates"
            )
        return output

    # Allocate accepted/candidate buffers reused throughout dart throwing.
    accepted_positions = torch.empty(
        (max_points, 3),
        device=mesh_vertices.device,
        dtype=torch.float32,
    )
    accepted_radii = torch.empty(
        (max_points,), device=mesh_vertices.device, dtype=torch.float32
    )
    accepted_count = torch.zeros((1,), device=mesh_vertices.device, dtype=torch.int32)

    candidate_positions = torch.empty(
        (batch_size, 3),
        device=mesh_vertices.device,
        dtype=torch.float32,
    )
    candidate_radii = torch.empty(
        (batch_size,), device=mesh_vertices.device, dtype=torch.float32
    )
    candidate_priorities = torch.empty(
        (batch_size,),
        device=mesh_vertices.device,
        dtype=torch.float32,
    )
    candidate_alive = torch.ones(
        (batch_size,), device=mesh_vertices.device, dtype=torch.int32
    )

    # Cache adaptive-radius maximum once for dynamic search radius updates.
    adaptive_max_radius = (
        float(per_vertex_radius.max().item()) if per_vertex_radius.numel() > 0 else 0.0
    )

    wp_launch_device, wp_launch_stream = FunctionSpec.warp_launch_context(mesh_vertices)
    with wp.ScopedStream(wp_launch_stream):
        # Convert static input tensors once for repeated kernel launches.
        wp_triangle_vertices = wp.from_torch(
            tri_vertices, dtype=wp.vec3f, return_ctype=True
        )
        wp_triangle_edge1 = wp.from_torch(tri_edge1, dtype=wp.vec3f, return_ctype=True)
        wp_triangle_edge2 = wp.from_torch(tri_edge2, dtype=wp.vec3f, return_ctype=True)
        wp_triangle_vertex_indices = wp.from_torch(
            mesh_indices,
            dtype=wp.int32,
            return_ctype=True,
        )
        wp_area_cdf = wp.from_torch(area_cdf, dtype=wp.float32, return_ctype=True)
        wp_per_vertex_radius = wp.from_torch(
            per_vertex_radius,
            dtype=wp.float32,
            return_ctype=True,
        )

        # Convert mutable buffers reused each iteration.
        wp_candidate_positions = wp.from_torch(
            candidate_positions,
            dtype=wp.vec3f,
            return_ctype=True,
        )
        wp_candidate_radii = wp.from_torch(
            candidate_radii,
            dtype=wp.float32,
            return_ctype=True,
        )
        wp_candidate_priorities = wp.from_torch(
            candidate_priorities,
            dtype=wp.float32,
            return_ctype=True,
        )
        wp_candidate_alive = wp.from_torch(
            candidate_alive,
            dtype=wp.int32,
            return_ctype=True,
        )
        wp_accepted_positions = wp.from_torch(
            accepted_positions,
            dtype=wp.vec3f,
            return_ctype=True,
        )
        wp_accepted_radii = wp.from_torch(
            accepted_radii,
            dtype=wp.float32,
            return_ctype=True,
        )
        wp_accepted_count = wp.from_torch(
            accepted_count,
            dtype=wp.int32,
            return_ctype=True,
        )
        wp_candidate_positions_array = wp.from_torch(
            candidate_positions, dtype=wp.vec3f
        )

        # Reuse hash-grid objects across iterations to reduce object churn.
        accepted_grid = wp.HashGrid(
            dim_x=grid_resolution[0],
            dim_y=grid_resolution[1],
            dim_z=grid_resolution[2],
            device=str(mesh_vertices.device),
        )
        accepted_grid.reserve(max_points)
        candidate_grid = wp.HashGrid(
            dim_x=grid_resolution[0],
            dim_y=grid_resolution[1],
            dim_z=grid_resolution[2],
            device=str(mesh_vertices.device),
        )
        candidate_grid.reserve(batch_size)

        def _run_dart_throwing_pass(
            *,
            pass_distance: float,
            pass_seed: int,
            pass_limit: int,
            stage_name: str,
        ) -> int:
            accepted_count.zero_()
            current_count = 0

            # Main iterative parallel dart-throwing loop.
            for iteration in range(max_iterations):
                candidate_alive.fill_(1)

                # Generate one candidate batch on the mesh surface.
                wp.launch(
                    kernel=_generate_surface_candidates,
                    dim=batch_size,
                    inputs=[
                        wp_triangle_vertices,
                        wp_triangle_edge1,
                        wp_triangle_edge2,
                        wp_triangle_vertex_indices,
                        wp_area_cdf,
                        wp_per_vertex_radius,
                        float(pass_distance),
                        int(pass_seed + iteration * 104729),
                        wp_candidate_positions,
                        wp_candidate_radii,
                        wp_candidate_priorities,
                    ],
                    device=wp_launch_device,
                    stream=wp_launch_stream,
                )

                # Reject candidates near previously accepted points.
                if current_count > 0:
                    search_radius = max(pass_distance, adaptive_max_radius)
                    accepted_view = accepted_positions[:current_count]
                    accepted_grid.build(
                        points=wp.from_torch(accepted_view, dtype=wp.vec3f),
                        radius=search_radius,
                    )
                    wp.launch(
                        kernel=_reject_candidates_vs_accepted,
                        dim=batch_size,
                        inputs=[
                            accepted_grid.id,
                            wp_candidate_positions,
                            wp_candidate_radii,
                            wp_candidate_alive,
                            wp.from_torch(
                                accepted_view,
                                dtype=wp.vec3f,
                                return_ctype=True,
                            ),
                        ],
                        device=wp_launch_device,
                        stream=wp_launch_stream,
                    )

                # Resolve conflicts among this iteration's candidates.
                search_radius = max(pass_distance, adaptive_max_radius)
                candidate_grid.build(
                    points=wp_candidate_positions_array,
                    radius=search_radius,
                )
                wp.launch(
                    kernel=_resolve_candidate_conflicts,
                    dim=batch_size,
                    inputs=[
                        candidate_grid.id,
                        wp_candidate_positions,
                        wp_candidate_radii,
                        wp_candidate_priorities,
                        wp_candidate_alive,
                    ],
                    device=wp_launch_device,
                    stream=wp_launch_stream,
                )

                # Commit surviving candidates to accepted arrays.
                wp.launch(
                    kernel=_commit_accepted_candidates,
                    dim=batch_size,
                    inputs=[
                        wp_candidate_positions,
                        wp_candidate_radii,
                        wp_candidate_alive,
                        wp_accepted_positions,
                        wp_accepted_radii,
                        wp_accepted_count,
                    ],
                    device=wp_launch_device,
                    stream=wp_launch_stream,
                )

                # Read accepted count once per iteration and reuse it next iteration.
                count_after = int(accepted_count[0].item())
                accepted_now = min(count_after, pass_limit) - min(
                    current_count, pass_limit
                )
                current_count = count_after

                if verbose:
                    print(
                        f"{stage_name} iteration {iteration:02d}: accepted {accepted_now} "
                        f"(total: {min(current_count, pass_limit)})"
                    )

                # Stop on saturation or no-progress iterations.
                if accepted_now <= 0 or current_count >= pass_limit:
                    break

            return min(current_count, pass_limit)

        # Default mode: direct iterative dart throwing.
        final_count = _run_dart_throwing_pass(
            pass_distance=min_distance,
            pass_seed=random_seed,
            pass_limit=max_points,
            stage_name="dart-throwing",
        )
        return accepted_positions[:final_count].contiguous()


class MeshPoissonDiskSample(FunctionSpec):
    r"""Generate Poisson-disk samples on a triangle mesh surface with Warp.

    This functional supports two sampling modes on triangle meshes:

    1. ``dart_throwing``:
       iterative parallel dart throwing where each iteration draws area-weighted
       candidates, rejects points near accepted samples, resolves candidate-candidate
       conflicts with random-priority MIS, and commits survivors.
    2. ``weighted_sample_elimination``:
       builds an oversampled Poisson-quality pool, then downsamples to
       ``target_num_points`` using a radius-aware elimination pass.

    Both modes produce blue-noise-like sample sets. ``dart_throwing`` emphasizes
    throughput and minimum-distance control; ``weighted_sample_elimination``
    emphasizes distribution quality at a fixed output count.

    Parameters
    ----------
    mesh_vertices : torch.Tensor
        Mesh vertex positions with shape ``(n_vertices, 3)``.
    mesh_indices : torch.Tensor
        Triangle connectivity in shape ``(n_faces, 3)`` or flattened
        shape ``(3 * n_faces,)``.
    min_distance : float, optional
        Minimum Poisson distance for constant-radius mode. Default is ``0.02``.
        In ``weighted_sample_elimination`` mode this is treated as a lower-bound
        hint while the algorithm primarily targets ``target_num_points`` quality.
    per_vertex_radius : torch.Tensor | None, optional
        Optional adaptive radius with shape ``(n_vertices,)``.
        If provided, candidate radius is barycentrically interpolated.
    mode : str, optional
        Sampling mode. ``"dart_throwing"`` uses iterative parallel dart throwing.
        ``"weighted_sample_elimination"`` builds an oversampled Poisson pool and
        then downsamples to ``target_num_points`` with radius-aware elimination.
    batch_size : int, optional
        Number of generated candidates per iteration. Default is ``131072``.
    max_points : int, optional
        Maximum number of accepted samples. Default is ``2_000_000``.
        For ``mode="weighted_sample_elimination"``, this is also the default
        ``target_num_points`` when that argument is omitted.
    target_num_points : int | None, optional
        Number of output points for ``mode="weighted_sample_elimination"``.
        If ``None``, the mode uses ``max_points``.
    max_iterations : int, optional
        Iteration cap for the sampler. Default is ``64``.
    verbose : bool, optional
        If ``True``, prints per-iteration acceptance stats.
    random_seed : int, optional
        Base random seed for deterministic candidate generation.
    open3d_init_factor : int, optional
        Oversampling factor used by the Yuksel-style weighted elimination path.
        This mirrors Open3D's default behavior. Default is ``5``.
    hash_grid_resolution : int | Sequence[int], optional
        Hash-grid resolution, either scalar or ``(nx, ny, nz)``.
        Default is ``128``.
    implementation : str | None, optional
        Explicit implementation name. Defaults to dispatch behavior.

    Returns
    -------
    torch.Tensor
        Accepted sample positions with shape ``(n_samples, 3)`` and dtype
        ``torch.float32``.

    Notes
    -----
    - ``mode="weighted_sample_elimination"`` uses Warp kernels and follows
      Open3D's Yuksel-style weighting equations.
    - ``per_vertex_radius`` is ignored in weighted elimination mode.
    - The output order is implementation-specific and not semantically meaningful.
    """

    _BENCHMARK_CASES = (
        ("small-subdiv2-cst", 2, False, 4096, 0.07),
        ("medium-subdiv3-cst", 3, False, 8192, 0.05),
        ("large-subdiv3-adapt", 3, True, 8192, 0.05),
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
        min_distance: float = 0.02,
        per_vertex_radius: torch.Tensor | None = None,
        batch_size: int = 131072,
        max_points: int = 2_000_000,
        max_iterations: int = 64,
        verbose: bool = False,
        random_seed: int = 42,
        open3d_init_factor: int = 5,
        hash_grid_resolution: int | Sequence[int] | torch.Tensor = 128,
        mode: str = _DART_THROWING_MODE,
        target_num_points: int | None = None,
    ) -> torch.Tensor:
        return _mesh_poisson_disk_sample_warp(
            mesh_vertices=mesh_vertices,
            mesh_indices=mesh_indices,
            min_distance=min_distance,
            per_vertex_radius=per_vertex_radius,
            batch_size=batch_size,
            max_points=max_points,
            max_iterations=max_iterations,
            verbose=verbose,
            random_seed=random_seed,
            open3d_init_factor=open3d_init_factor,
            hash_grid_resolution=hash_grid_resolution,
            mode=mode,
            target_num_points=target_num_points,
        )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        from physicsnemo.mesh.primitives.procedural.lumpy_sphere import (
            load as load_lumpy_sphere,
        )

        device = torch.device(device)

        # Build benchmark cases in increasing workload order.
        for seed, (
            label,
            subdivisions,
            adaptive,
            batch_size,
            min_distance,
        ) in enumerate(cls._BENCHMARK_CASES):
            mesh = load_lumpy_sphere(subdivisions=subdivisions, device=str(device))
            mesh_vertices = mesh.points.to(torch.float32).contiguous()
            mesh_indices = mesh.cells.to(torch.int32).contiguous()

            per_vertex_radius = None
            if adaptive:
                # Smoothly varying positive radii derived from normalized z-coordinate.
                z = mesh_vertices[:, 2]
                z_min = z.min()
                z_max = z.max()
                denom = (z_max - z_min).clamp_min(1.0e-6)
                z_norm = (z - z_min) / denom
                per_vertex_radius = (min_distance * (0.75 + 0.5 * z_norm)).to(
                    torch.float32
                )

            yield (
                label,
                (mesh_vertices, mesh_indices),
                {
                    "min_distance": min_distance,
                    "per_vertex_radius": per_vertex_radius,
                    "batch_size": batch_size,
                    "max_points": 32768,
                    "max_iterations": 12,
                    "verbose": False,
                    "random_seed": 2026 + seed,
                    "hash_grid_resolution": 128,
                },
            )


mesh_poisson_disk_sample = MeshPoissonDiskSample.make_function(
    "mesh_poisson_disk_sample"
)


__all__ = ["MeshPoissonDiskSample", "mesh_poisson_disk_sample"]
