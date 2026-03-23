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

import warp as wp


# ----------------------------------------------------------------------------
# Neighborhood Count Kernel
# ----------------------------------------------------------------------------
@wp.kernel
def _count_neighbors_within_radius(
    hashgrid_id: wp.uint64,
    points: wp.array(dtype=wp.vec3f),
    search_radius: wp.float32,
    max_neighbors: wp.int32,
    out_counts: wp.array(dtype=wp.int32),
):
    point_idx = wp.tid()
    point = points[point_idx]
    radius_sq = search_radius * search_radius

    count = int(0)
    for neighbor_idx in wp.hash_grid_query(hashgrid_id, point, search_radius):
        if neighbor_idx == point_idx:
            continue

        distance_sq = wp.length_sq(point - points[neighbor_idx])
        if distance_sq > radius_sq:
            continue

        if count < max_neighbors:
            count += 1

    out_counts[point_idx] = count


# ----------------------------------------------------------------------------
# Neighborhood Write Kernel
# ----------------------------------------------------------------------------
@wp.kernel
def _write_neighbors_within_radius(
    hashgrid_id: wp.uint64,
    points: wp.array(dtype=wp.vec3f),
    search_radius: wp.float32,
    max_neighbors: wp.int32,
    row_ptr: wp.array(dtype=wp.int32),
    out_col_idx: wp.array(dtype=wp.int32),
):
    point_idx = wp.tid()
    point = points[point_idx]
    radius_sq = search_radius * search_radius

    row_start = row_ptr[point_idx]
    write_count = int(0)

    for neighbor_idx in wp.hash_grid_query(hashgrid_id, point, search_radius):
        if neighbor_idx == point_idx:
            continue

        distance_sq = wp.length_sq(point - points[neighbor_idx])
        if distance_sq > radius_sq:
            continue

        if write_count < max_neighbors:
            out_col_idx[row_start + write_count] = neighbor_idx
            write_count += 1


__all__ = [
    "_count_neighbors_within_radius",
    "_write_neighbors_within_radius",
]
