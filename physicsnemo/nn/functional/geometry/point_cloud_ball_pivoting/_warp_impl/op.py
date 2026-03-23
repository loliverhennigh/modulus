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
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import torch

from physicsnemo.core.function_spec import FunctionSpec

from ._topology_cpp import load_topology_cpp_module

_WARP_AVAILABLE = True
_WARP_IMPORT_ERROR: Exception | None = None

try:  # pragma: no cover - optional dependency
    import warp as wp
except Exception as exc:  # pragma: no cover - optional dependency
    _WARP_AVAILABLE = False
    _WARP_IMPORT_ERROR = exc

if _WARP_AVAILABLE:
    from ._kernels import (
        _count_neighbors_within_radius,
        _write_neighbors_within_radius,
    )

    wp.init()
    wp.config.quiet = True


# ----------------------------------------------------------------------------
# Topology Record Types
# ----------------------------------------------------------------------------
_VERTEX_ORPHAN = 0
_VERTEX_FRONT = 1
_VERTEX_INNER = 2

_EDGE_BORDER = 0
_EDGE_FRONT = 1
_EDGE_INNER = 2

_FRONT_MODE_SERIAL = "serial"
_FRONT_MODE_BATCHED = "batched"

_CPP_TOPOLOGY_RUNTIME_ERROR: Exception | None = None
_CPP_TOPOLOGY_WARNED = False


@dataclass
class _EdgeRecord:
    source: int
    target: int
    triangle0: int | None = None
    triangle1: int | None = None
    edge_type: int = _EDGE_FRONT


@dataclass
class _TriangleRecord:
    v0: int
    v1: int
    v2: int
    ball_center: np.ndarray


# ----------------------------------------------------------------------------
# Input Validation Helpers
# ----------------------------------------------------------------------------
def _normalize_points(points: torch.Tensor, normals: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
    if points.ndim != 2 or points.shape[1] != 3:
        raise ValueError("points must have shape (n_points, 3)")
    if normals.ndim != 2 or normals.shape[1] != 3:
        raise ValueError("normals must have shape (n_points, 3)")
    if points.shape[0] != normals.shape[0]:
        raise ValueError("points and normals must have the same number of rows")
    if points.shape[0] < 3:
        raise ValueError("at least three points are required")
    if points.device != normals.device:
        raise ValueError("points and normals must be on the same device")
    if points.dtype not in {torch.float16, torch.float32, torch.float64}:
        raise TypeError("points must be a floating dtype tensor")
    if normals.dtype not in {torch.float16, torch.float32, torch.float64}:
        raise TypeError("normals must be a floating dtype tensor")
    return points.to(torch.float32).contiguous(), normals.to(torch.float32).contiguous()


def _normalize_radii(radii: Sequence[float] | torch.Tensor) -> list[float]:
    if torch.is_tensor(radii):
        if radii.ndim != 1:
            raise ValueError("radii tensor must be rank-1")
        values = [float(v) for v in radii.detach().cpu().tolist()]
    else:
        values = [float(v) for v in radii]

    if not values:
        raise ValueError("radii must contain at least one value")

    for value in values:
        if not math.isfinite(value) or value <= 0.0:
            raise ValueError("all radii values must be strictly positive finite floats")
    return values


def _normalize_hash_grid_dim(hash_grid_dim: int | Sequence[int]) -> tuple[int, int, int]:
    if isinstance(hash_grid_dim, int):
        if hash_grid_dim <= 0:
            raise ValueError("hash_grid_dim must be strictly positive")
        return hash_grid_dim, hash_grid_dim, hash_grid_dim

    if len(hash_grid_dim) != 3:
        raise ValueError("hash_grid_dim sequence must contain exactly three values")
    dims = tuple(int(v) for v in hash_grid_dim)
    if dims[0] <= 0 or dims[1] <= 0 or dims[2] <= 0:
        raise ValueError("hash_grid_dim values must be strictly positive")
    return dims


def _normalize_max_neighbors(max_neighbors: int) -> int:
    value = int(max_neighbors)
    if value < 3:
        raise ValueError("max_neighbors must be >= 3 for triangle construction")
    return value


def _normalize_max_triangles(max_triangles: int | None) -> int | None:
    if max_triangles is None:
        return None
    value = int(max_triangles)
    if value <= 0:
        raise ValueError("max_triangles must be strictly positive when provided")
    return value


def _normalize_front_mode(front_mode: str) -> str:
    value = str(front_mode).strip().lower()
    if value not in {_FRONT_MODE_SERIAL, _FRONT_MODE_BATCHED}:
        raise ValueError(
            f"front_mode must be one of: {_FRONT_MODE_SERIAL!r}, {_FRONT_MODE_BATCHED!r}"
        )
    return value


def _normalize_front_batch_size(front_batch_size: int) -> int:
    value = int(front_batch_size)
    if value <= 0:
        raise ValueError("front_batch_size must be strictly positive")
    return value


def _run_topology_cpp(
    points_np: np.ndarray,
    normals_np: np.ndarray,
    row_ptr_np: np.ndarray,
    col_idx_np: np.ndarray,
    radius_values: Sequence[float],
    max_triangles: int | None,
    front_mode: str,
    front_batch_size: int,
) -> np.ndarray | None:
    global _CPP_TOPOLOGY_RUNTIME_ERROR

    try:
        module = load_topology_cpp_module()
        faces_np = module.run_topology(
            points_np,
            normals_np,
            row_ptr_np,
            col_idx_np,
            [float(v) for v in radius_values],
            -1 if max_triangles is None else int(max_triangles),
            front_mode,
            int(front_batch_size),
        )
    except Exception as exc:  # pragma: no cover - compile/runtime environment dependent
        _CPP_TOPOLOGY_RUNTIME_ERROR = exc
        return None

    return np.asarray(faces_np, dtype=np.int32, order="C")


# ----------------------------------------------------------------------------
# Warp Neighbor CSR Builder
# ----------------------------------------------------------------------------
def _auto_hash_grid_dims(points: torch.Tensor, search_radius: float) -> tuple[int, int, int]:
    bbox_min = points.min(dim=0).values
    bbox_max = points.max(dim=0).values
    extent = float((bbox_max - bbox_min).amax().detach().cpu().item())
    extent = max(extent, search_radius)
    approx = int(math.ceil(extent / max(search_radius, 1.0e-6))) + 3
    dim = max(8, min(512, approx))
    return dim, dim, dim


def _build_neighbor_csr(
    points: torch.Tensor,
    *,
    search_radius: float,
    max_neighbors: int,
    hash_grid_dim: int | Sequence[int] | None,
) -> tuple[torch.Tensor, torch.Tensor]:
    n_points = points.shape[0]
    if n_points == 0:
        row_ptr = torch.zeros(1, dtype=torch.int32, device=points.device)
        col_idx = torch.zeros(0, dtype=torch.int32, device=points.device)
        return row_ptr, col_idx

    if hash_grid_dim is None:
        dim_x, dim_y, dim_z = _auto_hash_grid_dims(points, search_radius)
    else:
        dim_x, dim_y, dim_z = _normalize_hash_grid_dim(hash_grid_dim)

    counts = torch.zeros(n_points, dtype=torch.int32, device=points.device)
    row_ptr = torch.zeros(n_points + 1, dtype=torch.int32, device=points.device)
    wp_device, wp_stream = FunctionSpec.warp_launch_context(points)

    with wp.ScopedStream(wp_stream):
        wp_points = wp.from_torch(points, dtype=wp.vec3f)
        wp_counts = wp.from_torch(counts, dtype=wp.int32)

        hash_grid = wp.HashGrid(dim_x, dim_y, dim_z, device=wp_device)
        hash_grid.build(wp_points, float(search_radius))

        wp.launch(
            kernel=_count_neighbors_within_radius,
            dim=n_points,
            inputs=[
                hash_grid.id,
                wp_points,
                float(search_radius),
                int(max_neighbors),
                wp_counts,
            ],
            device=wp_device,
            stream=wp_stream,
        )

        row_ptr[1:] = torch.cumsum(counts, dim=0)
        total_neighbors = int(row_ptr[-1].item())
        col_idx = torch.full(
            (total_neighbors,),
            -1,
            dtype=torch.int32,
            device=points.device,
        )

        wp.launch(
            kernel=_write_neighbors_within_radius,
            dim=n_points,
            inputs=[
                hash_grid.id,
                wp_points,
                float(search_radius),
                int(max_neighbors),
                wp.from_torch(row_ptr, dtype=wp.int32),
                wp.from_torch(col_idx, dtype=wp.int32),
            ],
            device=wp_device,
            stream=wp_stream,
        )

    return row_ptr, col_idx


# ----------------------------------------------------------------------------
# Ball-Pivot Core State
# ----------------------------------------------------------------------------
class _BallPivotState:
    def __init__(
        self,
        *,
        points: np.ndarray,
        normals: np.ndarray,
        neighbors: list[list[int]],
        max_triangles: int | None,
    ):
        self.points = points
        self.normals = normals
        self.n_points = points.shape[0]
        self.neighbors = neighbors
        self.max_triangles = max_triangles

        self.vertex_types = np.zeros(self.n_points, dtype=np.int8)
        self.vertex_edges: list[set[int]] = [set() for _ in range(self.n_points)]

        self.edges: list[_EdgeRecord] = []
        self.edge_lookup: dict[tuple[int, int], int] = {}

        self.triangles: list[_TriangleRecord] = []
        self.faces: list[tuple[int, int, int]] = []

        self.edge_front: deque[int] = deque()
        self.border_edges: list[int] = []
        self._candidate_mark = np.zeros(self.n_points, dtype=np.int32)
        self._candidate_tag = 1

        self._eps = 1.0e-12

    # ---------------------------------------------------------------------
    # Geometry Helpers
    # ---------------------------------------------------------------------
    def _face_normal(self, v0: int, v1: int, v2: int) -> np.ndarray:
        n = np.cross(self.points[v1] - self.points[v0], self.points[v2] - self.points[v0])
        norm = float(np.linalg.norm(n))
        if norm <= self._eps:
            return np.zeros(3, dtype=np.float64)
        return n / norm

    def _is_compatible(self, v0: int, v1: int, v2: int) -> bool:
        normal = self._face_normal(v0, v1, v2)
        if float(np.dot(normal, self.normals[v0])) < -1.0e-16:
            normal = -normal
        return (
            float(np.dot(normal, self.normals[v0])) > -1.0e-16
            and float(np.dot(normal, self.normals[v1])) > -1.0e-16
            and float(np.dot(normal, self.normals[v2])) > -1.0e-16
        )

    def _compute_ball_center(self, v0: int, v1: int, v2: int, radius: float) -> tuple[bool, np.ndarray]:
        p0 = self.points[v0]
        p1 = self.points[v1]
        p2 = self.points[v2]

        c = float(np.dot(p1 - p0, p1 - p0))
        b = float(np.dot(p0 - p2, p0 - p2))
        a = float(np.dot(p2 - p1, p2 - p1))

        alpha = a * (b + c - a)
        beta = b * (a + c - b)
        gamma = c * (a + b - c)
        abg = alpha + beta + gamma
        if abg < 1.0e-16:
            return False, np.zeros(3, dtype=np.float64)

        alpha /= abg
        beta /= abg
        gamma /= abg

        circ_center = alpha * p0 + beta * p1 + gamma * p2
        circ_radius_sq = a * b * c

        a = math.sqrt(max(a, 0.0))
        b = math.sqrt(max(b, 0.0))
        c = math.sqrt(max(c, 0.0))
        denom = (a + b + c) * (b + c - a) * (c + a - b) * (a + b - c)
        if abs(denom) <= self._eps:
            return False, np.zeros(3, dtype=np.float64)
        circ_radius_sq = circ_radius_sq / denom

        height_sq = radius * radius - circ_radius_sq
        if height_sq < 0.0:
            return False, np.zeros(3, dtype=np.float64)

        tri_norm = np.cross(p1 - p0, p2 - p0)
        tri_norm_norm = float(np.linalg.norm(tri_norm))
        if tri_norm_norm <= self._eps:
            return False, np.zeros(3, dtype=np.float64)
        tri_norm /= tri_norm_norm

        point_norm = self.normals[v0] + self.normals[v1] + self.normals[v2]
        point_norm_norm = float(np.linalg.norm(point_norm))
        if point_norm_norm <= self._eps:
            return False, np.zeros(3, dtype=np.float64)
        point_norm /= point_norm_norm

        if float(np.dot(tri_norm, point_norm)) < 0.0:
            tri_norm *= -1.0

        center = circ_center + math.sqrt(max(height_sq, 0.0)) * tri_norm
        return True, center

    # ---------------------------------------------------------------------
    # Edge/Vertex Topology Helpers
    # ---------------------------------------------------------------------
    @staticmethod
    def _edge_key(v0: int, v1: int) -> tuple[int, int]:
        return (v0, v1) if v0 < v1 else (v1, v0)

    def _get_linking_edge(self, v0: int, v1: int) -> int | None:
        return self.edge_lookup.get(self._edge_key(v0, v1))

    def _get_or_create_edge(self, v0: int, v1: int) -> int:
        key = self._edge_key(v0, v1)
        existing = self.edge_lookup.get(key)
        if existing is not None:
            return existing
        edge_idx = len(self.edges)
        self.edges.append(_EdgeRecord(source=v0, target=v1))
        self.edge_lookup[key] = edge_idx
        return edge_idx

    def _edge_opposite_vertex(self, edge_idx: int) -> int | None:
        edge = self.edges[edge_idx]
        if edge.triangle0 is None:
            return None
        tri = self.triangles[edge.triangle0]
        if tri.v0 != edge.source and tri.v0 != edge.target:
            return tri.v0
        if tri.v1 != edge.source and tri.v1 != edge.target:
            return tri.v1
        return tri.v2

    def _add_adjacent_triangle(self, edge_idx: int, tri_idx: int) -> None:
        edge = self.edges[edge_idx]
        if edge.triangle0 != tri_idx and edge.triangle1 != tri_idx:
            if edge.triangle0 is None:
                edge.triangle0 = tri_idx
                edge.edge_type = _EDGE_FRONT

                opp = self._edge_opposite_vertex(edge_idx)
                if opp is not None:
                    tri_norm = np.cross(
                        self.points[edge.target] - self.points[edge.source],
                        self.points[opp] - self.points[edge.source],
                    )
                    tri_norm_norm = float(np.linalg.norm(tri_norm))
                    if tri_norm_norm > self._eps:
                        tri_norm /= tri_norm_norm
                        pt_norm = (
                            self.normals[edge.source]
                            + self.normals[edge.target]
                            + self.normals[opp]
                        )
                        pt_norm_norm = float(np.linalg.norm(pt_norm))
                        if pt_norm_norm > self._eps:
                            pt_norm /= pt_norm_norm
                            if float(np.dot(pt_norm, tri_norm)) < 0.0:
                                edge.source, edge.target = edge.target, edge.source
            elif edge.triangle1 is None:
                edge.triangle1 = tri_idx
                edge.edge_type = _EDGE_INNER

    def _update_vertex_type(self, vertex_idx: int) -> None:
        edges = self.vertex_edges[vertex_idx]
        if not edges:
            self.vertex_types[vertex_idx] = _VERTEX_ORPHAN
            return
        for edge_idx in edges:
            if self.edges[edge_idx].edge_type != _EDGE_INNER:
                self.vertex_types[vertex_idx] = _VERTEX_FRONT
                return
        self.vertex_types[vertex_idx] = _VERTEX_INNER

    # ---------------------------------------------------------------------
    # Triangle Construction and Candidate Search
    # ---------------------------------------------------------------------
    def _create_triangle(self, v0: int, v1: int, v2: int, center: np.ndarray) -> bool:
        if self.max_triangles is not None and len(self.faces) >= self.max_triangles:
            return False

        tri_idx = len(self.triangles)
        self.triangles.append(_TriangleRecord(v0=v0, v1=v1, v2=v2, ball_center=center))

        e0 = self._get_or_create_edge(v0, v1)
        self._add_adjacent_triangle(e0, tri_idx)
        self.vertex_edges[v0].add(e0)
        self.vertex_edges[v1].add(e0)

        e1 = self._get_or_create_edge(v1, v2)
        self._add_adjacent_triangle(e1, tri_idx)
        self.vertex_edges[v1].add(e1)
        self.vertex_edges[v2].add(e1)

        e2 = self._get_or_create_edge(v2, v0)
        self._add_adjacent_triangle(e2, tri_idx)
        self.vertex_edges[v2].add(e2)
        self.vertex_edges[v0].add(e2)

        self._update_vertex_type(v0)
        self._update_vertex_type(v1)
        self._update_vertex_type(v2)

        face_normal = self._face_normal(v0, v1, v2)
        if float(np.dot(face_normal, self.normals[v0])) > -1.0e-16:
            self.faces.append((v0, v1, v2))
        else:
            self.faces.append((v0, v2, v1))
        return True

    def _candidate_pool_from_vertices(
        self,
        v0: int,
        v1: int,
        v2: int | None,
        *,
        midpoint: np.ndarray,
        radius: float,
    ) -> list[int]:
        # Reuse a mark array to deduplicate candidates without allocating a new set.
        self._candidate_tag += 1
        if self._candidate_tag >= np.iinfo(np.int32).max:
            self._candidate_mark.fill(0)
            self._candidate_tag = 1
        tag = self._candidate_tag
        marks = self._candidate_mark

        candidates: list[int] = []

        def _append_neighbors(vertex_idx: int) -> None:
            for idx in self.neighbors[vertex_idx]:
                if idx < 0 or idx >= self.n_points:
                    continue
                if marks[idx] == tag:
                    continue
                marks[idx] = tag
                candidates.append(idx)

        _append_neighbors(v0)
        _append_neighbors(v1)
        if v2 is not None:
            _append_neighbors(v2)

        max_dist_sq = (2.0 * radius) * (2.0 * radius)
        return [
            idx
            for idx in candidates
            if (
                float(np.dot(self.points[idx] - midpoint, self.points[idx] - midpoint))
                <= max_dist_sq
            )
        ]

    def _is_empty_ball(
        self,
        center: np.ndarray,
        radius: float,
        *,
        excluded: set[int],
        candidates: list[int],
    ) -> bool:
        threshold = radius - 1.0e-16
        threshold_sq = threshold * threshold
        for idx in candidates:
            if idx in excluded:
                continue
            if float(np.dot(center - self.points[idx], center - self.points[idx])) < threshold_sq:
                return False
        return True

    def _is_empty_ball_excluding_three(
        self,
        center: np.ndarray,
        radius: float,
        *,
        e0: int,
        e1: int,
        e2: int,
        candidates: list[int],
    ) -> bool:
        threshold_sq = (radius - 1.0e-16) * (radius - 1.0e-16)
        for idx in candidates:
            if idx == e0 or idx == e1 or idx == e2:
                continue
            if (
                float(np.dot(center - self.points[idx], center - self.points[idx]))
                < threshold_sq
            ):
                return False
        return True

    def _find_candidate_vertex(
        self, edge_idx: int, radius: float
    ) -> tuple[int | None, np.ndarray | None, float]:
        edge = self.edges[edge_idx]
        if edge.triangle0 is None:
            return None, None, 2.0 * math.pi

        src = edge.source
        tgt = edge.target
        opp = self._edge_opposite_vertex(edge_idx)
        if opp is None:
            return None, None, 2.0 * math.pi

        tri = self.triangles[edge.triangle0]
        center = tri.ball_center
        midpoint = 0.5 * (self.points[src] + self.points[tgt])

        edge_dir = self.points[tgt] - self.points[src]
        edge_norm = float(np.linalg.norm(edge_dir))
        if edge_norm <= self._eps:
            return None, None, 2.0 * math.pi
        edge_dir /= edge_norm

        a = center - midpoint
        a_norm = float(np.linalg.norm(a))
        if a_norm <= self._eps:
            return None, None, 2.0 * math.pi
        a /= a_norm

        candidates = self._candidate_pool_from_vertices(
            src,
            tgt,
            opp,
            midpoint=midpoint,
            radius=radius,
        )
        min_angle = 2.0 * math.pi
        best_idx: int | None = None
        best_center: np.ndarray | None = None

        for candidate in candidates:
            if candidate in {src, tgt, opp}:
                continue

            valid, new_center = self._compute_ball_center(src, tgt, candidate, radius)
            if not valid:
                continue

            b = new_center - midpoint
            b_norm = float(np.linalg.norm(b))
            if b_norm <= self._eps:
                continue
            b /= b_norm

            cos_angle = max(-1.0, min(1.0, float(np.dot(a, b))))
            angle = math.acos(cos_angle)
            c = np.cross(a, b)
            if float(np.dot(c, edge_dir)) < 0.0:
                angle = 2.0 * math.pi - angle

            if angle >= min_angle:
                continue

            if not self._is_empty_ball_excluding_three(
                new_center,
                radius,
                e0=src,
                e1=tgt,
                e2=candidate,
                candidates=candidates,
            ):
                continue

            min_angle = angle
            best_idx = candidate
            best_center = new_center

        return best_idx, best_center, min_angle

    # ---------------------------------------------------------------------
    # Seed and Front Expansion
    # ---------------------------------------------------------------------
    def _try_triangle_seed(
        self,
        v0: int,
        v1: int,
        v2: int,
        *,
        radius: float,
        neighborhood: list[int],
    ) -> tuple[bool, np.ndarray | None]:
        if not self._is_compatible(v0, v1, v2):
            return False, None

        e0 = self._get_linking_edge(v0, v2)
        e1 = self._get_linking_edge(v1, v2)
        if e0 is not None and self.edges[e0].edge_type == _EDGE_INNER:
            return False, None
        if e1 is not None and self.edges[e1].edge_type == _EDGE_INNER:
            return False, None

        valid, center = self._compute_ball_center(v0, v1, v2, radius)
        if not valid:
            return False, None

        if not self._is_empty_ball(center, radius, excluded={v0, v1, v2}, candidates=neighborhood):
            return False, None
        return True, center

    def _try_seed(self, vertex_idx: int, radius: float) -> bool:
        indices = self.neighbors[vertex_idx]
        if len(indices) < 2:
            return False

        for i, nb0_idx in enumerate(indices):
            if nb0_idx == vertex_idx or self.vertex_types[nb0_idx] != _VERTEX_ORPHAN:
                continue

            candidate_v2 = None
            candidate_center = None

            for nb1_idx in indices[i + 1 :]:
                if nb1_idx == vertex_idx or self.vertex_types[nb1_idx] != _VERTEX_ORPHAN:
                    continue
                ok, center = self._try_triangle_seed(
                    vertex_idx,
                    nb0_idx,
                    nb1_idx,
                    radius=radius,
                    neighborhood=indices,
                )
                if ok:
                    candidate_v2 = nb1_idx
                    candidate_center = center
                    break

            if candidate_v2 is None or candidate_center is None:
                continue

            e0 = self._get_linking_edge(vertex_idx, candidate_v2)
            e1 = self._get_linking_edge(nb0_idx, candidate_v2)
            e2 = self._get_linking_edge(vertex_idx, nb0_idx)
            if (e0 is not None and self.edges[e0].edge_type != _EDGE_FRONT) or (
                e1 is not None and self.edges[e1].edge_type != _EDGE_FRONT
            ) or (e2 is not None and self.edges[e2].edge_type != _EDGE_FRONT):
                continue

            created = self._create_triangle(vertex_idx, nb0_idx, candidate_v2, candidate_center)
            if not created:
                return False

            e0 = self._get_linking_edge(vertex_idx, candidate_v2)
            e1 = self._get_linking_edge(nb0_idx, candidate_v2)
            e2 = self._get_linking_edge(vertex_idx, nb0_idx)
            for edge_idx in (e0, e1, e2):
                if edge_idx is not None and self.edges[edge_idx].edge_type == _EDGE_FRONT:
                    self.edge_front.appendleft(edge_idx)

            if self.edge_front:
                return True
        return False

    def _expand_triangulation(self, radius: float) -> None:
        while self.edge_front:
            edge_idx = self.edge_front.popleft()
            if self.edges[edge_idx].edge_type != _EDGE_FRONT:
                continue

            candidate, center, _ = self._find_candidate_vertex(edge_idx, radius)
            edge = self.edges[edge_idx]
            if (
                candidate is None
                or center is None
                or self.vertex_types[candidate] == _VERTEX_INNER
                or not self._is_compatible(candidate, edge.source, edge.target)
            ):
                edge.edge_type = _EDGE_BORDER
                self.border_edges.append(edge_idx)
                continue

            e0 = self._get_linking_edge(candidate, edge.source)
            e1 = self._get_linking_edge(candidate, edge.target)
            if (e0 is not None and self.edges[e0].edge_type != _EDGE_FRONT) or (
                e1 is not None and self.edges[e1].edge_type != _EDGE_FRONT
            ):
                edge.edge_type = _EDGE_BORDER
                self.border_edges.append(edge_idx)
                continue

            created = self._create_triangle(edge.source, edge.target, candidate, center)
            if not created:
                return

            e0 = self._get_linking_edge(candidate, edge.source)
            e1 = self._get_linking_edge(candidate, edge.target)
            if e0 is not None and self.edges[e0].edge_type == _EDGE_FRONT:
                self.edge_front.appendleft(e0)
            if e1 is not None and self.edges[e1].edge_type == _EDGE_FRONT:
                self.edge_front.appendleft(e1)

    def _expand_triangulation_batched(self, radius: float, batch_size: int) -> None:
        while self.edge_front:
            active_edges: list[int] = []
            deferred_edges: list[int] = []
            occupied_vertices: set[int] = set()
            while self.edge_front and len(active_edges) < batch_size:
                edge_idx = self.edge_front.popleft()
                if self.edges[edge_idx].edge_type == _EDGE_FRONT:
                    edge = self.edges[edge_idx]
                    if (
                        edge.source in occupied_vertices
                        or edge.target in occupied_vertices
                    ):
                        deferred_edges.append(edge_idx)
                        continue
                    active_edges.append(edge_idx)
                    occupied_vertices.update((edge.source, edge.target))
            if not active_edges:
                for edge_idx in deferred_edges:
                    self.edge_front.append(edge_idx)
                continue
            accepted_new_edges: list[int] = []
            for edge_idx in active_edges:
                if self.edges[edge_idx].edge_type != _EDGE_FRONT:
                    continue
                candidate, center, _ = self._find_candidate_vertex(edge_idx, radius)
                edge = self.edges[edge_idx]
                if (
                    candidate is None
                    or center is None
                    or self.vertex_types[candidate] == _VERTEX_INNER
                    or not self._is_compatible(candidate, edge.source, edge.target)
                ):
                    edge.edge_type = _EDGE_BORDER
                    self.border_edges.append(edge_idx)
                    continue

                e0 = self._get_linking_edge(candidate, edge.source)
                e1 = self._get_linking_edge(candidate, edge.target)
                if (e0 is not None and self.edges[e0].edge_type != _EDGE_FRONT) or (
                    e1 is not None and self.edges[e1].edge_type != _EDGE_FRONT
                ):
                    edge.edge_type = _EDGE_BORDER
                    self.border_edges.append(edge_idx)
                    continue

                created = self._create_triangle(
                    edge.source,
                    edge.target,
                    candidate,
                    center,
                )
                if not created:
                    return

                e0 = self._get_linking_edge(candidate, edge.source)
                e1 = self._get_linking_edge(candidate, edge.target)
                if e0 is not None and self.edges[e0].edge_type == _EDGE_FRONT:
                    accepted_new_edges.append(e0)
                if e1 is not None and self.edges[e1].edge_type == _EDGE_FRONT:
                    accepted_new_edges.append(e1)

            for edge_idx in accepted_new_edges:
                self.edge_front.appendleft(edge_idx)
            for edge_idx in deferred_edges:
                self.edge_front.append(edge_idx)

    def _expand_triangulation_with_mode(
        self,
        radius: float,
        *,
        front_mode: str,
        front_batch_size: int,
    ) -> None:
        if front_mode == _FRONT_MODE_BATCHED:
            self._expand_triangulation_batched(radius, front_batch_size)
        else:
            self._expand_triangulation(radius)

    def _find_seed_triangle(
        self,
        radius: float,
        *,
        front_mode: str,
        front_batch_size: int,
    ) -> None:
        for vertex_idx in range(self.n_points):
            if self.vertex_types[vertex_idx] != _VERTEX_ORPHAN:
                continue
            if self._try_seed(vertex_idx, radius):
                self._expand_triangulation_with_mode(
                    radius,
                    front_mode=front_mode,
                    front_batch_size=front_batch_size,
                )
            if self.max_triangles is not None and len(self.faces) >= self.max_triangles:
                return

    def _refresh_border_edges_for_radius(self, radius: float) -> None:
        kept: list[int] = []
        for edge_idx in self.border_edges:
            edge = self.edges[edge_idx]
            if edge.triangle0 is None:
                kept.append(edge_idx)
                continue

            tri = self.triangles[edge.triangle0]
            valid, center = self._compute_ball_center(tri.v0, tri.v1, tri.v2, radius)
            if not valid:
                kept.append(edge_idx)
                continue

            midpoint = (self.points[tri.v0] + self.points[tri.v1] + self.points[tri.v2]) / 3.0
            candidates = self._candidate_pool_from_vertices(
                tri.v0,
                tri.v1,
                tri.v2,
                midpoint=midpoint,
                radius=radius,
            )
            if self._is_empty_ball_excluding_three(
                center,
                radius,
                e0=tri.v0,
                e1=tri.v1,
                e2=tri.v2,
                candidates=candidates,
            ):
                edge.edge_type = _EDGE_FRONT
                self.edge_front.append(edge_idx)
            else:
                kept.append(edge_idx)
        self.border_edges = kept

    # ---------------------------------------------------------------------
    # Public Execution Entry
    # ---------------------------------------------------------------------
    def run(
        self,
        radii: Sequence[float],
        *,
        front_mode: str,
        front_batch_size: int,
    ) -> np.ndarray:
        self.faces.clear()
        for radius in radii:
            self._refresh_border_edges_for_radius(radius)
            if not self.edge_front:
                self._find_seed_triangle(
                    radius,
                    front_mode=front_mode,
                    front_batch_size=front_batch_size,
                )
            else:
                self._expand_triangulation_with_mode(
                    radius,
                    front_mode=front_mode,
                    front_batch_size=front_batch_size,
                )
            if self.max_triangles is not None and len(self.faces) >= self.max_triangles:
                break

        if not self.faces:
            return np.zeros((0, 3), dtype=np.int32)
        return np.asarray(self.faces, dtype=np.int32)


# ----------------------------------------------------------------------------
# Public Warp Functional Entry
# ----------------------------------------------------------------------------
if _WARP_AVAILABLE:

    def point_cloud_ball_pivoting_warp(
        points: torch.Tensor,
        normals: torch.Tensor,
        radii: Sequence[float] | torch.Tensor,
        *,
        max_neighbors: int = 128,
        hash_grid_dim: int | Sequence[int] | None = None,
        max_triangles: int | None = None,
        front_mode: str = _FRONT_MODE_SERIAL,
        front_batch_size: int = 16,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """Reconstruct a triangle mesh from oriented points via ball pivoting.

        Notes
        -----
        This implementation follows Open3D-style front propagation and uses Warp
        ``HashGrid`` kernels for neighborhood precomputation.
        """
        points, normals = _normalize_points(points, normals)
        radius_values = _normalize_radii(radii)
        max_neighbors = _normalize_max_neighbors(max_neighbors)
        max_triangles = _normalize_max_triangles(max_triangles)
        front_mode = _normalize_front_mode(front_mode)
        front_batch_size = _normalize_front_batch_size(front_batch_size)

        # Build a Warp neighbor graph once with radius large enough for all pivots.
        search_radius = 2.0 * max(radius_values)
        row_ptr, col_idx = _build_neighbor_csr(
            points,
            search_radius=search_radius,
            max_neighbors=max_neighbors,
            hash_grid_dim=hash_grid_dim,
        )

        # Use float64 host geometry math for robust ball-center calculations.
        points_np = points.detach().cpu().numpy().astype(np.float64, copy=False)
        normals_np = normals.detach().cpu().numpy().astype(np.float64, copy=False)
        normal_norms = np.linalg.norm(normals_np, axis=1, keepdims=True)
        normal_norms = np.where(normal_norms > 1.0e-12, normal_norms, 1.0)
        normals_np = normals_np / normal_norms
        row_ptr_np = row_ptr.detach().cpu().numpy().astype(np.int32, copy=False)
        col_idx_np = col_idx.detach().cpu().numpy().astype(np.int32, copy=False)

        # Prefer compiled pybind11 topology; fall back to Python implementation
        # when local toolchain/extension build is unavailable.
        faces_np = _run_topology_cpp(
            points_np=points_np,
            normals_np=normals_np,
            row_ptr_np=row_ptr_np,
            col_idx_np=col_idx_np,
            radius_values=radius_values,
            max_triangles=max_triangles,
            front_mode=front_mode,
            front_batch_size=front_batch_size,
        )
        if faces_np is None:  # pragma: no cover - fallback path depends on local C++ toolchain
            global _CPP_TOPOLOGY_WARNED
            if not _CPP_TOPOLOGY_WARNED:
                warnings.warn(
                    "point_cloud_ball_pivoting: compiled topology backend unavailable; "
                    "falling back to slower Python topology loop",
                    RuntimeWarning,
                    stacklevel=2,
                )
                _CPP_TOPOLOGY_WARNED = True

            row_ptr_cpu = row_ptr_np.tolist()
            col_idx_cpu = col_idx_np.tolist()
            neighbors: list[list[int]] = []
            for point_idx in range(points.shape[0]):
                start = row_ptr_cpu[point_idx]
                end = row_ptr_cpu[point_idx + 1]
                neighbors.append([int(v) for v in col_idx_cpu[start:end] if int(v) >= 0])

            state = _BallPivotState(
                points=points_np,
                normals=normals_np,
                neighbors=neighbors,
                max_triangles=max_triangles,
            )
            faces_np = state.run(
                radius_values,
                front_mode=front_mode,
                front_batch_size=front_batch_size,
            )

        # Match PhysicsNeMo functional convention: tensors-in, tensors-out.
        out_vertices = points.contiguous()
        out_faces = torch.from_numpy(faces_np).to(device=points.device, dtype=torch.int32)
        return out_vertices, out_faces

else:

    def point_cloud_ball_pivoting_warp(*args, **kwargs):
        raise ImportError("point_cloud_ball_pivoting_warp requires 'warp>=0.6.0'") from _WARP_IMPORT_ERROR


__all__ = ["point_cloud_ball_pivoting_warp"]
