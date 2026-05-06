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

"""Example-local Euler finite-volume operators.

These operators intentionally live in the example instead of
``physicsnemo.nn.functional`` because primitive conversion, Euler boundary
conditions, MUSCL-Hancock prediction, Rusanov flux evaluation, and conserved
state updates are specific to compressible Euler. The public functions below
have both Torch and Warp backends so the solver loop can stay as one clear
algorithm.
"""

from __future__ import annotations

import torch

from physicsnemo.core.function_spec import FunctionSpec

try:
    import warp as wp

    wp.init()
    wp.config.quiet = True
    _WARP_AVAILABLE = True
except ImportError:
    wp = None
    _WARP_AVAILABLE = False


INTERIOR = 0
WALL_SLIP = 1
INFLOW_SUPERSONIC = 2
OUTFLOW_ZERO_GRAD = 3


def _require_warp() -> None:
    if not _WARP_AVAILABLE:
        raise ImportError("The fused Euler solver requires the 'warp' package")


def _launch_context(reference: torch.Tensor):
    return FunctionSpec.warp_launch_context(reference)


def primitive_to_conservative_torch(W: torch.Tensor, gamma: float) -> torch.Tensor:
    """Convert primitive variables [rho, velocity..., pressure] to conserved form."""
    rho = W[:, 0]
    velocity = W[:, 1:-1]
    pressure = W[:, -1]
    kinetic = 0.5 * rho * (velocity * velocity).sum(dim=1)
    energy = pressure / (gamma - 1.0) + kinetic
    return torch.cat((rho[:, None], rho[:, None] * velocity, energy[:, None]), dim=1)


def euler_conservative_to_primitive_torch(
    U: torch.Tensor,
    gamma: float,
    density_floor: float,
    pressure_floor: float,
) -> torch.Tensor:
    """Convert conserved variables [rho, rho*velocity..., energy] to primitives."""
    rho = U[:, 0].clamp_min(density_floor)
    momentum = U[:, 1:-1]
    velocity = momentum / rho[:, None]
    energy = U[:, -1]
    kinetic = 0.5 * rho * (velocity * velocity).sum(dim=1)
    pressure = ((gamma - 1.0) * (energy - kinetic)).clamp_min(pressure_floor)
    return torch.cat((rho[:, None], velocity, pressure[:, None]), dim=1)


def _apply_euler_boundary_state_torch(
    left: torch.Tensor,
    normals: torch.Tensor,
    tags: torch.Tensor,
    inflow_state: torch.Tensor,
) -> torch.Tensor:
    right = left.clone()
    dims = normals.shape[1]

    inflow = tags == INFLOW_SUPERSONIC
    if bool(torch.any(inflow).item()):
        right[inflow] = inflow_state.to(device=left.device, dtype=left.dtype)

    wall = tags == WALL_SLIP
    if bool(torch.any(wall).item()):
        velocity = left[wall, 1 : 1 + dims]
        normal = normals[wall]
        normal_velocity = (velocity * normal).sum(dim=1, keepdim=True)
        right[wall, 1 : 1 + dims] = velocity - 2.0 * normal_velocity * normal

    return right


def _euler_flux_normal_torch(
    W: torch.Tensor, normals: torch.Tensor, gamma: float
) -> torch.Tensor:
    rho = W[:, 0]
    velocity = W[:, 1:-1]
    pressure = W[:, -1]
    normal_velocity = (velocity * normals).sum(dim=1)
    U = primitive_to_conservative_torch(W, gamma)

    flux = torch.empty_like(U)
    flux[:, 0] = rho * normal_velocity
    flux[:, 1:-1] = (
        rho[:, None] * velocity * normal_velocity[:, None] + pressure[:, None] * normals
    )
    flux[:, -1] = (U[:, -1] + pressure) * normal_velocity
    return flux


def _rusanov_euler_flux_torch(
    W_left: torch.Tensor,
    W_right: torch.Tensor,
    normals: torch.Tensor,
    gamma: float,
    density_floor: float,
    pressure_floor: float,
) -> tuple[torch.Tensor, torch.Tensor]:
    W_left = W_left.clone()
    W_right = W_right.clone()
    W_left[:, 0] = W_left[:, 0].clamp_min(density_floor)
    W_right[:, 0] = W_right[:, 0].clamp_min(density_floor)
    W_left[:, -1] = W_left[:, -1].clamp_min(pressure_floor)
    W_right[:, -1] = W_right[:, -1].clamp_min(pressure_floor)

    U_left = primitive_to_conservative_torch(W_left, gamma)
    U_right = primitive_to_conservative_torch(W_right, gamma)
    flux_left = _euler_flux_normal_torch(W_left, normals, gamma)
    flux_right = _euler_flux_normal_torch(W_right, normals, gamma)

    velocity_left = W_left[:, 1:-1]
    velocity_right = W_right[:, 1:-1]
    sound_left = torch.sqrt(gamma * W_left[:, -1] / W_left[:, 0])
    sound_right = torch.sqrt(gamma * W_right[:, -1] / W_right[:, 0])
    un_left = (velocity_left * normals).sum(dim=1).abs()
    un_right = (velocity_right * normals).sum(dim=1).abs()
    wavespeed = torch.maximum(un_left + sound_left, un_right + sound_right)
    flux = 0.5 * (flux_left + flux_right) - 0.5 * wavespeed[:, None] * (
        U_right - U_left
    )
    return flux, wavespeed


def _euler_primitive_time_derivative_torch(
    W: torch.Tensor,
    grad_W: torch.Tensor,
    gamma: float,
) -> torch.Tensor:
    rho = W[:, 0]
    velocity = W[:, 1:-1]
    pressure = W[:, -1]
    grad_rho = grad_W[:, :, 0]
    grad_velocity = grad_W[:, :, 1:-1]
    grad_pressure = grad_W[:, :, -1]

    def advect(grad: torch.Tensor) -> torch.Tensor:
        return (velocity * grad).sum(dim=1)

    div_velocity = torch.diagonal(grad_velocity, dim1=1, dim2=2).sum(dim=1)

    dW = torch.empty_like(W)
    dW[:, 0] = -(advect(grad_rho) + rho * div_velocity)
    for dim in range(velocity.shape[1]):
        dW[:, 1 + dim] = -(
            advect(grad_velocity[:, :, dim]) + grad_pressure[:, dim] / rho
        )
    dW[:, -1] = -(advect(grad_pressure) + gamma * pressure * div_velocity)
    return dW


def _cell_geometry_torch(
    points: torch.Tensor,
    cells: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Compute simplex centroids and volumes from connectivity."""
    cell_points = points[cells.to(dtype=torch.int64)]
    centroids = cell_points.mean(dim=1)
    if points.shape[1] == 2:
        p0, p1, p2 = cell_points[:, 0], cell_points[:, 1], cell_points[:, 2]
        volumes = 0.5 * torch.abs(
            (p1[:, 0] - p0[:, 0]) * (p2[:, 1] - p0[:, 1])
            - (p1[:, 1] - p0[:, 1]) * (p2[:, 0] - p0[:, 0])
        )
    else:
        p0, p1, p2, p3 = (
            cell_points[:, 0],
            cell_points[:, 1],
            cell_points[:, 2],
            cell_points[:, 3],
        )
        volumes = (
            torch.abs(
                torch.einsum("bi,bi->b", p1 - p0, torch.cross(p2 - p0, p3 - p0, dim=-1))
            )
            / 6.0
        )
    return centroids, volumes.clamp_min(1.0e-12)


def _local_face_geometry_torch(
    points: torch.Tensor,
    cells: torch.Tensor,
    cell_centroids: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Compute local face centroids and outward area vectors for each cell."""
    cell_points = points[cells.to(dtype=torch.int64)]
    dims = points.shape[1]
    n_faces = cells.shape[1]
    face_centroids = []
    area_vectors = []
    for face_idx in range(n_faces):
        face_local = [idx for idx in range(n_faces) if idx != face_idx]
        verts = cell_points[:, face_local]
        face_center = verts.mean(dim=1)
        if dims == 2:
            edge = verts[:, 1] - verts[:, 0]
            area_vector = torch.stack((edge[:, 1], -edge[:, 0]), dim=1)
        else:
            area_vector = 0.5 * torch.cross(
                verts[:, 1] - verts[:, 0], verts[:, 2] - verts[:, 0], dim=-1
            )
        to_face = face_center - cell_centroids
        flip = (area_vector * to_face).sum(dim=1) < 0.0
        area_vector = torch.where(flip[:, None], -area_vector, area_vector)
        face_centroids.append(face_center)
        area_vectors.append(area_vector)

    face_centroids_t = torch.stack(face_centroids, dim=1)
    area_vectors_t = torch.stack(area_vectors, dim=1)
    face_areas_t = area_vectors_t.norm(dim=2)
    return face_centroids_t, area_vectors_t, face_areas_t


def _euler_barth_jespersen_phi_torch(
    cell_values: torch.Tensor,
    cell_gradients: torch.Tensor,
    cell_centroids: torch.Tensor,
    face_centroids: torch.Tensor,
    cell_neighbors: torch.Tensor,
    eps: float = 1.0e-12,
) -> torch.Tensor:
    """Compute componentwise Barth-Jespersen limiter coefficients."""
    n_components = cell_values.shape[1]
    mins = cell_values.clone()
    maxs = cell_values.clone()

    neighbors_i64 = cell_neighbors.to(dtype=torch.int64)
    for face_idx in range(cell_neighbors.shape[1]):
        neighbors = neighbors_i64[:, face_idx]
        interior = neighbors >= 0
        if bool(torch.any(interior).item()):
            active_neighbors = neighbors[interior]
            neighbor_values = cell_values[active_neighbors]
            mins[interior] = torch.minimum(mins[interior], neighbor_values)
            maxs[interior] = torch.maximum(maxs[interior], neighbor_values)

    phi = torch.ones(
        (cell_values.shape[0], n_components),
        device=cell_values.device,
        dtype=cell_values.dtype,
    )
    for face_idx in range(cell_neighbors.shape[1]):
        dx = face_centroids[:, face_idx] - cell_centroids
        delta = torch.einsum("cd,cdk->ck", dx, cell_gradients)
        values = cell_values
        ratio = torch.ones_like(delta)
        positive = delta > eps
        negative = delta < -eps
        ratio[positive] = (maxs - values)[positive] / delta[positive]
        ratio[negative] = (mins - values)[negative] / delta[negative]
        ratio = ratio.clamp(0.0, 1.0)
        phi = torch.minimum(phi, ratio)
    return phi


def _linear_reconstruct_to_local_faces_torch(
    cell_values: torch.Tensor,
    cell_gradients: torch.Tensor,
    cell_centroids: torch.Tensor,
    face_centroids: torch.Tensor,
    cell_neighbors: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor]:
    dx = face_centroids - cell_centroids[:, None, :]
    left = cell_values[:, None, :] + torch.einsum("cfd,cdk->cfk", dx, cell_gradients)

    right_cells = cell_neighbors.to(dtype=torch.int64).clamp_min(0)
    right = cell_values[right_cells].clone()
    interior = cell_neighbors >= 0
    if bool(torch.any(interior).item()):
        neighbor_dx = face_centroids - cell_centroids[right_cells]
        right[interior] = (
            right[interior]
            + torch.einsum("cfd,cfdk->cfk", neighbor_dx, cell_gradients[right_cells])[
                interior
            ]
        )
    return left, right


def euler_cfl_timestep_torch(
    cell_values: torch.Tensor,
    points: torch.Tensor,
    cells: torch.Tensor,
    cell_neighbors: torch.Tensor,
    boundary_tags: torch.Tensor,
    inflow_state: torch.Tensor,
    gamma: float,
    cfl: float,
    density_floor: float,
    pressure_floor: float,
    eps: float = 1.0e-12,
) -> torch.Tensor:
    """Compute the global explicit Euler CFL timestep."""
    centroids, volumes = _cell_geometry_torch(points, cells)
    _, area_vectors, face_areas = _local_face_geometry_torch(points, cells, centroids)
    normals = area_vectors / face_areas[..., None].clamp_min(eps)
    right_cells = cell_neighbors.to(dtype=torch.int64).clamp_min(0)
    spectral = torch.zeros_like(volumes)
    for face_idx in range(cells.shape[1]):
        W_left = cell_values
        W_right = cell_values[right_cells[:, face_idx]].clone()
        boundary = cell_neighbors[:, face_idx] < 0
        if bool(torch.any(boundary).item()):
            W_right[boundary] = _apply_euler_boundary_state_torch(
                W_left[boundary],
                normals[:, face_idx][boundary],
                boundary_tags[:, face_idx][boundary],
                inflow_state,
            )
        _, wavespeeds = _rusanov_euler_flux_torch(
            W_left,
            W_right,
            normals[:, face_idx],
            gamma,
            density_floor,
            pressure_floor,
        )
        spectral = spectral + face_areas[:, face_idx] * wavespeeds.clamp_min(0.0)
    return float(cfl) * torch.min(volumes / spectral.clamp_min(float(eps)))


def euler_update_torch(
    U: torch.Tensor,
    cell_values: torch.Tensor,
    cell_gradients: torch.Tensor,
    points: torch.Tensor,
    cells: torch.Tensor,
    cell_neighbors: torch.Tensor,
    boundary_tags: torch.Tensor,
    inflow_state: torch.Tensor,
    dt: torch.Tensor,
    gamma: float,
    density_floor: float,
    pressure_floor: float,
) -> torch.Tensor:
    """Torch backend for the same MUSCL-Hancock Euler update as the Warp kernel."""
    cell_centroids, cell_volumes = _cell_geometry_torch(points, cells)
    face_centroids, face_area_vectors, face_areas = _local_face_geometry_torch(
        points, cells, cell_centroids
    )
    phi = _euler_barth_jespersen_phi_torch(
        cell_values,
        cell_gradients,
        cell_centroids,
        face_centroids,
        cell_neighbors,
    )
    limited_gradients = cell_gradients * phi[:, None, :]
    W_half = cell_values + 0.5 * dt * _euler_primitive_time_derivative_torch(
        cell_values, limited_gradients, gamma
    )
    W_half[:, 0] = W_half[:, 0].clamp_min(density_floor)
    W_half[:, -1] = W_half[:, -1].clamp_min(pressure_floor)

    W_left, W_right = _linear_reconstruct_to_local_faces_torch(
        W_half,
        limited_gradients,
        cell_centroids,
        face_centroids,
        cell_neighbors,
    )
    W_left[..., 0] = W_left[..., 0].clamp_min(density_floor)
    W_left[..., -1] = W_left[..., -1].clamp_min(pressure_floor)
    W_right[..., 0] = W_right[..., 0].clamp_min(density_floor)
    W_right[..., -1] = W_right[..., -1].clamp_min(pressure_floor)

    normals = face_area_vectors / face_areas[..., None].clamp_min(1.0e-12)
    boundary = cell_neighbors < 0
    if bool(torch.any(boundary).item()):
        W_right[boundary] = _apply_euler_boundary_state_torch(
            W_left[boundary],
            normals[boundary],
            boundary_tags[boundary],
            inflow_state,
        )
    flux, _ = _rusanov_euler_flux_torch(
        W_left.reshape(-1, W_left.shape[-1]),
        W_right.reshape(-1, W_right.shape[-1]),
        normals.reshape(-1, normals.shape[-1]),
        gamma,
        density_floor,
        pressure_floor,
    )
    flux = flux.reshape_as(W_left) * face_areas[..., None]

    U_next = U.clone()
    U_next = U_next - dt * flux.sum(dim=1) / cell_volumes[:, None]
    return U_next


if _WARP_AVAILABLE:

    @wp.kernel
    def _conservative_to_primitive_kernel(
        U: wp.array2d(dtype=wp.float32),
        gamma: float,
        density_floor: float,
        pressure_floor: float,
        dims: int,
        W: wp.array2d(dtype=wp.float32),
    ):
        cell = wp.tid()
        rho = wp.max(U[cell, 0], density_floor)
        inv_rho = 1.0 / rho
        speed2 = float(0.0)
        for d in range(dims):
            velocity = U[cell, 1 + d] * inv_rho
            W[cell, 1 + d] = velocity
            speed2 = speed2 + velocity * velocity
        pressure = (gamma - 1.0) * (U[cell, dims + 1] - 0.5 * rho * speed2)

        W[cell, 0] = rho
        W[cell, dims + 1] = wp.max(pressure, pressure_floor)

    @wp.func
    def _select_phi(
        comp: int,
        phi_0: float,
        phi_1: float,
        phi_2: float,
        phi_3: float,
        phi_4: float,
    ):
        if comp == 0:
            return phi_0
        elif comp == 1:
            return phi_1
        elif comp == 2:
            return phi_2
        elif comp == 3:
            return phi_3
        return phi_4

    @wp.func
    def _cell_centroid_vec(
        points: wp.array2d(dtype=wp.float32),
        cells: wp.array2d(dtype=wp.int32),
        cell: int,
        dims: int,
    ):
        centroid = wp.vec3(0.0, 0.0, 0.0)
        inv_count = 1.0 / float(dims + 1)
        for local_vertex in range(4):
            if local_vertex < dims + 1:
                point = cells[cell, local_vertex]
                centroid[0] = centroid[0] + points[point, 0] * inv_count
                centroid[1] = centroid[1] + points[point, 1] * inv_count
                if dims == 3:
                    centroid[2] = centroid[2] + points[point, 2] * inv_count
        return centroid

    @wp.func
    def _cell_volume(
        points: wp.array2d(dtype=wp.float32),
        cells: wp.array2d(dtype=wp.int32),
        cell: int,
        dims: int,
    ):
        p0 = cells[cell, 0]
        p1 = cells[cell, 1]
        p2 = cells[cell, 2]
        if dims == 2:
            x10 = points[p1, 0] - points[p0, 0]
            y10 = points[p1, 1] - points[p0, 1]
            x20 = points[p2, 0] - points[p0, 0]
            y20 = points[p2, 1] - points[p0, 1]
            return wp.max(0.5 * wp.abs(x10 * y20 - y10 * x20), 1.0e-30)

        p3 = cells[cell, 3]
        a = wp.vec3(
            points[p1, 0] - points[p0, 0],
            points[p1, 1] - points[p0, 1],
            points[p1, 2] - points[p0, 2],
        )
        b = wp.vec3(
            points[p2, 0] - points[p0, 0],
            points[p2, 1] - points[p0, 1],
            points[p2, 2] - points[p0, 2],
        )
        c = wp.vec3(
            points[p3, 0] - points[p0, 0],
            points[p3, 1] - points[p0, 1],
            points[p3, 2] - points[p0, 2],
        )
        return wp.max(wp.abs(wp.dot(a, wp.cross(b, c))) / 6.0, 1.0e-30)

    @wp.func
    def _local_face_centroid_vec(
        points: wp.array2d(dtype=wp.float32),
        cells: wp.array2d(dtype=wp.int32),
        cell: int,
        local_face: int,
        dims: int,
    ):
        centroid = wp.vec3(0.0, 0.0, 0.0)
        inv_count = 1.0 / float(dims)
        for local_vertex in range(4):
            if local_vertex < dims + 1 and local_vertex != local_face:
                point = cells[cell, local_vertex]
                centroid[0] = centroid[0] + points[point, 0] * inv_count
                centroid[1] = centroid[1] + points[point, 1] * inv_count
                if dims == 3:
                    centroid[2] = centroid[2] + points[point, 2] * inv_count
        return centroid

    @wp.func
    def _local_face_area_vector(
        points: wp.array2d(dtype=wp.float32),
        cells: wp.array2d(dtype=wp.int32),
        cell: int,
        local_face: int,
        cell_centroid: wp.vec3,
        face_centroid: wp.vec3,
        dims: int,
    ):
        a = int(-1)
        b = int(-1)
        c = int(-1)
        count = int(0)
        for local_vertex in range(4):
            if local_vertex < dims + 1 and local_vertex != local_face:
                point = cells[cell, local_vertex]
                if count == 0:
                    a = point
                elif count == 1:
                    b = point
                else:
                    c = point
                count = count + 1

        if dims == 2:
            edge_x = points[b, 0] - points[a, 0]
            edge_y = points[b, 1] - points[a, 1]
            area_vector = wp.vec3(edge_y, -edge_x, 0.0)
        else:
            ab = wp.vec3(
                points[b, 0] - points[a, 0],
                points[b, 1] - points[a, 1],
                points[b, 2] - points[a, 2],
            )
            ac = wp.vec3(
                points[c, 0] - points[a, 0],
                points[c, 1] - points[a, 1],
                points[c, 2] - points[a, 2],
            )
            area_vector = 0.5 * wp.cross(ab, ac)

        if wp.dot(area_vector, face_centroid - cell_centroid) < 0.0:
            area_vector = -area_vector
        return area_vector

    @wp.func
    def _cell_limiter_phi(
        points: wp.array2d(dtype=wp.float32),
        cells: wp.array2d(dtype=wp.int32),
        values: wp.array2d(dtype=wp.float32),
        gradients: wp.array3d(dtype=wp.float32),
        neighbors: wp.array2d(dtype=wp.int32),
        cell: int,
        comp: int,
        dims: int,
        n_cell_faces: int,
        eps: float,
    ):
        value = values[cell, comp]
        value_min = value
        value_max = value
        for local_face in range(n_cell_faces):
            neighbor = neighbors[cell, local_face]
            if neighbor >= 0:
                neighbor_value = values[neighbor, comp]
                value_min = wp.min(value_min, neighbor_value)
                value_max = wp.max(value_max, neighbor_value)

        cell_centroid = _cell_centroid_vec(points, cells, cell, dims)
        phi = float(1.0)
        for local_face in range(n_cell_faces):
            face_centroid = _local_face_centroid_vec(
                points, cells, cell, local_face, dims
            )
            delta = float(0.0)
            for d in range(dims):
                dx = face_centroid[d] - cell_centroid[d]
                delta = delta + dx * gradients[cell, d, comp]

            ratio = float(1.0)
            if delta > eps:
                ratio = (value_max - value) / delta
            elif delta < -eps:
                ratio = (value_min - value) / delta
            phi = wp.min(phi, wp.min(wp.max(ratio, 0.0), 1.0))
        return phi

    @wp.func
    def _limited_grad(
        gradients: wp.array3d(dtype=wp.float32),
        cell: int,
        dim: int,
        comp: int,
        phi_0: float,
        phi_1: float,
        phi_2: float,
        phi_3: float,
        phi_4: float,
    ):
        return gradients[cell, dim, comp] * _select_phi(
            comp, phi_0, phi_1, phi_2, phi_3, phi_4
        )

    @wp.func
    def _predicted_value(
        values: wp.array2d(dtype=wp.float32),
        gradients: wp.array3d(dtype=wp.float32),
        cell: int,
        comp: int,
        dt: float,
        gamma: float,
        dims: int,
        phi_0: float,
        phi_1: float,
        phi_2: float,
        phi_3: float,
        phi_4: float,
    ):
        rho = wp.max(values[cell, 0], 1.0e-30)
        pressure = values[cell, dims + 1]

        advected = float(0.0)
        div_velocity = float(0.0)
        for d in range(dims):
            velocity_d = values[cell, 1 + d]
            advected = advected + velocity_d * _limited_grad(
                gradients, cell, d, comp, phi_0, phi_1, phi_2, phi_3, phi_4
            )
            div_velocity = div_velocity + _limited_grad(
                gradients, cell, d, 1 + d, phi_0, phi_1, phi_2, phi_3, phi_4
            )

        derivative = -advected
        if comp == 0:
            derivative = derivative - rho * div_velocity
        elif comp == dims + 1:
            derivative = derivative - gamma * pressure * div_velocity
        else:
            velocity_dim = comp - 1
            derivative = (
                derivative
                - _limited_grad(
                    gradients,
                    cell,
                    velocity_dim,
                    dims + 1,
                    phi_0,
                    phi_1,
                    phi_2,
                    phi_3,
                    phi_4,
                )
                / rho
            )

        return values[cell, comp] + 0.5 * dt * derivative

    @wp.func
    def _reconstruct_predicted(
        values: wp.array2d(dtype=wp.float32),
        gradients: wp.array3d(dtype=wp.float32),
        cell_centroid: wp.vec3,
        face_centroid: wp.vec3,
        cell: int,
        comp: int,
        dt: float,
        gamma: float,
        dims: int,
        phi_0: float,
        phi_1: float,
        phi_2: float,
        phi_3: float,
        phi_4: float,
    ):
        value = _predicted_value(
            values,
            gradients,
            cell,
            comp,
            dt,
            gamma,
            dims,
            phi_0,
            phi_1,
            phi_2,
            phi_3,
            phi_4,
        )
        for d in range(dims):
            dx = face_centroid[d] - cell_centroid[d]
            value = value + dx * _limited_grad(
                gradients, cell, d, comp, phi_0, phi_1, phi_2, phi_3, phi_4
            )
        return value

    @wp.func
    def _primitive_energy(
        rho: float,
        u: float,
        v: float,
        w: float,
        pressure: float,
        gamma: float,
        dims: int,
    ):
        speed2 = u * u + v * v
        if dims == 3:
            speed2 = speed2 + w * w
        return pressure / (gamma - 1.0) + 0.5 * rho * speed2

    @wp.func
    def _normal_flux_component(
        comp: int,
        rho: float,
        u: float,
        v: float,
        w: float,
        pressure: float,
        energy: float,
        un: float,
        nx: float,
        ny: float,
        nz: float,
        dims: int,
    ):
        if comp == 0:
            return rho * un
        elif comp == 1:
            return rho * u * un + pressure * nx
        elif comp == 2:
            return rho * v * un + pressure * ny
        elif comp == 3 and dims == 3:
            return rho * w * un + pressure * nz
        else:
            return (energy + pressure) * un

    @wp.func
    def _conservative_component(
        comp: int,
        rho: float,
        u: float,
        v: float,
        w: float,
        energy: float,
        dims: int,
    ):
        if comp == 0:
            return rho
        elif comp == 1:
            return rho * u
        elif comp == 2:
            return rho * v
        elif comp == 3 and dims == 3:
            return rho * w
        else:
            return energy

    @wp.kernel
    def _euler_cfl_cell_kernel(
        values: wp.array2d(dtype=wp.float32),
        points: wp.array2d(dtype=wp.float32),
        cells: wp.array2d(dtype=wp.int32),
        neighbors: wp.array2d(dtype=wp.int32),
        boundary_tags: wp.array2d(dtype=wp.int32),
        inflow: wp.array(dtype=wp.float32),
        gamma: float,
        density_floor: float,
        pressure_floor: float,
        dims: int,
        spectral: wp.array(dtype=wp.float32),
    ):
        cell = wp.tid()
        n_cell_faces = dims + 1
        cell_centroid = _cell_centroid_vec(points, cells, cell, dims)
        spectral_sum = float(0.0)

        rho_l = wp.max(values[cell, 0], density_floor)
        u_l = values[cell, 1]
        v_l = values[cell, 2]
        w_l = float(0.0)
        if dims == 3:
            w_l = values[cell, 3]
        p_l = wp.max(values[cell, dims + 1], pressure_floor)

        for local_face in range(4):
            if local_face < n_cell_faces:
                neighbor = neighbors[cell, local_face]
                face_centroid = _local_face_centroid_vec(
                    points, cells, cell, local_face, dims
                )
                area_vector = _local_face_area_vector(
                    points, cells, cell, local_face, cell_centroid, face_centroid, dims
                )
                area = wp.max(wp.length(area_vector), 1.0e-30)
                nx = area_vector[0] / area
                ny = area_vector[1] / area
                nz = float(0.0)
                if dims == 3:
                    nz = area_vector[2] / area

                rho_r = rho_l
                u_r = u_l
                v_r = v_l
                w_r = w_l
                p_r = p_l
                if neighbor >= 0:
                    rho_r = wp.max(values[neighbor, 0], density_floor)
                    u_r = values[neighbor, 1]
                    v_r = values[neighbor, 2]
                    if dims == 3:
                        w_r = values[neighbor, 3]
                    p_r = wp.max(values[neighbor, dims + 1], pressure_floor)
                elif boundary_tags[cell, local_face] == INFLOW_SUPERSONIC:
                    rho_r = wp.max(inflow[0], density_floor)
                    u_r = inflow[1]
                    v_r = inflow[2]
                    if dims == 3:
                        w_r = inflow[3]
                    p_r = wp.max(inflow[dims + 1], pressure_floor)
                elif boundary_tags[cell, local_face] == WALL_SLIP:
                    un_l = u_l * nx + v_l * ny + w_l * nz
                    u_r = u_l - 2.0 * un_l * nx
                    v_r = v_l - 2.0 * un_l * ny
                    if dims == 3:
                        w_r = w_l - 2.0 * un_l * nz

                un_l_abs = wp.abs(u_l * nx + v_l * ny + w_l * nz)
                un_r_abs = wp.abs(u_r * nx + v_r * ny + w_r * nz)
                c_l = wp.sqrt(gamma * p_l / rho_l)
                c_r = wp.sqrt(gamma * p_r / rho_r)
                spectral_sum = spectral_sum + area * wp.max(
                    un_l_abs + c_l, un_r_abs + c_r
                )

        spectral[cell] = spectral_sum

    @wp.func
    def _euler_update_cell(
        cell: int,
        U_next: wp.array2d(dtype=wp.float32),
        points: wp.array2d(dtype=wp.float32),
        cells: wp.array2d(dtype=wp.int32),
        values: wp.array2d(dtype=wp.float32),
        gradients: wp.array3d(dtype=wp.float32),
        neighbors: wp.array2d(dtype=wp.int32),
        boundary_tags: wp.array2d(dtype=wp.int32),
        inflow: wp.array(dtype=wp.float32),
        dt: float,
        gamma: float,
        density_floor: float,
        pressure_floor: float,
        dims: int,
        n_cell_faces: int,
    ):
        cell_centroid = _cell_centroid_vec(points, cells, cell, dims)
        cell_volume = _cell_volume(points, cells, cell, dims)
        phi_0 = _cell_limiter_phi(
            points,
            cells,
            values,
            gradients,
            neighbors,
            cell,
            0,
            dims,
            n_cell_faces,
            1.0e-12,
        )
        phi_1 = _cell_limiter_phi(
            points,
            cells,
            values,
            gradients,
            neighbors,
            cell,
            1,
            dims,
            n_cell_faces,
            1.0e-12,
        )
        phi_2 = _cell_limiter_phi(
            points,
            cells,
            values,
            gradients,
            neighbors,
            cell,
            2,
            dims,
            n_cell_faces,
            1.0e-12,
        )
        phi_3 = _cell_limiter_phi(
            points,
            cells,
            values,
            gradients,
            neighbors,
            cell,
            3,
            dims,
            n_cell_faces,
            1.0e-12,
        )
        phi_4 = float(1.0)
        if dims == 3:
            phi_4 = _cell_limiter_phi(
                points,
                cells,
                values,
                gradients,
                neighbors,
                cell,
                4,
                dims,
                n_cell_faces,
                1.0e-12,
            )

        for local_face in range(n_cell_faces):
            other = neighbors[cell, local_face]
            face_centroid = _local_face_centroid_vec(
                points, cells, cell, local_face, dims
            )
            area_vector = _local_face_area_vector(
                points, cells, cell, local_face, cell_centroid, face_centroid, dims
            )
            area = wp.max(wp.length(area_vector), 1.0e-30)
            nx = area_vector[0] / area
            ny = area_vector[1] / area
            nz = float(0.0)
            if dims == 3:
                nz = area_vector[2] / area

            rho_l = _reconstruct_predicted(
                values,
                gradients,
                cell_centroid,
                face_centroid,
                cell,
                0,
                dt,
                gamma,
                dims,
                phi_0,
                phi_1,
                phi_2,
                phi_3,
                phi_4,
            )
            u_l = _reconstruct_predicted(
                values,
                gradients,
                cell_centroid,
                face_centroid,
                cell,
                1,
                dt,
                gamma,
                dims,
                phi_0,
                phi_1,
                phi_2,
                phi_3,
                phi_4,
            )
            v_l = _reconstruct_predicted(
                values,
                gradients,
                cell_centroid,
                face_centroid,
                cell,
                2,
                dt,
                gamma,
                dims,
                phi_0,
                phi_1,
                phi_2,
                phi_3,
                phi_4,
            )
            w_l = float(0.0)
            if dims == 3:
                w_l = _reconstruct_predicted(
                    values,
                    gradients,
                    cell_centroid,
                    face_centroid,
                    cell,
                    3,
                    dt,
                    gamma,
                    dims,
                    phi_0,
                    phi_1,
                    phi_2,
                    phi_3,
                    phi_4,
                )
            p_l = _reconstruct_predicted(
                values,
                gradients,
                cell_centroid,
                face_centroid,
                cell,
                dims + 1,
                dt,
                gamma,
                dims,
                phi_0,
                phi_1,
                phi_2,
                phi_3,
                phi_4,
            )
            rho_l = wp.max(rho_l, density_floor)
            p_l = wp.max(p_l, pressure_floor)

            rho_r = rho_l
            u_r = u_l
            v_r = v_l
            w_r = w_l
            p_r = p_l
            if other >= 0:
                other_centroid = _cell_centroid_vec(points, cells, other, dims)
                other_phi_0 = _cell_limiter_phi(
                    points,
                    cells,
                    values,
                    gradients,
                    neighbors,
                    other,
                    0,
                    dims,
                    n_cell_faces,
                    1.0e-12,
                )
                other_phi_1 = _cell_limiter_phi(
                    points,
                    cells,
                    values,
                    gradients,
                    neighbors,
                    other,
                    1,
                    dims,
                    n_cell_faces,
                    1.0e-12,
                )
                other_phi_2 = _cell_limiter_phi(
                    points,
                    cells,
                    values,
                    gradients,
                    neighbors,
                    other,
                    2,
                    dims,
                    n_cell_faces,
                    1.0e-12,
                )
                other_phi_3 = _cell_limiter_phi(
                    points,
                    cells,
                    values,
                    gradients,
                    neighbors,
                    other,
                    3,
                    dims,
                    n_cell_faces,
                    1.0e-12,
                )
                other_phi_4 = float(1.0)
                if dims == 3:
                    other_phi_4 = _cell_limiter_phi(
                        points,
                        cells,
                        values,
                        gradients,
                        neighbors,
                        other,
                        4,
                        dims,
                        n_cell_faces,
                        1.0e-12,
                    )
                rho_r = _reconstruct_predicted(
                    values,
                    gradients,
                    other_centroid,
                    face_centroid,
                    other,
                    0,
                    dt,
                    gamma,
                    dims,
                    other_phi_0,
                    other_phi_1,
                    other_phi_2,
                    other_phi_3,
                    other_phi_4,
                )
                u_r = _reconstruct_predicted(
                    values,
                    gradients,
                    other_centroid,
                    face_centroid,
                    other,
                    1,
                    dt,
                    gamma,
                    dims,
                    other_phi_0,
                    other_phi_1,
                    other_phi_2,
                    other_phi_3,
                    other_phi_4,
                )
                v_r = _reconstruct_predicted(
                    values,
                    gradients,
                    other_centroid,
                    face_centroid,
                    other,
                    2,
                    dt,
                    gamma,
                    dims,
                    other_phi_0,
                    other_phi_1,
                    other_phi_2,
                    other_phi_3,
                    other_phi_4,
                )
                if dims == 3:
                    w_r = _reconstruct_predicted(
                        values,
                        gradients,
                        other_centroid,
                        face_centroid,
                        other,
                        3,
                        dt,
                        gamma,
                        dims,
                        other_phi_0,
                        other_phi_1,
                        other_phi_2,
                        other_phi_3,
                        other_phi_4,
                    )
                p_r = _reconstruct_predicted(
                    values,
                    gradients,
                    other_centroid,
                    face_centroid,
                    other,
                    dims + 1,
                    dt,
                    gamma,
                    dims,
                    other_phi_0,
                    other_phi_1,
                    other_phi_2,
                    other_phi_3,
                    other_phi_4,
                )
                rho_r = wp.max(rho_r, density_floor)
                p_r = wp.max(p_r, pressure_floor)
            elif boundary_tags[cell, local_face] == INFLOW_SUPERSONIC:
                rho_r = wp.max(inflow[0], density_floor)
                u_r = inflow[1]
                v_r = inflow[2]
                if dims == 3:
                    w_r = inflow[3]
                p_r = wp.max(inflow[dims + 1], pressure_floor)
            elif boundary_tags[cell, local_face] == WALL_SLIP:
                un_l_boundary = u_l * nx + v_l * ny + w_l * nz
                u_r = u_l - 2.0 * un_l_boundary * nx
                v_r = v_l - 2.0 * un_l_boundary * ny
                if dims == 3:
                    w_r = w_l - 2.0 * un_l_boundary * nz

            energy_l = _primitive_energy(rho_l, u_l, v_l, w_l, p_l, gamma, dims)
            energy_r = _primitive_energy(rho_r, u_r, v_r, w_r, p_r, gamma, dims)
            un_l = u_l * nx + v_l * ny + w_l * nz
            un_r = u_r * nx + v_r * ny + w_r * nz
            c_l = wp.sqrt(gamma * p_l / rho_l)
            c_r = wp.sqrt(gamma * p_r / rho_r)
            speed = wp.max(wp.abs(un_l) + c_l, wp.abs(un_r) + c_r)

            for comp in range(5):
                if comp < dims + 2:
                    flux_l = _normal_flux_component(
                        comp,
                        rho_l,
                        u_l,
                        v_l,
                        w_l,
                        p_l,
                        energy_l,
                        un_l,
                        nx,
                        ny,
                        nz,
                        dims,
                    )
                    flux_r = _normal_flux_component(
                        comp,
                        rho_r,
                        u_r,
                        v_r,
                        w_r,
                        p_r,
                        energy_r,
                        un_r,
                        nx,
                        ny,
                        nz,
                        dims,
                    )
                    conserved_l = _conservative_component(
                        comp, rho_l, u_l, v_l, w_l, energy_l, dims
                    )
                    conserved_r = _conservative_component(
                        comp, rho_r, u_r, v_r, w_r, energy_r, dims
                    )
                    flux = area * (
                        0.5 * (flux_l + flux_r)
                        - 0.5 * speed * (conserved_r - conserved_l)
                    )
                    U_next[cell, comp] = U_next[cell, comp] - dt * flux / cell_volume

    @wp.kernel
    def _euler_update_2d_cell_kernel(
        U_next: wp.array2d(dtype=wp.float32),
        points: wp.array2d(dtype=wp.float32),
        cells: wp.array2d(dtype=wp.int32),
        values: wp.array2d(dtype=wp.float32),
        gradients: wp.array3d(dtype=wp.float32),
        neighbors: wp.array2d(dtype=wp.int32),
        boundary_tags: wp.array2d(dtype=wp.int32),
        inflow: wp.array(dtype=wp.float32),
        dt: float,
        gamma: float,
        density_floor: float,
        pressure_floor: float,
        n_cell_faces: int,
    ):
        _euler_update_cell(
            wp.tid(),
            U_next,
            points,
            cells,
            values,
            gradients,
            neighbors,
            boundary_tags,
            inflow,
            dt,
            gamma,
            density_floor,
            pressure_floor,
            2,
            n_cell_faces,
        )

    @wp.kernel
    def _euler_update_3d_cell_kernel(
        U_next: wp.array2d(dtype=wp.float32),
        points: wp.array2d(dtype=wp.float32),
        cells: wp.array2d(dtype=wp.int32),
        values: wp.array2d(dtype=wp.float32),
        gradients: wp.array3d(dtype=wp.float32),
        neighbors: wp.array2d(dtype=wp.int32),
        boundary_tags: wp.array2d(dtype=wp.int32),
        inflow: wp.array(dtype=wp.float32),
        dt: float,
        gamma: float,
        density_floor: float,
        pressure_floor: float,
        n_cell_faces: int,
    ):
        _euler_update_cell(
            wp.tid(),
            U_next,
            points,
            cells,
            values,
            gradients,
            neighbors,
            boundary_tags,
            inflow,
            dt,
            gamma,
            density_floor,
            pressure_floor,
            3,
            n_cell_faces,
        )


def euler_conservative_to_primitive_warp(
    U: torch.Tensor,
    gamma: float,
    density_floor: float,
    pressure_floor: float,
) -> torch.Tensor:
    """Convert Euler conserved variables to primitives with one Warp cell kernel."""
    _require_warp()
    dims = U.shape[1] - 2
    U_fp32 = U.to(dtype=torch.float32).contiguous()
    W = torch.empty_like(U_fp32)
    wp_device, wp_stream = _launch_context(U_fp32)
    with wp.ScopedStream(wp_stream):
        wp.launch(
            _conservative_to_primitive_kernel,
            dim=U.shape[0],
            inputs=[
                wp.from_torch(U_fp32, dtype=wp.float32),
                float(gamma),
                float(density_floor),
                float(pressure_floor),
                int(dims),
                wp.from_torch(W, dtype=wp.float32),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    return W.to(dtype=U.dtype)


def euler_cfl_timestep_warp(
    cell_values: torch.Tensor,
    points: torch.Tensor,
    cells: torch.Tensor,
    cell_neighbors: torch.Tensor,
    boundary_tags: torch.Tensor,
    inflow_state: torch.Tensor,
    gamma: float,
    cfl: float,
    density_floor: float,
    pressure_floor: float,
    eps: float = 1.0e-12,
) -> torch.Tensor:
    """Compute the explicit Euler CFL timestep without face-state tensors."""
    _require_warp()
    dims = cell_values.shape[1] - 2
    n_cells = cell_values.shape[0]

    values_fp32 = cell_values.to(dtype=torch.float32).contiguous()
    points_fp32 = points.to(dtype=torch.float32).contiguous()
    cells_i32 = cells.to(dtype=torch.int32).contiguous()
    neighbors_i32 = cell_neighbors.to(dtype=torch.int32).contiguous()
    tags_i32 = boundary_tags.to(dtype=torch.int32).contiguous()
    inflow_fp32 = inflow_state.to(
        device=cell_values.device, dtype=torch.float32
    ).contiguous()
    spectral = torch.zeros((n_cells,), device=cell_values.device, dtype=torch.float32)

    wp_device, wp_stream = _launch_context(values_fp32)
    with wp.ScopedStream(wp_stream):
        wp.launch(
            _euler_cfl_cell_kernel,
            dim=n_cells,
            inputs=[
                wp.from_torch(values_fp32, dtype=wp.float32),
                wp.from_torch(points_fp32, dtype=wp.float32),
                wp.from_torch(cells_i32, dtype=wp.int32),
                wp.from_torch(neighbors_i32, dtype=wp.int32),
                wp.from_torch(tags_i32, dtype=wp.int32),
                wp.from_torch(inflow_fp32, dtype=wp.float32),
                float(gamma),
                float(density_floor),
                float(pressure_floor),
                int(dims),
                wp.from_torch(spectral, dtype=wp.float32),
            ],
            device=wp_device,
            stream=wp_stream,
        )

    _, volumes = _cell_geometry_torch(points_fp32, cells_i32)
    dt = float(cfl) * torch.min(volumes / spectral.clamp_min(float(eps)))
    return dt.to(dtype=cell_values.dtype)


def euler_update_warp(
    U: torch.Tensor,
    cell_values: torch.Tensor,
    cell_gradients: torch.Tensor,
    points: torch.Tensor,
    cells: torch.Tensor,
    cell_neighbors: torch.Tensor,
    boundary_tags: torch.Tensor,
    inflow_state: torch.Tensor,
    dt: torch.Tensor,
    gamma: float,
    density_floor: float,
    pressure_floor: float,
) -> torch.Tensor:
    """Fuse MUSCL-Hancock reconstruction, boundary states, fluxes, and update."""
    _require_warp()
    dims = U.shape[1] - 2
    if dims not in (2, 3):
        raise ValueError("Euler update supports only 2D and 3D states")
    n_cells = U.shape[0]
    n_cell_faces = cells.shape[1]

    U_next = U.to(dtype=torch.float32).contiguous().clone()
    points_fp32 = points.to(dtype=torch.float32).contiguous()
    cells_i32 = cells.to(dtype=torch.int32).contiguous()
    values_fp32 = cell_values.to(dtype=torch.float32).contiguous()
    gradients_fp32 = cell_gradients.to(dtype=torch.float32).contiguous()
    neighbors_i32 = cell_neighbors.to(dtype=torch.int32).contiguous()
    tags_i32 = boundary_tags.to(dtype=torch.int32).contiguous()
    inflow_fp32 = inflow_state.to(device=U.device, dtype=torch.float32).contiguous()

    wp_device, wp_stream = _launch_context(U_next)
    update_kernel = (
        _euler_update_2d_cell_kernel if dims == 2 else _euler_update_3d_cell_kernel
    )
    with wp.ScopedStream(wp_stream):
        wp.launch(
            update_kernel,
            dim=n_cells,
            inputs=[
                wp.from_torch(U_next, dtype=wp.float32),
                wp.from_torch(points_fp32, dtype=wp.float32),
                wp.from_torch(cells_i32, dtype=wp.int32),
                wp.from_torch(values_fp32, dtype=wp.float32),
                wp.from_torch(gradients_fp32, dtype=wp.float32),
                wp.from_torch(neighbors_i32, dtype=wp.int32),
                wp.from_torch(tags_i32, dtype=wp.int32),
                wp.from_torch(inflow_fp32, dtype=wp.float32),
                float(dt),
                float(gamma),
                float(density_floor),
                float(pressure_floor),
                int(n_cell_faces),
            ],
            device=wp_device,
            stream=wp_stream,
        )
    return U_next.to(dtype=U.dtype)


def euler_conservative_to_primitive(
    U: torch.Tensor,
    gamma: float,
    density_floor: float,
    pressure_floor: float,
    implementation: str,
) -> torch.Tensor:
    """Convert conserved variables to primitives using the selected backend."""
    if implementation == "torch":
        return euler_conservative_to_primitive_torch(
            U, gamma, density_floor, pressure_floor
        )
    if implementation == "warp":
        return euler_conservative_to_primitive_warp(
            U, gamma, density_floor, pressure_floor
        )
    raise ValueError("solver.implementation must be 'torch' or 'warp'")


def euler_cfl_timestep(
    cell_values: torch.Tensor,
    points: torch.Tensor,
    cells: torch.Tensor,
    cell_neighbors: torch.Tensor,
    boundary_tags: torch.Tensor,
    inflow_state: torch.Tensor,
    gamma: float,
    cfl: float,
    density_floor: float,
    pressure_floor: float,
    implementation: str,
    eps: float = 1.0e-12,
) -> torch.Tensor:
    """Compute the explicit CFL timestep using the selected backend."""
    if implementation == "torch":
        return euler_cfl_timestep_torch(
            cell_values,
            points,
            cells,
            cell_neighbors,
            boundary_tags,
            inflow_state,
            gamma,
            cfl,
            density_floor,
            pressure_floor,
            eps,
        )
    if implementation == "warp":
        return euler_cfl_timestep_warp(
            cell_values,
            points,
            cells,
            cell_neighbors,
            boundary_tags,
            inflow_state,
            gamma,
            cfl,
            density_floor,
            pressure_floor,
            eps,
        )
    raise ValueError("solver.implementation must be 'torch' or 'warp'")


def euler_update(
    U: torch.Tensor,
    cell_values: torch.Tensor,
    cell_gradients: torch.Tensor,
    points: torch.Tensor,
    cells: torch.Tensor,
    cell_neighbors: torch.Tensor,
    boundary_tags: torch.Tensor,
    inflow_state: torch.Tensor,
    dt: torch.Tensor,
    gamma: float,
    density_floor: float,
    pressure_floor: float,
    implementation: str,
) -> torch.Tensor:
    """Advance conserved variables by one Euler finite-volume step."""
    if implementation == "torch":
        return euler_update_torch(
            U,
            cell_values,
            cell_gradients,
            points,
            cells,
            cell_neighbors,
            boundary_tags,
            inflow_state,
            dt,
            gamma,
            density_floor,
            pressure_floor,
        )
    if implementation == "warp":
        return euler_update_warp(
            U,
            cell_values,
            cell_gradients,
            points,
            cells,
            cell_neighbors,
            boundary_tags,
            inflow_state,
            dt,
            gamma,
            density_floor,
            pressure_floor,
        )
    raise ValueError("solver.implementation must be 'torch' or 'warp'")
