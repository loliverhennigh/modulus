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

"""ShardTensor smoke coverage for representative Mesh operation families."""

import pytest
import torch
from tensordict import TensorDict
from torch.distributed.device_mesh import DeviceMesh

from physicsnemo.mesh.mesh import Mesh
from physicsnemo.mesh.utilities._scatter_ops import scatter_aggregate

from .shard_tensor_utils import (
    SHARD_MESH_TENSOR_MODES,
    assert_adjacency_equal,
    assert_allclose,
    assert_equal,
    assert_shard_tensor,
    mesh_to_mode,
    to_mode_tensor,
)


def _surface_mesh() -> Mesh:
    """Create a two-triangle surface mesh with point and cell fields."""
    points = torch.tensor(
        [
            [0.0, 0.0, 0.0],
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [1.0, 1.0, 0.0],
        ],
        dtype=torch.float32,
    )
    cells = torch.tensor([[0, 1, 2], [1, 3, 2]], dtype=torch.int64)
    point_data = TensorDict(
        {
            "phi": points[:, 0] + 2.0 * points[:, 1],
            "velocity": torch.stack(
                [points[:, 1], -points[:, 0], torch.ones(points.shape[0])], dim=-1
            ),
        },
        batch_size=[points.shape[0]],
    )
    cell_data = TensorDict(
        {"temperature": torch.tensor([[1.0], [3.0]], dtype=torch.float32)},
        batch_size=[cells.shape[0]],
    )
    return Mesh(points=points, cells=cells, point_data=point_data, cell_data=cell_data)


@pytest.mark.parametrize("mesh_tensor_mode", SHARD_MESH_TENSOR_MODES)
def test_shard_tensor_scatter_aggregate_matches_dense(
    mesh_tensor_mode: str, mesh_shard_device_mesh: DeviceMesh
) -> None:
    """Compare direct scatter aggregation for dense and ShardTensor inputs."""
    src_data = torch.tensor([[1.0, 2.0], [3.0, 5.0], [7.0, 11.0]])
    src_to_dst = torch.tensor([0, 0, 1], dtype=torch.int64)
    weights = torch.tensor([1.0, 3.0, 2.0])

    expected = scatter_aggregate(src_data, src_to_dst, n_dst=2, weights=weights)
    actual = scatter_aggregate(
        to_mode_tensor(
            src_data,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        ),
        to_mode_tensor(
            src_to_dst,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        ),
        n_dst=2,
        weights=to_mode_tensor(
            weights,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        ),
    )

    assert_shard_tensor(actual)
    assert_allclose(actual, expected)


@pytest.mark.parametrize("mesh_tensor_mode", SHARD_MESH_TENSOR_MODES)
def test_shard_tensor_geometry_conversion_and_transforms_match_dense(
    mesh_tensor_mode: str, mesh_shard_device_mesh: DeviceMesh
) -> None:
    """Exercise geometry, data conversion, and transforms with ShardTensor meshes."""
    dense_mesh = _surface_mesh()
    shard_mesh = mesh_to_mode(
        dense_mesh,
        mesh_tensor_mode=mesh_tensor_mode,
        mesh_shard_device_mesh=mesh_shard_device_mesh,
    )

    assert_shard_tensor(shard_mesh.points)
    assert_shard_tensor(shard_mesh.cells)
    assert_allclose(shard_mesh.cell_centroids, dense_mesh.cell_centroids)
    assert_allclose(shard_mesh.cell_areas, dense_mesh.cell_areas)
    assert_allclose(shard_mesh.point_normals, dense_mesh.point_normals)

    dense_point_data = dense_mesh.cell_data_to_point_data().point_data["temperature"]
    shard_point_data = shard_mesh.cell_data_to_point_data().point_data["temperature"]
    assert_shard_tensor(shard_point_data)
    assert_allclose(shard_point_data, dense_point_data)

    dense_transformed = dense_mesh.translate([1.0, -2.0, 0.5]).scale(2.0)
    shard_transformed = shard_mesh.translate([1.0, -2.0, 0.5]).scale(2.0)
    assert_shard_tensor(shard_transformed.points)
    assert_allclose(shard_transformed.points, dense_transformed.points)
    assert_allclose(shard_transformed.cell_areas, dense_transformed.cell_areas)


@pytest.mark.parametrize("mesh_tensor_mode", SHARD_MESH_TENSOR_MODES)
def test_shard_tensor_topology_and_boundary_queries_match_dense(
    mesh_tensor_mode: str, mesh_shard_device_mesh: DeviceMesh
) -> None:
    """Exercise representative neighbor and boundary queries with ShardTensor."""
    dense_mesh = _surface_mesh()
    shard_mesh = mesh_to_mode(
        dense_mesh,
        mesh_tensor_mode=mesh_tensor_mode,
        mesh_shard_device_mesh=mesh_shard_device_mesh,
    )

    assert_adjacency_equal(
        shard_mesh.get_point_to_points_adjacency(),
        dense_mesh.get_point_to_points_adjacency(),
    )
    assert_adjacency_equal(
        shard_mesh.get_cell_to_cells_adjacency(),
        dense_mesh.get_cell_to_cells_adjacency(),
    )
    assert_adjacency_equal(
        shard_mesh.get_cell_to_points_adjacency(),
        dense_mesh.get_cell_to_points_adjacency(),
    )

    shard_boundary = shard_mesh.get_boundary_mesh()
    dense_boundary = dense_mesh.get_boundary_mesh()
    assert_equal(shard_boundary.cells, dense_boundary.cells)
    assert_allclose(shard_boundary.points, dense_boundary.points)


@pytest.mark.parametrize("mesh_tensor_mode", SHARD_MESH_TENSOR_MODES)
def test_shard_tensor_calculus_and_subdivision_match_dense(
    mesh_tensor_mode: str, mesh_shard_device_mesh: DeviceMesh
) -> None:
    """Exercise derivative and subdivision paths with ShardTensor meshes."""
    dense_mesh = _surface_mesh()
    shard_mesh = mesh_to_mode(
        dense_mesh,
        mesh_tensor_mode=mesh_tensor_mode,
        mesh_shard_device_mesh=mesh_shard_device_mesh,
    )

    dense_grad = dense_mesh.clone().compute_point_derivatives(
        keys="phi", method="lsq", gradient_type="extrinsic"
    )
    shard_grad = shard_mesh.clone().compute_point_derivatives(
        keys="phi", method="lsq", gradient_type="extrinsic"
    )
    assert_shard_tensor(shard_grad.point_data["phi_gradient"])
    assert_allclose(
        shard_grad.point_data["phi_gradient"],
        dense_grad.point_data["phi_gradient"],
        atol=1e-5,
        rtol=1e-5,
    )

    dense_subdivided = dense_mesh.subdivide(levels=1, filter="linear")
    shard_subdivided = shard_mesh.subdivide(levels=1, filter="linear")
    assert_shard_tensor(shard_subdivided.points)
    assert_shard_tensor(shard_subdivided.cells)
    assert_allclose(shard_subdivided.points, dense_subdivided.points)
    assert_equal(shard_subdivided.cells, dense_subdivided.cells)
