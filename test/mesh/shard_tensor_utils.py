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

"""Shared helpers for Mesh tests that compare dense and ShardTensor behavior."""

from __future__ import annotations

import pytest
import torch
from tensordict import TensorDict
from torch.distributed.device_mesh import DeviceMesh
from torch.distributed.tensor.placement_types import Replicate, Shard

from physicsnemo.domain_parallel import ShardTensor
from physicsnemo.mesh.mesh import Mesh
from physicsnemo.mesh.neighbors import Adjacency

SHARD_MESH_TENSOR_MODES = ("shard_replicate", "shard_sharded")


def to_dense_tensor(tensor: torch.Tensor) -> torch.Tensor:
    """Materialize ShardTensor values for dense reference comparisons."""
    if ShardTensor is not None and isinstance(tensor, ShardTensor):
        return tensor.full_tensor()
    return tensor


def assert_allclose(a: torch.Tensor, b: torch.Tensor, **kwargs: object) -> None:
    """Assert approximate equality after materializing distributed tensors."""
    assert torch.allclose(to_dense_tensor(a), to_dense_tensor(b), **kwargs)


def assert_equal(a: torch.Tensor, b: torch.Tensor) -> None:
    """Assert exact equality after materializing distributed tensors."""
    assert torch.equal(to_dense_tensor(a), to_dense_tensor(b))


def assert_shard_tensor(tensor: torch.Tensor) -> None:
    """Assert that ``tensor`` is a ShardTensor."""
    assert ShardTensor is not None
    assert isinstance(tensor, ShardTensor)


def assert_adjacency_equal(actual: Adjacency, expected: Adjacency) -> None:
    """Assert two adjacency encodings are equal after materializing tensors."""
    assert_equal(actual.offsets, expected.offsets)
    assert_equal(actual.indices, expected.indices)


def placement_for_mode(mesh_tensor_mode: str) -> Replicate | Shard:
    """Return the point/cell placement used for a mesh tensor test mode."""
    if mesh_tensor_mode == "shard_sharded":
        return Shard(0)
    return Replicate()


def to_mode_tensor(
    tensor: torch.Tensor,
    *,
    mesh_tensor_mode: str,
    mesh_shard_device_mesh: DeviceMesh | None,
    placement: Replicate | Shard | None = None,
) -> torch.Tensor:
    """Convert a dense tensor to the representation for the active test mode."""
    if mesh_tensor_mode == "dense":
        return tensor
    if ShardTensor is None:
        pytest.skip("ShardTensor runtime is unavailable in this environment")
    if mesh_shard_device_mesh is None:
        raise ValueError("mesh_shard_device_mesh is required for ShardTensor tests")
    if tensor.device.type != "cpu":
        pytest.skip("ShardTensor mesh tests currently run on CPU test tensors")

    placement = placement or placement_for_mode(mesh_tensor_mode)
    sharding_shapes = (
        {0: [tuple(tensor.shape)]} if isinstance(placement, Shard) else "infer"
    )
    return ShardTensor.from_local(
        tensor,
        mesh_shard_device_mesh,
        [placement],
        sharding_shapes=sharding_shapes,
    )


def convert_leaf_for_mode(
    value: torch.Tensor,
    *,
    n_points: int,
    n_cells: int,
    point_placement: Replicate | Shard,
    cell_placement: Replicate | Shard,
    mesh_tensor_mode: str,
    mesh_shard_device_mesh: DeviceMesh | None,
) -> torch.Tensor:
    """Convert a TensorDict leaf according to whether it is point or cell data."""
    if value.ndim == 0:
        placement = Replicate()
    elif value.shape[0] == n_points:
        placement = point_placement
    elif value.shape[0] == n_cells:
        placement = cell_placement
    else:
        placement = Replicate()
    return to_mode_tensor(
        value,
        mesh_tensor_mode=mesh_tensor_mode,
        mesh_shard_device_mesh=mesh_shard_device_mesh,
        placement=placement,
    )


def convert_data_for_mode(
    data: TensorDict | dict[str, object] | None,
    *,
    n_points: int,
    n_cells: int,
    point_placement: Replicate | Shard,
    cell_placement: Replicate | Shard,
    mesh_tensor_mode: str,
    mesh_shard_device_mesh: DeviceMesh | None,
) -> TensorDict | dict[str, object] | None:
    """Convert a possibly nested data dictionary to the active tensor test mode."""
    if data is None or mesh_tensor_mode == "dense":
        return data

    def convert_value(value: object) -> object:
        """Convert tensor leaves while preserving non-tensor values."""
        if not isinstance(value, torch.Tensor):
            return value
        return convert_leaf_for_mode(
            value,
            n_points=n_points,
            n_cells=n_cells,
            point_placement=point_placement,
            cell_placement=cell_placement,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        )

    data_td = data if isinstance(data, TensorDict) else TensorDict(data, batch_size=[])
    return data_td.apply(convert_value)


def mesh_to_mode(
    mesh: Mesh,
    *,
    mesh_tensor_mode: str,
    mesh_shard_device_mesh: DeviceMesh | None,
) -> Mesh:
    """Convert a dense fixture mesh to the active tensor mode."""
    if mesh_tensor_mode == "dense":
        return mesh

    point_placement = placement_for_mode(mesh_tensor_mode)
    cell_placement = placement_for_mode(mesh_tensor_mode)

    return Mesh(
        points=to_mode_tensor(
            mesh.points,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
            placement=point_placement,
        ),
        cells=to_mode_tensor(
            mesh.cells,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
            placement=cell_placement,
        ),
        point_data=convert_data_for_mode(
            mesh.point_data,
            n_points=mesh.n_points,
            n_cells=mesh.n_cells,
            point_placement=point_placement,
            cell_placement=cell_placement,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        ),
        cell_data=convert_data_for_mode(
            mesh.cell_data,
            n_points=mesh.n_points,
            n_cells=mesh.n_cells,
            point_placement=point_placement,
            cell_placement=cell_placement,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        ),
        global_data=convert_data_for_mode(
            mesh.global_data,
            n_points=mesh.n_points,
            n_cells=mesh.n_cells,
            point_placement=Replicate(),
            cell_placement=Replicate(),
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        ),
    )
