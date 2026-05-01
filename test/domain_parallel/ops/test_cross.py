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

"""Tests for cross-product operations on ShardTensor."""

import math

import pytest
import torch
import torch.distributed as dist
from torch.distributed.tensor.placement_types import Shard

from physicsnemo.distributed import DistributedManager
from physicsnemo.domain_parallel import ShardTensor, scatter_tensor


@pytest.mark.multigpu_static
def test_torch_cross_default_dim_uses_global_shape(distributed_mesh):
    """``torch.cross(dim=None)`` should follow global, not local, shape."""
    dm = DistributedManager()
    local_leading_dim = 3
    full_shape = (dm.world_size * local_leading_dim, 3, 2)
    full_a = torch.arange(
        math.prod(full_shape),
        device=dm.device,
        dtype=torch.float32,
    ).reshape(full_shape)
    full_b = torch.flip(full_a, dims=(0,)) + 1

    placements = (Shard(0),)
    shard_a = scatter_tensor(
        full_a,
        global_src=0,
        mesh=distributed_mesh,
        placements=placements,
        global_shape=full_a.shape,
        dtype=full_a.dtype,
    )
    shard_b = scatter_tensor(
        full_b,
        global_src=0,
        mesh=distributed_mesh,
        placements=placements,
        global_shape=full_b.shape,
        dtype=full_b.dtype,
    )

    result = torch.cross(shard_a, shard_b).full_tensor()
    expected = torch.cross(full_a, full_b)

    assert torch.allclose(result, expected)


@pytest.mark.multigpu_static
def test_aten_cross_ops(distributed_mesh):
    """ATen cross ops should use ShardTensor handlers, not DTensor fallback."""
    dm = DistributedManager()
    local_leading_dim = 3
    full_shape = (dm.world_size * local_leading_dim, 3, 2)
    full_a = torch.arange(
        math.prod(full_shape),
        device=dm.device,
        dtype=torch.float32,
    ).reshape(full_shape)
    full_b = torch.flip(full_a, dims=(0,)) + 1

    placements = (Shard(0),)
    shard_a = scatter_tensor(
        full_a,
        global_src=0,
        mesh=distributed_mesh,
        placements=placements,
        global_shape=full_a.shape,
        dtype=full_a.dtype,
    )
    shard_b = scatter_tensor(
        full_b,
        global_src=0,
        mesh=distributed_mesh,
        placements=placements,
        global_shape=full_b.shape,
        dtype=full_b.dtype,
    )

    aten_cross = torch.ops.aten.cross.default(shard_a, shard_b).full_tensor()
    expected_cross = torch.ops.aten.cross.default(full_a, full_b)
    assert torch.allclose(aten_cross, expected_cross)

    aten_linalg_cross = torch.ops.aten.linalg_cross.default(
        shard_a, shard_b, dim=1
    ).full_tensor()
    expected_linalg_cross = torch.ops.aten.linalg_cross.default(full_a, full_b, dim=1)
    assert torch.allclose(aten_linalg_cross, expected_linalg_cross)


@pytest.mark.multigpu_static
def test_linalg_cross_rejects_sharded_cross_dim(distributed_mesh):
    """Cross products cannot be computed when the vector dimension is sharded."""
    dm = DistributedManager()
    if dm.world_size != 2:
        pytest.skip("This test expects a two-rank shard of a size-3 vector dimension.")

    mesh_rank = dist.get_group_rank(distributed_mesh.get_group(0), dm.rank)
    local_size = 2 if mesh_rank == 0 else 1
    local_a = torch.arange(
        2 * 2 * local_size,
        device=dm.device,
        dtype=torch.float32,
    ).reshape(2, 2, local_size)
    local_b = local_a + 1
    placements = (Shard(2),)

    shard_a = ShardTensor.from_local(
        local_a,
        device_mesh=distributed_mesh,
        placements=placements,
        sharding_shapes="infer",
    )
    shard_b = ShardTensor.from_local(
        local_b,
        device_mesh=distributed_mesh,
        placements=placements,
        sharding_shapes="infer",
    )

    with pytest.raises(RuntimeError, match="sharded dimension"):
        torch.linalg.cross(shard_a, shard_b, dim=2)


@pytest.mark.multigpu_static
def test_cross_rejects_out_of_range_dim(distributed_mesh):
    """Explicit cross-product dimensions should match PyTorch range checks."""
    dm = DistributedManager()
    full_shape = (3, dm.world_size * 2, 5)
    full_a = torch.arange(
        math.prod(full_shape),
        device=dm.device,
        dtype=torch.float32,
    ).reshape(full_shape)
    full_b = full_a + 1

    placements = (Shard(1),)
    shard_a = scatter_tensor(
        full_a,
        global_src=0,
        mesh=distributed_mesh,
        placements=placements,
        global_shape=full_a.shape,
        dtype=full_a.dtype,
    )
    shard_b = scatter_tensor(
        full_b,
        global_src=0,
        mesh=distributed_mesh,
        placements=placements,
        global_shape=full_b.shape,
        dtype=full_b.dtype,
    )

    for op in (torch.cross, torch.linalg.cross):
        with pytest.raises(IndexError, match="Dimension out of range"):
            op(shard_a, shard_b, dim=3)
        with pytest.raises(IndexError, match="Dimension out of range"):
            op(shard_a, shard_b, dim=-4)


@pytest.mark.multigpu_static
def test_linalg_cross_rejects_none_dim(distributed_mesh):
    """``torch.linalg.cross`` should reject ``dim=None`` like dense PyTorch."""
    dm = DistributedManager()
    full_shape = (3, dm.world_size * 2, 5)
    full_a = torch.arange(
        math.prod(full_shape),
        device=dm.device,
        dtype=torch.float32,
    ).reshape(full_shape)
    full_b = full_a + 1

    placements = (Shard(1),)
    shard_a = scatter_tensor(
        full_a,
        global_src=0,
        mesh=distributed_mesh,
        placements=placements,
        global_shape=full_a.shape,
        dtype=full_a.dtype,
    )
    shard_b = scatter_tensor(
        full_b,
        global_src=0,
        mesh=distributed_mesh,
        placements=placements,
        global_shape=full_b.shape,
        dtype=full_b.dtype,
    )

    with pytest.raises(TypeError, match="dim"):
        torch.linalg.cross(shard_a, shard_b, dim=None)
