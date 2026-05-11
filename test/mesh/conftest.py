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

"""Pytest configuration and shared fixtures for physicsnemo.mesh tests.

This module provides common test fixtures, utilities, and parametrization helpers
for exhaustive testing across spatial dimensions, manifold dimensions, and backends.

All functions and fixtures defined here are automatically available to all test files
without explicit imports.
"""

import os
import sys
import tempfile
from collections.abc import Generator
from dataclasses import dataclass

import pytest
import torch
import torch.distributed as dist
from tensordict import TensorDict
from torch.distributed.device_mesh import DeviceMesh, init_device_mesh
from torch.distributed.tensor.placement_types import Replicate, Shard

from physicsnemo.domain_parallel import ST_AVAILABLE, ShardTensor
from physicsnemo.mesh.mesh import Mesh
from test.mesh.tensor_mode_testing import set_active_mesh_tensor_mode_factory

### Mesh Tensor Modes ###

MESH_TENSOR_MODES = ("dense", "shard_replicate", "shard_sharded")
SHARD_MESH_TENSOR_MODES = ("shard_replicate", "shard_sharded")


@dataclass(frozen=True)
class MeshTensorModeFactory:
    """Create meshes and assertions for dense or ShardTensor mesh test modes."""

    mesh_tensor_mode: str
    mesh_shard_device_mesh: DeviceMesh | None

    def to_dense_tensor(self, tensor: torch.Tensor) -> torch.Tensor:
        """Materialize ShardTensor inputs for dense reference comparisons."""
        if ShardTensor is not None and isinstance(tensor, ShardTensor):
            return tensor.full_tensor()
        return tensor

    def assert_allclose(
        self, a: torch.Tensor, b: torch.Tensor, **kwargs: object
    ) -> None:
        """Assert approximate equality after materializing distributed tensors."""
        assert torch.allclose(
            self.to_dense_tensor(a), self.to_dense_tensor(b), **kwargs
        )

    def assert_equal(self, a: torch.Tensor, b: torch.Tensor) -> None:
        """Assert exact equality after materializing distributed tensors."""
        assert torch.equal(self.to_dense_tensor(a), self.to_dense_tensor(b))

    def assert_shard_tensor(self, tensor: torch.Tensor) -> None:
        """Assert that a tensor is backed by ShardTensor in sharded modes."""
        if self.mesh_tensor_mode == "dense":
            return
        assert ShardTensor is not None
        assert isinstance(tensor, ShardTensor)

    def placement_for_mode(self) -> Replicate | Shard:
        """Return the point/cell placement for the active mesh tensor mode."""
        if self.mesh_tensor_mode == "shard_sharded":
            return Shard(0)
        return Replicate()

    def to_mode_tensor(
        self,
        tensor: torch.Tensor,
        placement: Replicate | Shard | None = None,
    ) -> torch.Tensor:
        """Convert a dense tensor to the active mesh tensor mode."""
        if ShardTensor is not None and isinstance(tensor, ShardTensor):
            return tensor
        if self.mesh_tensor_mode == "dense":
            return tensor
        if ShardTensor is None:
            pytest.skip("ShardTensor runtime is unavailable in this environment")
        if self.mesh_shard_device_mesh is None:
            raise ValueError("mesh_shard_device_mesh is required for ShardTensor tests")
        if tensor.device.type != "cpu":
            pytest.skip("ShardTensor mesh test mode currently runs on CPU tensors")

        placement = placement or self.placement_for_mode()
        sharding_shapes = (
            {0: [tuple(tensor.shape)]} if isinstance(placement, Shard) else "infer"
        )
        return ShardTensor.from_local(
            tensor,
            self.mesh_shard_device_mesh,
            [placement],
            sharding_shapes=sharding_shapes,
        )

    def convert_leaf_for_mode(
        self,
        value: torch.Tensor,
        *,
        n_points: int,
        n_cells: int,
        point_placement: Replicate | Shard,
        cell_placement: Replicate | Shard,
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
        return self.to_mode_tensor(value, placement)

    def convert_data_for_mode(
        self,
        data: TensorDict | dict[str, object] | None,
        *,
        n_points: int,
        n_cells: int,
        point_placement: Replicate | Shard,
        cell_placement: Replicate | Shard,
    ) -> TensorDict | dict[str, object] | None:
        """Convert a possibly nested data dictionary to the active tensor mode."""
        if data is None or self.mesh_tensor_mode == "dense":
            return data

        def convert_value(value: object) -> object:
            """Convert tensor leaves while preserving non-tensor entries."""
            if not isinstance(value, torch.Tensor):
                return value
            return self.convert_leaf_for_mode(
                value,
                n_points=n_points,
                n_cells=n_cells,
                point_placement=point_placement,
                cell_placement=cell_placement,
            )

        data_td = (
            data if isinstance(data, TensorDict) else TensorDict(data, batch_size=[])
        )
        return data_td.apply(convert_value)

    def convert_mesh_inputs_for_mode(
        self,
        *,
        points: torch.Tensor,
        cells: torch.Tensor | None = None,
        point_data: TensorDict | dict[str, object] | None = None,
        cell_data: TensorDict | dict[str, object] | None = None,
        global_data: TensorDict | dict[str, object] | None = None,
    ) -> tuple[
        torch.Tensor,
        torch.Tensor | None,
        TensorDict | dict[str, object] | None,
        TensorDict | dict[str, object] | None,
        TensorDict | dict[str, object] | None,
    ]:
        """Convert Mesh constructor inputs to the active tensor mode."""
        if self.mesh_tensor_mode == "dense":
            return points, cells, point_data, cell_data, global_data

        if not isinstance(points, torch.Tensor):
            return points, cells, point_data, cell_data, global_data

        n_points = points.shape[0] if points.ndim > 0 else 0
        n_cells = (
            cells.shape[0] if isinstance(cells, torch.Tensor) and cells.ndim > 0 else 0
        )
        point_placement = self.placement_for_mode()
        cell_placement = self.placement_for_mode()
        mesh_points = self.to_mode_tensor(points, point_placement)
        mesh_cells = (
            None if cells is None else self.to_mode_tensor(cells, cell_placement)
        )

        return (
            mesh_points,
            mesh_cells,
            self.convert_data_for_mode(
                point_data,
                n_points=n_points,
                n_cells=n_cells,
                point_placement=point_placement,
                cell_placement=cell_placement,
            ),
            self.convert_data_for_mode(
                cell_data,
                n_points=n_points,
                n_cells=n_cells,
                point_placement=point_placement,
                cell_placement=cell_placement,
            ),
            self.convert_data_for_mode(
                global_data,
                n_points=n_points,
                n_cells=n_cells,
                point_placement=Replicate(),
                cell_placement=Replicate(),
            ),
        )

    def convert_mesh_data_value(self, value: torch.Tensor, mesh: Mesh) -> torch.Tensor:
        """Convert a tensor assigned to mesh data after Mesh construction."""
        placement = self.placement_for_mode()
        return self.convert_leaf_for_mode(
            value,
            n_points=mesh.n_points,
            n_cells=mesh.n_cells,
            point_placement=placement,
            cell_placement=placement,
        )

    def make_mesh(
        self,
        *,
        points: torch.Tensor,
        cells: torch.Tensor | None = None,
        point_data: TensorDict | dict[str, object] | None = None,
        cell_data: TensorDict | dict[str, object] | None = None,
        global_data: TensorDict | dict[str, object] | None = None,
    ) -> Mesh:
        """Create a mesh with tensors converted for the active test mode."""
        points, cells, point_data, cell_data, global_data = (
            self.convert_mesh_inputs_for_mode(
                points=points,
                cells=cells,
                point_data=point_data,
                cell_data=cell_data,
                global_data=global_data,
            )
        )

        return Mesh(
            points=points,
            cells=cells,
            point_data=point_data,
            cell_data=cell_data,
            global_data=global_data,
        )

    def mesh_to_mode(self, mesh: Mesh) -> Mesh:
        """Convert a dense fixture mesh to the active test mode."""
        if self.mesh_tensor_mode == "dense":
            return mesh
        return self.make_mesh(
            points=mesh.points,
            cells=mesh.cells,
            point_data=mesh.point_data,
            cell_data=mesh.cell_data,
            global_data=mesh.global_data,
        )


### Pytest Hooks ###


_MESH_INIT_ARGS = ("points", "cells", "point_data", "cell_data", "global_data")


def pytest_configure(config: pytest.Config) -> None:
    """Register mesh tensor-mode test markers."""
    config.addinivalue_line(
        "markers",
        "mesh_dense_only: run this mesh test only in the dense tensor mode",
    )


def _convert_mesh_init_call(
    factory: MeshTensorModeFactory,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> tuple[tuple[object, ...], dict[str, object]]:
    """Convert Mesh.__init__ args for the active tensor mode."""
    if not args and "points" not in kwargs:
        return args, kwargs

    args_list = list(args)
    values = {}
    for index, name in enumerate(_MESH_INIT_ARGS):
        if index < len(args_list):
            values[name] = args_list[index]
        else:
            values[name] = kwargs.get(name)

    converted = factory.convert_mesh_inputs_for_mode(
        points=values["points"],
        cells=values["cells"],
        point_data=values["point_data"],
        cell_data=values["cell_data"],
        global_data=values["global_data"],
    )

    new_kwargs = dict(kwargs)
    for index, (name, value) in enumerate(zip(_MESH_INIT_ARGS, converted)):
        if index < len(args_list):
            args_list[index] = value
        else:
            new_kwargs[name] = value

    return tuple(args_list), new_kwargs


def pytest_collection_modifyitems(config, items):
    """Skip tests marked with 'cuda' if CUDA is not available.

    This hook runs during test collection phase and adds skip markers to CUDA tests
    when CUDA is unavailable. This is the idiomatic pytest approach for conditional
    skipping based on markers.
    """
    if torch.cuda.is_available():
        return  # CUDA available, run all tests

    skip_cuda = pytest.mark.skip(reason="CUDA not available")
    for item in items:
        if "cuda" in item.keywords:
            item.add_marker(skip_cuda)


### Device Management ###


def get_available_devices() -> list[str]:
    """Get list of available compute devices for testing.

    Returns both 'cpu' and 'cuda' (if available). Tests marked with 'cuda'
    will be automatically skipped if CUDA is not available via pytest_collection_modifyitems.
    """
    devices = ["cpu"]
    if torch.cuda.is_available():
        devices.append("cuda")
    return devices


### Dimension Configurations ###


# Common dimensional configurations: (n_spatial_dims, n_manifold_dims)
DIMENSION_CONFIGS_2D = [
    (2, 0),  # Points in 2D
    (2, 1),  # Edges in 2D
    (2, 2),  # Triangles in 2D
]

DIMENSION_CONFIGS_3D = [
    (3, 0),  # Points in 3D
    (3, 1),  # Edges in 3D
    (3, 2),  # Triangles in 3D (surfaces)
    (3, 3),  # Tetrahedra in 3D (volumes)
]

DIMENSION_CONFIGS_ALL = DIMENSION_CONFIGS_2D + DIMENSION_CONFIGS_3D

DIMENSION_CONFIGS_CODIM1 = [
    (2, 1),  # Edges in 2D
    (3, 2),  # Surfaces in 3D
]


### Mesh Generators (Standalone Functions) ###


def create_simple_mesh(
    n_spatial_dims: int,
    n_manifold_dims: int,
    device: torch.device | str = "cpu",
):
    """Create a simple mesh for testing.

    Args:
        n_spatial_dims: Dimension of embedding space (2 or 3)
        n_manifold_dims: Dimension of manifold (0, 1, 2, or 3)
        device: Compute device ('cpu' or 'cuda')

    Returns:
        A simple Mesh instance appropriate for the given dimensions
    """
    from physicsnemo.mesh.mesh import Mesh

    if n_manifold_dims > n_spatial_dims:
        raise ValueError(
            f"Manifold dimension {n_manifold_dims} cannot exceed spatial dimension {n_spatial_dims}"
        )

    if n_manifold_dims == 0:
        # Point cloud
        if n_spatial_dims == 2:
            points = torch.tensor([[0.0, 0.0], [1.0, 0.0], [0.5, 1.0]], device=device)
        elif n_spatial_dims == 3:
            points = torch.tensor(
                [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.5, 1.0, 0.0]], device=device
            )
        else:
            raise ValueError(f"Unsupported {n_spatial_dims=}")
        cells = torch.arange(len(points), device=device, dtype=torch.int64).unsqueeze(1)

    elif n_manifold_dims == 1:
        # Polyline
        if n_spatial_dims == 2:
            points = torch.tensor(
                [[0.0, 0.0], [1.0, 0.0], [1.5, 1.0], [0.5, 1.5]], device=device
            )
        elif n_spatial_dims == 3:
            points = torch.tensor(
                [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [1.0, 1.0, 0.0], [0.0, 1.0, 1.0]],
                device=device,
            )
        else:
            raise ValueError(f"Unsupported {n_spatial_dims=}")
        cells = torch.tensor([[0, 1], [1, 2], [2, 3]], device=device, dtype=torch.int64)

    elif n_manifold_dims == 2:
        # Triangular mesh
        if n_spatial_dims == 2:
            points = torch.tensor(
                [[0.0, 0.0], [1.0, 0.0], [0.5, 1.0], [1.5, 0.5]], device=device
            )
        elif n_spatial_dims == 3:
            points = torch.tensor(
                [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.5, 1.0, 0.0], [1.5, 0.5, 0.5]],
                device=device,
            )
        else:
            raise ValueError(f"Unsupported {n_spatial_dims=}")
        cells = torch.tensor([[0, 1, 2], [1, 3, 2]], device=device, dtype=torch.int64)

    elif n_manifold_dims == 3:
        # Tetrahedral mesh
        if n_spatial_dims == 3:
            points = torch.tensor(
                [
                    [0.0, 0.0, 0.0],
                    [1.0, 0.0, 0.0],
                    [0.0, 1.0, 0.0],
                    [0.0, 0.0, 1.0],
                    [1.0, 1.0, 1.0],
                ],
                device=device,
            )
            cells = torch.tensor(
                [[0, 1, 2, 3], [1, 2, 3, 4]], device=device, dtype=torch.int64
            )
        else:
            raise ValueError("3-simplices require 3D embedding space")
    else:
        raise ValueError(f"Unsupported {n_manifold_dims=}")

    return Mesh(points=points, cells=cells)


def create_single_cell_mesh(
    n_spatial_dims: int,
    n_manifold_dims: int,
    device: torch.device | str = "cpu",
):
    """Create a mesh with a single cell."""
    from physicsnemo.mesh.mesh import Mesh

    if n_manifold_dims > n_spatial_dims:
        raise ValueError(
            f"Manifold dimension {n_manifold_dims} cannot exceed spatial dimension {n_spatial_dims}"
        )

    if n_manifold_dims == 0:
        if n_spatial_dims == 2:
            points = torch.tensor([[0.5, 0.5]], device=device)
        elif n_spatial_dims == 3:
            points = torch.tensor([[0.5, 0.5, 0.5]], device=device)
        else:
            raise ValueError(f"Unsupported {n_spatial_dims=}")
        cells = torch.tensor([[0]], device=device, dtype=torch.int64)

    elif n_manifold_dims == 1:
        if n_spatial_dims == 2:
            points = torch.tensor([[0.0, 0.0], [1.0, 0.0]], device=device)
        elif n_spatial_dims == 3:
            points = torch.tensor([[0.0, 0.0, 0.0], [1.0, 0.0, 0.0]], device=device)
        else:
            raise ValueError(f"Unsupported {n_spatial_dims=}")
        cells = torch.tensor([[0, 1]], device=device, dtype=torch.int64)

    elif n_manifold_dims == 2:
        if n_spatial_dims == 2:
            points = torch.tensor([[0.0, 0.0], [1.0, 0.0], [0.0, 1.0]], device=device)
        elif n_spatial_dims == 3:
            points = torch.tensor(
                [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]], device=device
            )
        else:
            raise ValueError(f"Unsupported {n_spatial_dims=}")
        cells = torch.tensor([[0, 1, 2]], device=device, dtype=torch.int64)

    elif n_manifold_dims == 3:
        if n_spatial_dims == 3:
            points = torch.tensor(
                [
                    [0.0, 0.0, 0.0],
                    [1.0, 0.0, 0.0],
                    [0.0, 1.0, 0.0],
                    [0.0, 0.0, 1.0],
                ],
                device=device,
            )
            cells = torch.tensor([[0, 1, 2, 3]], device=device, dtype=torch.int64)
        else:
            raise ValueError("3-simplices require 3D embedding space")
    else:
        raise ValueError(f"Unsupported {n_manifold_dims=}")

    return Mesh(points=points, cells=cells)


### Assertion Helpers ###


def assert_on_device(tensor: torch.Tensor, expected_device: str) -> None:
    """Assert tensor is on expected device."""
    actual_device = tensor.device.type
    assert actual_device == expected_device, (
        f"Device mismatch: tensor is on {actual_device!r}, expected {expected_device!r}"
    )


### Pytest Fixtures ###


@pytest.fixture(autouse=True)
def disable_tf32():
    """Disable TF32 for deterministic float32 precision across GPU architectures.

    TensorFloat-32 (TF32) is enabled by default on Ampere and newer GPUs (A100, etc.),
    which reduces float32 matrix multiplication precision from 23-bit to 10-bit mantissa.
    This can cause tests to pass on older GPUs but fail on newer ones due to ~1e-3 to 1e-4
    precision differences. Disabling TF32 ensures consistent behavior across all GPUs.
    """
    if not torch.cuda.is_available():
        yield
        return

    orig_matmul = torch.backends.cuda.matmul.allow_tf32
    orig_cudnn = torch.backends.cudnn.allow_tf32
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    yield
    torch.backends.cuda.matmul.allow_tf32 = orig_matmul
    torch.backends.cudnn.allow_tf32 = orig_cudnn


@pytest.fixture(
    params=[
        "cpu",
        pytest.param("cuda", marks=pytest.mark.cuda),
    ]
)
def device(request):
    """Parametrize tests over all available devices (CPU, CUDA).

    CUDA tests are automatically skipped if CUDA is not available via
    the pytest_collection_modifyitems hook.
    """
    return request.param


@pytest.fixture(scope="module")
def mesh_single_rank_dist_group() -> Generator[None, None, None]:
    """Initialize a single-rank process group for local ShardTensor mesh tests."""
    if dist.is_initialized():
        yield
        return

    os.environ.setdefault(
        "GLOO_SOCKET_IFNAME", "lo0" if sys.platform == "darwin" else "lo"
    )
    with tempfile.TemporaryDirectory(prefix="mesh_shard_pg_") as tmpdir:
        dist.init_process_group(
            backend="gloo",
            init_method=f"file://{tmpdir}/rendezvous",
            rank=0,
            world_size=1,
        )
        try:
            yield
        finally:
            if dist.is_initialized():
                dist.destroy_process_group()


@pytest.fixture(scope="module")
def mesh_shard_device_mesh(request: pytest.FixtureRequest) -> DeviceMesh:
    """Create a single-rank CPU device mesh for ShardTensor mesh unit tests."""
    if not ST_AVAILABLE or ShardTensor is None:
        pytest.skip("ShardTensor runtime is unavailable in this environment")

    request.getfixturevalue("mesh_single_rank_dist_group")
    return init_device_mesh("cpu", (1,))


@pytest.fixture(params=MESH_TENSOR_MODES)
def mesh_tensor_mode(request: pytest.FixtureRequest) -> str:
    """Parametrize eligible mesh tests over dense and ShardTensor modes."""
    return request.param


@pytest.fixture
def mesh_tensor_mode_factory(
    request: pytest.FixtureRequest, mesh_tensor_mode: str
) -> MeshTensorModeFactory:
    """Create a bound mesh tensor-mode factory for the active test parameter."""
    mesh_shard_device_mesh = (
        request.getfixturevalue("mesh_shard_device_mesh")
        if mesh_tensor_mode != "dense"
        else None
    )
    return MeshTensorModeFactory(
        mesh_tensor_mode=mesh_tensor_mode,
        mesh_shard_device_mesh=mesh_shard_device_mesh,
    )


@pytest.fixture(autouse=True)
def mesh_tensor_mode_context(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    mesh_tensor_mode_factory: MeshTensorModeFactory,
) -> Generator[None, None, None]:
    """Bind every mesh test to the active dense or ShardTensor mode."""
    if (
        request.node.get_closest_marker("mesh_dense_only")
        and mesh_tensor_mode_factory.mesh_tensor_mode != "dense"
    ):
        pytest.skip("Test is marked dense-only")

    previous_factory = set_active_mesh_tensor_mode_factory(mesh_tensor_mode_factory)
    original_init = Mesh.__init__

    if mesh_tensor_mode_factory.mesh_tensor_mode != "dense":

        def mesh_init_for_active_mode(self, *args, **kwargs):
            """Initialize Mesh after converting constructor tensors to test mode."""
            converted_args, converted_kwargs = _convert_mesh_init_call(
                mesh_tensor_mode_factory,
                args,
                kwargs,
            )
            original_init(self, *converted_args, **converted_kwargs)

        monkeypatch.setattr(Mesh, "__init__", mesh_init_for_active_mode)

    try:
        yield
    finally:
        set_active_mesh_tensor_mode_factory(previous_factory)


@pytest.fixture(params=DIMENSION_CONFIGS_2D)
def dims_2d(request):
    """Parametrize over 2D dimension configurations."""
    return request.param


@pytest.fixture(params=DIMENSION_CONFIGS_3D)
def dims_3d(request):
    """Parametrize over 3D dimension configurations."""
    return request.param


@pytest.fixture(params=DIMENSION_CONFIGS_ALL)
def dims_all(request):
    """Parametrize over all dimension combinations."""
    return request.param


@pytest.fixture(params=DIMENSION_CONFIGS_CODIM1)
def dims_codim1(request):
    """Parametrize over codimension-1 configurations."""
    return request.param
