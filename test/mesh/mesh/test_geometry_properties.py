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

"""Tests for Mesh geometry properties against PyVista reference implementations.

Validates cell_centroids, cell_areas, cell_normals, and point_normals by comparing
against PyVista's compute_cell_sizes and compute_normals methods.
"""

import os
import sys
import tempfile

import pytest

pytest.importorskip("pyvista")

import numpy as np
import torch
import torch.distributed as dist
from torch.distributed.device_mesh import init_device_mesh
from torch.distributed.tensor.placement_types import Replicate, Shard

from physicsnemo.domain_parallel import ST_AVAILABLE, ShardTensor
from physicsnemo.mesh.io.io_pyvista import to_pyvista
from physicsnemo.mesh.mesh import Mesh
from physicsnemo.mesh.primitives.pyvista_datasets import bunny
from physicsnemo.mesh.primitives.volumes import sphere_volume

### Constants ###

ATOL = 1e-4
RTOL = 1e-4
# Opt-in shard matrix (keeps default CI path unchanged):
# PHYSICSNEMO_MESH_SHARD_TESTS=1 pytest ...
_ENABLE_SHARD_MESH_TEST_MODES = os.getenv(
    "PHYSICSNEMO_MESH_SHARD_TESTS", "0"
).strip().lower() in ("1", "true", "yes", "on")
_MESH_TENSOR_MODES = (
    ["dense", "shard_replicate", "shard_sharded"]
    if _ENABLE_SHARD_MESH_TEST_MODES
    else ["dense"]
)


@pytest.fixture(params=_MESH_TENSOR_MODES)
def mesh_tensor_mode(request) -> str:
    """Parametrize dense and opt-in ShardTensor geometry modes."""
    return request.param


@pytest.fixture(scope="module")
def _single_rank_dist_group():
    """Initialize a single-rank process group for local ShardTensor tests."""
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


@pytest.fixture
def mesh_shard_device_mesh(request, mesh_tensor_mode: str):
    """Create a single-rank CPU mesh only for ShardTensor test modes."""
    if mesh_tensor_mode == "dense":
        return None

    if not ST_AVAILABLE or ShardTensor is None:
        pytest.skip("ShardTensor runtime is unavailable in this environment")

    request.getfixturevalue("_single_rank_dist_group")
    return init_device_mesh("cpu", (1,))


### Helper Functions ###


def _to_dense_tensor(tensor: torch.Tensor) -> torch.Tensor:
    """Materialize ShardTensor values for robust assertions."""
    if ShardTensor is not None and isinstance(tensor, ShardTensor):
        return tensor.full_tensor()
    return tensor


def _assert_allclose(a: torch.Tensor, b: torch.Tensor, **kwargs) -> None:
    """Assert equality after materializing any distributed tensor inputs."""
    assert torch.allclose(_to_dense_tensor(a), _to_dense_tensor(b), **kwargs)


def _to_mode_tensor(
    tensor: torch.Tensor,
    *,
    mesh_tensor_mode: str,
    mesh_shard_device_mesh,
    placement: Replicate | Shard,
) -> torch.Tensor:
    """Wrap a tensor as a ShardTensor when the active mode requires it."""
    if mesh_tensor_mode == "dense":
        return tensor
    if tensor.device.type != "cpu":
        pytest.skip("ShardTensor mesh geometry tests currently run on CPU tensors")
    sharding_shapes = (
        {0: [tuple(tensor.shape)]} if isinstance(placement, Shard) else "infer"
    )
    return ShardTensor.from_local(
        tensor,
        mesh_shard_device_mesh,
        [placement],
        sharding_shapes=sharding_shapes,
    )


def _mesh_to_mode(mesh: Mesh, *, mesh_tensor_mode: str, mesh_shard_device_mesh) -> Mesh:
    """Convert a dense fixture mesh to the active tensor mode."""
    if mesh_tensor_mode == "dense":
        return mesh
    placement = Shard(0) if mesh_tensor_mode == "shard_sharded" else Replicate()
    return Mesh(
        points=_to_mode_tensor(
            mesh.points,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
            placement=placement,
        ),
        cells=_to_mode_tensor(
            mesh.cells,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
            placement=placement,
        ),
        point_data=mesh.point_data,
        cell_data=mesh.cell_data,
        global_data=mesh.global_data,
    )


def assert_normals_equal(
    mesh_normals: torch.Tensor,
    pv_normals: np.ndarray,
    atol: float = ATOL,
    rtol: float = RTOL,
) -> None:
    """Assert normals match, allowing for sign flips.

    Performs two checks:
    1. Component-wise equality allowing sign flip (abs values match)
    2. Alignment via dot product (should be ±1 for unit vectors)

    Isolated vertices (with zero-length normals) are handled separately - both
    implementations should return zero vectors for these.

    Parameters
    ----------
    mesh_normals : torch.Tensor
        Normals from Mesh, shape (n, 3).
    pv_normals : np.ndarray
        Normals from PyVista, shape (n, 3).
    atol : float
        Absolute tolerance for comparison.
    rtol : float
        Relative tolerance for comparison.
    """
    mesh_normals = _to_dense_tensor(mesh_normals)
    pv_tensor = torch.from_numpy(pv_normals).float()

    ### Identify isolated vertices (zero-length normals in both)
    mesh_norms = mesh_normals.norm(dim=-1)
    pv_norms = pv_tensor.norm(dim=-1)
    isolated_mask = (mesh_norms < atol) & (pv_norms < atol)
    connected_mask = ~isolated_mask

    ### Check that isolated vertices have matching zero normals
    if isolated_mask.any():
        assert torch.allclose(
            mesh_normals[isolated_mask], pv_tensor[isolated_mask], atol=atol, rtol=rtol
        ), "Isolated vertex normals should both be zero vectors"

    ### For connected vertices, check alignment
    if connected_mask.any():
        mesh_connected = mesh_normals[connected_mask]
        pv_connected = pv_tensor[connected_mask]

        ### Check 1: Component-wise equality allowing sign flip
        assert torch.allclose(
            mesh_connected.abs(), pv_connected.abs(), atol=atol, rtol=rtol
        ), (
            f"Normal magnitudes differ.\n"
            f"Max abs diff: {(mesh_connected.abs() - pv_connected.abs()).abs().max()}"
        )

        ### Check 2: Alignment via dot product (should be ±1 for unit vectors)
        dot_products = (mesh_connected * pv_connected).sum(dim=-1)
        assert torch.allclose(
            dot_products.abs(), torch.ones_like(dot_products), atol=atol, rtol=rtol
        ), (
            f"Normals not aligned.\n"
            f"Min |dot|: {dot_products.abs().min()}, Max |dot|: {dot_products.abs().max()}"
        )


### Test Classes ###


class TestCellCentroids:
    """Tests for Mesh.cell_centroids property."""

    def test_2d_manifold_bunny(self, mesh_tensor_mode, mesh_shard_device_mesh):
        """Test cell centroids on 2D manifold (triangular surface mesh)."""
        ### Load bunny mesh and convert to PyVista
        dense_mesh = bunny.load()
        mesh = _mesh_to_mode(
            dense_mesh,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        )
        pv_mesh = to_pyvista(dense_mesh)

        ### Compute centroids with both implementations
        mesh_centroids = mesh.cell_centroids  # shape: (n_cells, 3)
        pv_centroids = pv_mesh.cell_centers().points  # shape: (n_cells, 3)

        ### Compare results
        pv_tensor = torch.from_numpy(pv_centroids).float()
        _assert_allclose(mesh_centroids, pv_tensor, atol=ATOL, rtol=RTOL)

    def test_3d_manifold_sphere_volume(self, mesh_tensor_mode, mesh_shard_device_mesh):
        """Test cell centroids on 3D manifold (tetrahedral volume mesh)."""
        ### Load sphere volume mesh and convert to PyVista
        dense_mesh = sphere_volume.load()
        mesh = _mesh_to_mode(
            dense_mesh,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        )
        pv_mesh = to_pyvista(dense_mesh)

        ### Compute centroids with both implementations
        mesh_centroids = mesh.cell_centroids  # shape: (n_cells, 3)
        pv_centroids = pv_mesh.cell_centers().points  # shape: (n_cells, 3)

        ### Compare results
        pv_tensor = torch.from_numpy(pv_centroids).float()
        _assert_allclose(mesh_centroids, pv_tensor, atol=ATOL, rtol=RTOL)


class TestCellAreas:
    """Tests for Mesh.cell_areas property."""

    def test_2d_manifold_bunny(self, mesh_tensor_mode, mesh_shard_device_mesh):
        """Test cell areas on 2D manifold (triangular surface mesh)."""
        ### Load bunny mesh and convert to PyVista
        dense_mesh = bunny.load()
        mesh = _mesh_to_mode(
            dense_mesh,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        )
        pv_mesh = to_pyvista(dense_mesh)

        ### Compute areas with both implementations
        mesh_areas = mesh.cell_areas  # shape: (n_cells,)
        pv_sized = pv_mesh.compute_cell_sizes(area=True, volume=False)
        pv_areas = pv_sized.cell_data["Area"]  # shape: (n_cells,)

        ### Compare results
        pv_tensor = torch.from_numpy(pv_areas).float()
        _assert_allclose(mesh_areas, pv_tensor, atol=ATOL, rtol=RTOL)

    def test_3d_manifold_sphere_volume(self, mesh_tensor_mode, mesh_shard_device_mesh):
        """Test cell volumes on 3D manifold (tetrahedral volume mesh).

        Note: For 3D manifolds, cell_areas returns the volume of each tetrahedron.
        """
        ### Load sphere volume mesh and convert to PyVista
        dense_mesh = sphere_volume.load()
        mesh = _mesh_to_mode(
            dense_mesh,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        )
        pv_mesh = to_pyvista(dense_mesh)

        ### Compute volumes with both implementations
        mesh_volumes = mesh.cell_areas  # shape: (n_cells,)
        pv_sized = pv_mesh.compute_cell_sizes(area=False, volume=True)
        pv_volumes = pv_sized.cell_data["Volume"]  # shape: (n_cells,)

        ### Compare results
        pv_tensor = torch.from_numpy(pv_volumes).float()
        _assert_allclose(mesh_volumes, pv_tensor, atol=ATOL, rtol=RTOL)


class TestCellNormals:
    """Tests for Mesh.cell_normals property."""

    def test_2d_manifold_bunny(self, mesh_tensor_mode, mesh_shard_device_mesh):
        """Test cell normals on 2D manifold (triangular surface mesh).

        Cell normals are only defined for codimension-1 manifolds (e.g., triangles in 3D).
        """
        ### Load bunny mesh and convert to PyVista
        dense_mesh = bunny.load()
        mesh = _mesh_to_mode(
            dense_mesh,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        )
        pv_mesh = to_pyvista(dense_mesh)

        ### Compute normals with both implementations
        mesh_normals = mesh.cell_normals  # shape: (n_cells, 3)
        pv_normed = pv_mesh.compute_normals(cell_normals=True, point_normals=False)
        pv_normals = pv_normed.cell_data["Normals"]  # shape: (n_cells, 3)

        ### Compare results (allowing for sign flips)
        assert_normals_equal(mesh_normals, pv_normals)


class TestPointNormals:
    """Tests for Mesh.point_normals property and compute_point_normals method.

    Mesh supports four weighting schemes for point normals:
    - "area": Area-weighted averaging (larger faces have more influence) - default
    - "unweighted": Simple averaging (equal weight per face, matches PyVista/VTK)
    - "angle": Angle-weighted averaging (weight by interior angle at vertex)
    - "angle_area": Combined angle and area weighting (Maya default)

    The point_normals property returns area-weighted normals (canonical default).
    The compute_point_normals() method allows explicit weighting selection.

    Tests use weighting="unweighted" to match PyVista/VTK's compute_normals behavior.
    """

    def test_2d_manifold_bunny(self, mesh_tensor_mode, mesh_shard_device_mesh):
        """Test point normals on 2D manifold (triangular surface mesh).

        Uses unweighted averaging to match PyVista/VTK behavior.
        """
        ### Load bunny mesh and convert to PyVista
        dense_mesh = bunny.load()
        mesh = _mesh_to_mode(
            dense_mesh,
            mesh_tensor_mode=mesh_tensor_mode,
            mesh_shard_device_mesh=mesh_shard_device_mesh,
        )
        pv_mesh = to_pyvista(dense_mesh)

        ### Compute normals with both implementations
        # Use compute_point_normals with weighting="unweighted" to match PyVista/VTK
        mesh_normals = mesh.compute_point_normals(
            weighting="unweighted"
        )  # (n_points, 3)
        pv_normed = pv_mesh.compute_normals(cell_normals=False, point_normals=True)
        pv_normals = pv_normed.point_data["Normals"]  # shape: (n_points, 3)

        ### Compare results (allowing for sign flips)
        assert_normals_equal(mesh_normals, pv_normals)
