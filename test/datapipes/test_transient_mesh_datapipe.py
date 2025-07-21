# SPDX-FileCopyrightText: Copyright (c) 2023 - 2024 NVIDIA CORPORATION & AFFILIATES.
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

import random
from pathlib import Path

import pytest
from pytest_utils import import_or_fail


@import_or_fail(["vtk"])
@pytest.mark.parametrize("device", ["cuda", "cpu"])
@pytest.mark.parametrize("file_format", ["vtp", "vtu"])
def test_transient_mesh_datapipe(device, file_format, tmp_path, pytestconfig):
    """Smoke-tests the TransientMeshDatapipe with a synthetic VTP time-sequence."""

    import vtk

    from physicsnemo.datapipes.cae import TransientMeshDatapipe

    def _write_random_mesh(num_points: int, num_triangles: int, out_file: Path):
        """Create a random VTP or VTU mesh depending on file extension."""
        # Create random points
        points = vtk.vtkPoints()
        for _ in range(num_points):
            x, y, z = (
                random.uniform(-10, 10),
                random.uniform(-10, 10),
                random.uniform(-10, 10),
            )
            points.InsertNextPoint(x, y, z)

        # Create triangles
        triangles = vtk.vtkCellArray()
        for _ in range(num_triangles):
            p1, p2, p3 = (
                random.randint(0, num_points - 1),
                random.randint(0, num_points - 1),
                random.randint(0, num_points - 1),
            )
            triangle = vtk.vtkTriangle()
            triangle.GetPointIds().SetId(0, p1)
            triangle.GetPointIds().SetId(1, p2)
            triangle.GetPointIds().SetId(2, p3)
            triangles.InsertNextCell(triangle)

        # Attribute array
        scalars = vtk.vtkDoubleArray()
        scalars.SetName("RandomFeatures")
        for _ in range(num_points):
            scalars.InsertNextValue(random.uniform(0, 1))

        if out_file.suffix == ".vtp":
            poly = vtk.vtkPolyData()
            poly.SetPoints(points)
            poly.SetPolys(triangles)
            poly.GetPointData().SetScalars(scalars)
            writer = vtk.vtkXMLPolyDataWriter()
            writer.SetFileName(str(out_file))
            writer.SetInputData(poly)
            writer.Write()
        else:
            grid = vtk.vtkUnstructuredGrid()
            grid.SetPoints(points)
            grid.SetCells(vtk.VTK_TRIANGLE, triangles)
            grid.GetPointData().SetScalars(scalars)
            writer = vtk.vtkXMLUnstructuredGridWriter()
            writer.SetFileName(str(out_file))
            writer.SetInputData(grid)
            writer.Write()

    # ------------------------------------------------------------------
    # Build temporary dataset: 1 simulation directory, 3 timesteps.
    # ------------------------------------------------------------------
    root_dir = tmp_path / "dataset"
    sim_dir = root_dir / "simulation_000"
    sim_dir.mkdir(parents=True)

    for step in range(3):  # Need >= sequence_length (2) + 1
        file_path = sim_dir / f"mesh_{step:04d}.{file_format}"
        _write_random_mesh(num_points=10, num_triangles=20, out_file=file_path)

    # ------------------------------------------------------------------
    # Instantiate the datapipe and validate basic behaviour.
    # ------------------------------------------------------------------
    sequence_length = 2
    dp = TransientMeshDatapipe(
        data_dir=root_dir,
        variables=["RandomFeatures"],
        num_variables=1,
        file_format=file_format,
        sequence_length=sequence_length,
        batch_size=1,
        shuffle=False,
        num_workers=1,
        device=device,
    )

    # There are 3 files → (3 - 2 + 1) = 2 sequences.
    assert len(dp) == 2

    for batch in dp:
        sample = batch[0]
        vertices = sample["vertices"]
        x = sample["x"]
        edges = sample["edges"]

        # Expected shapes: (B=1, S, V, ...)
        assert vertices.shape[:2] == (1, sequence_length)
        assert vertices.shape[-1] == 3
        assert x.shape[:3] == (1, sequence_length, 10)
        assert x.shape[-1] == 1
        # Edges tensor last dim must be 2
        assert edges.shape[-1] == 2

        # Only iterate first batch for speed.
        break
