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

import os
from typing import Any

import numpy as np
import torch
import vtk

Tensor = torch.Tensor


def parse_vtk_polydata(polydata, variables):
    # Fetch vertices
    points = polydata.GetPoints()
    if points is None:
        raise ValueError("Failed to get points from the polydata.")
    vertices = torch.tensor(
        np.array([points.GetPoint(i) for i in range(points.GetNumberOfPoints())]),
        dtype=torch.float32,
    )

    # Fetch node attributes  # TODO modularize
    attributes = []
    point_data = polydata.GetPointData()
    if point_data is None:
        raise ValueError("Failed to get point data from the unstructured grid.")
    for array_name in variables:
        try:
            array = point_data.GetArray(array_name)
        except ValueError:
            raise ValueError(
                f"Failed to get array {array_name} from the unstructured grid."
            )
        array_data = np.zeros(
            (points.GetNumberOfPoints(), array.GetNumberOfComponents())
        )
        for j in range(points.GetNumberOfPoints()):
            array.GetTuple(j, array_data[j])
        attributes.append(torch.tensor(array_data, dtype=torch.float32))
    attributes = torch.cat(attributes, dim=-1)
    # TODO torch.cat is usually very inefficient when the number of items is large.
    # If possible, the resulting tensor should be pre-allocated and filled in during the loop.

    # Fetch edges
    polys = polydata.GetPolys()
    if polys is None:
        raise ValueError("Failed to get polygons from the polydata.")
    polys.InitTraversal()
    edges = []
    id_list = vtk.vtkIdList()
    for _ in range(polys.GetNumberOfCells()):
        polys.GetNextCell(id_list)
        num_ids = id_list.GetNumberOfIds()
        edges = [
            (id_list.GetId(j), id_list.GetId((j + 1) % num_ids)) for j in range(num_ids)
        ]
    edges = torch.tensor(edges, dtype=torch.long)

    return vertices, attributes, edges


def parse_vtk_unstructuredgrid(grid, variables):
    # Fetch vertices
    points = grid.GetPoints()
    if points is None:
        raise ValueError("Failed to get points from the unstructured grid.")
    vertices = torch.tensor(
        np.array([points.GetPoint(i) for i in range(points.GetNumberOfPoints())]),
        dtype=torch.float32,
    )

    # Fetch node attributes  # TODO modularize
    attributes = []
    point_data = grid.GetPointData()
    if point_data is None:
        raise ValueError("Failed to get point data from the unstructured grid.")
    for array_name in variables:
        try:
            array = point_data.GetArray(array_name)
        except ValueError:
            raise ValueError(
                f"Failed to get array {array_name} from the unstructured grid."
            )
        array_data = np.zeros(
            (points.GetNumberOfPoints(), array.GetNumberOfComponents())
        )
        for j in range(points.GetNumberOfPoints()):
            array.GetTuple(j, array_data[j])
        attributes.append(torch.tensor(array_data, dtype=torch.float32))
    if variables:
        attributes = torch.cat(attributes, dim=-1)
    else:
        attributes = torch.zeros((1,), dtype=torch.float32)

    # Return a dummy tensor of zeros for edges since they are not directly computable
    return (
        vertices,
        attributes,
        torch.zeros((0, 2), dtype=torch.long),
    )  # Dummy tensor for edges


def read_vtp(file_path: str) -> Any:  # TODO add support for older format (VTK)
    """
    Read a VTP file and return the polydata.

    Parameters
    ----------
    file_path : str
        Path to the VTP file.

    Returns
    -------
    vtkPolyData
        The polydata read from the VTP file.
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist.")

    # Check if file has .vtp extension
    if not file_path.endswith(".vtp"):
        raise ValueError(f"Expected a .vtp file, got {file_path}")

    reader = vtk.vtkXMLPolyDataReader()
    reader.SetFileName(file_path)
    reader.Update()

    # Get the polydata
    polydata = reader.GetOutput()

    # Check if polydata is valid
    if polydata is None:
        raise ValueError(f"Failed to read polydata from {file_path}")

    return polydata


def read_vtu(file_path: str) -> Any:
    """
    Read a VTU file and return the unstructured grid data.

    Parameters
    ----------
    file_path : str
        Path to the VTU file.

    Returns
    -------
    vtkUnstructuredGrid
        The unstructured grid data read from the VTU file.
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist.")

    # Check if file has .vtu extension
    if not file_path.endswith(".vtu"):
        raise ValueError(f"Expected a .vtu file, got {file_path}")

    reader = vtk.vtkXMLUnstructuredGridReader()
    reader.SetFileName(file_path)
    reader.Update()

    # Get the unstructured grid data
    grid = reader.GetOutput()

    # Check if grid is valid
    if grid is None:
        raise ValueError(f"Failed to read unstructured grid data from {file_path}")

    return grid


def read_vtm(file_path: str) -> Any:
    """
    Read a VTM (VTK MultiBlock) file and return the unstructured grid data.

    Parameters
    ----------
    file_path : str
        Path to the VTM file.

    Returns
    -------
    vtkUnstructuredGrid
        The unstructured grid data extracted from the multi-block dataset.
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist.")

    # Check if file has .vtm extension
    if not file_path.endswith(".vtm"):
        raise ValueError(f"Expected a .vtm file, got {file_path}")

    # Create a VTM reader
    reader = vtk.vtkXMLMultiBlockDataReader()
    reader.SetFileName(file_path)
    reader.Update()

    # Get the multi-block dataset
    multi_block = reader.GetOutput()

    # Check if the multi-block dataset is valid
    if multi_block is None:
        raise ValueError(f"Failed to read multi-block data from {file_path}")

    # Extract and return the vtkUnstructuredGrid from the multi-block dataset
    return _extract_unstructured_grid(multi_block)


def read_cgns(file_path: str) -> Any:
    """
    Read a CGNS file and return the unstructured grid data.

    Parameters
    ----------
    file_path : str
        Path to the CGNS file.

    Returns
    -------
    vtkUnstructuredGrid
        The unstructured grid data read from the CGNS file.
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist.")

    # Check if file has .cgns extension
    if not file_path.endswith(".cgns"):
        raise ValueError(f"Expected a .cgns file, got {file_path}")

    reader = vtk.vtkCGNSReader()
    reader.SetFileName(file_path)
    reader.Update()

    # Get the multi-block dataset
    multi_block = reader.GetOutput()

    # Check if the multi-block dataset is valid
    if multi_block is None:
        raise ValueError(f"Failed to read multi-block data from {file_path}")

    # Extract and return the vtkUnstructuredGrid from the multi-block dataset
    return _extract_unstructured_grid(multi_block)


def read_stl(file_path: str) -> vtk.vtkPolyData:
    """
    Read an STL file and return the polydata.

    Parameters
    ----------
    file_path : str
        Path to the STL file.

    Returns
    -------
    vtkPolyData
        The polydata read from the STL file.
    """
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist.")

    # Check if file has .stl extension
    if not file_path.endswith(".stl"):
        raise ValueError(f"Expected a .stl file, got {file_path}")

    # Create an STL reader
    reader = vtk.vtkSTLReader()
    reader.SetFileName(file_path)
    reader.Update()

    # Get the polydata
    polydata = reader.GetOutput()

    # Check if polydata is valid
    if polydata is None:
        raise ValueError(f"Failed to read polydata from {file_path}")

    return polydata


def _extract_unstructured_grid(
    multi_block: vtk.vtkMultiBlockDataSet,
) -> vtk.vtkUnstructuredGrid:
    """
    Extracts a vtkUnstructuredGrid from a vtkMultiBlockDataSet.

    Parameters
    ----------
    multi_block : vtk.vtkMultiBlockDataSet
        The multi-block dataset containing various data blocks.

    Returns
    -------
    vtk.vtkUnstructuredGrid
        The unstructured grid extracted from the multi-block dataset.
    """
    block = multi_block.GetBlock(0).GetBlock(0)
    if isinstance(block, vtk.vtkUnstructuredGrid):
        return block
    raise ValueError("No vtkUnstructuredGrid found in the vtkMultiBlockDataSet.")
