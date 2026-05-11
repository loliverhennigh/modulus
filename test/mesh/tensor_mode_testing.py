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

"""Shared helpers for running mesh tests across dense and sharded tensor modes."""

from typing import Any

import torch

from physicsnemo.mesh.mesh import Mesh

_ACTIVE_MESH_TENSOR_MODE_FACTORY: Any | None = None


def set_active_mesh_tensor_mode_factory(factory: Any | None) -> Any | None:
    """Set the active mesh tensor-mode factory and return the previous value."""
    global _ACTIVE_MESH_TENSOR_MODE_FACTORY
    previous = _ACTIVE_MESH_TENSOR_MODE_FACTORY
    _ACTIVE_MESH_TENSOR_MODE_FACTORY = factory
    return previous


def get_active_mesh_tensor_mode_factory() -> Any | None:
    """Return the active mesh tensor-mode factory, if a test bound one."""
    return _ACTIVE_MESH_TENSOR_MODE_FACTORY


def to_dense_tensor_for_active_mode(tensor: torch.Tensor) -> torch.Tensor:
    """Materialize distributed tensor values for dense reference assertions."""
    factory = get_active_mesh_tensor_mode_factory()
    if factory is not None:
        return factory.to_dense_tensor(tensor)
    if hasattr(tensor, "full_tensor"):
        return tensor.full_tensor()
    return tensor


def assert_allclose_for_active_mode(
    a: torch.Tensor, b: torch.Tensor, **kwargs: object
) -> None:
    """Assert approximate equality after materializing distributed tensors."""
    assert torch.allclose(
        to_dense_tensor_for_active_mode(a),
        to_dense_tensor_for_active_mode(b),
        **kwargs,
    )


def assert_equal_for_active_mode(a: torch.Tensor, b: torch.Tensor) -> None:
    """Assert exact equality after materializing distributed tensors."""
    assert torch.equal(
        to_dense_tensor_for_active_mode(a),
        to_dense_tensor_for_active_mode(b),
    )


def mesh_to_active_mode(mesh: Mesh) -> Mesh:
    """Convert a dense mesh fixture to the active tensor mode."""
    factory = get_active_mesh_tensor_mode_factory()
    if factory is None:
        return mesh
    return factory.mesh_to_mode(mesh)


def make_mesh_for_active_mode(
    *,
    points: torch.Tensor,
    cells: torch.Tensor | None = None,
    point_data: dict[str, object] | None = None,
    cell_data: dict[str, object] | None = None,
    global_data: dict[str, object] | None = None,
) -> Mesh:
    """Create a mesh in the active tensor mode."""
    factory = get_active_mesh_tensor_mode_factory()
    if factory is None:
        return Mesh(
            points=points,
            cells=cells,
            point_data=point_data,
            cell_data=cell_data,
            global_data=global_data,
        )
    return factory.make_mesh(
        points=points,
        cells=cells,
        point_data=point_data,
        cell_data=cell_data,
        global_data=global_data,
    )


def convert_mesh_data_for_active_mode(value: torch.Tensor, mesh: Mesh) -> torch.Tensor:
    """Convert post-construction mesh data assignments to the active test mode."""
    factory = get_active_mesh_tensor_mode_factory()
    if factory is None:
        return value
    return factory.convert_mesh_data_value(value, mesh)
