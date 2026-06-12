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

import torch

from physicsnemo.core.function_spec import FunctionSpec
from physicsnemo.nn.functional.derivatives._mesh_lsq_operator_utils import (
    make_knn_csr_case,
)

from ._torch_impl import mesh_lsq_divergence_torch
from ._warp_impl import mesh_lsq_divergence_warp


class MeshLSQDivergence(FunctionSpec):
    r"""Compute divergence of an unstructured vector field by LSQ reconstruction.

    For each entity, this functional reconstructs the local Jacobian of a
    vector field from CSR neighborhoods using weighted least-squares, then
    returns its trace.

    Parameters
    ----------
    points : torch.Tensor
        Entity coordinates with shape ``(n_entities, dims)``.
    vector_field : torch.Tensor
        Vector values with shape ``(n_entities, dims)``.
    neighbor_offsets : torch.Tensor
        CSR offsets with shape ``(n_entities + 1,)``.
    neighbor_indices : torch.Tensor
        CSR flattened neighbor indices with shape ``(nnz,)``.
    weight_power : float, optional
        Inverse-distance exponent used for weighting.
    min_neighbors : int, optional
        Entities with fewer than this count get zero gradients.
    safe_epsilon : float | None, optional
        Positive floor applied to squared neighbor distances.
    implementation : {"warp", "torch"} or None
        Explicit backend selection. When ``None``, dispatch selects by rank.

    Returns
    -------
    torch.Tensor
        Divergence with shape ``(n_entities,)``.
    """

    _COMPARE_ATOL = 8e-3
    _COMPARE_RTOL = 8e-3

    @FunctionSpec.register(name="warp", required_imports=("warp>=0.6.0",), rank=0)
    def warp_forward(
        points: torch.Tensor,
        vector_field: torch.Tensor,
        neighbor_offsets: torch.Tensor,
        neighbor_indices: torch.Tensor,
        weight_power: float = 2.0,
        min_neighbors: int = 0,
        safe_epsilon: float | None = None,
    ) -> torch.Tensor:
        """Dispatch LSQ mesh divergence to the Warp backend."""
        return mesh_lsq_divergence_warp(
            points=points,
            vector_field=vector_field,
            neighbor_offsets=neighbor_offsets,
            neighbor_indices=neighbor_indices,
            weight_power=weight_power,
            min_neighbors=min_neighbors,
            safe_epsilon=safe_epsilon,
        )

    @FunctionSpec.register(name="torch", rank=1, baseline=True)
    def torch_forward(
        points: torch.Tensor,
        vector_field: torch.Tensor,
        neighbor_offsets: torch.Tensor,
        neighbor_indices: torch.Tensor,
        weight_power: float = 2.0,
        min_neighbors: int = 0,
        safe_epsilon: float | None = None,
    ) -> torch.Tensor:
        """Dispatch LSQ mesh divergence to eager PyTorch."""
        return mesh_lsq_divergence_torch(
            points=points,
            vector_field=vector_field,
            neighbor_offsets=neighbor_offsets,
            neighbor_indices=neighbor_indices,
            weight_power=weight_power,
            min_neighbors=min_neighbors,
            safe_epsilon=safe_epsilon,
        )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        """Yield representative forward benchmark and parity input cases."""
        for label, n_entities, n_dims, k_neighbors in (
            ("small-2d-n512-k12", 512, 2, 12),
            ("medium-3d-n1024-k16", 1024, 3, 16),
        ):
            points, offsets, indices = make_knn_csr_case(
                device=device,
                n_entities=n_entities,
                n_dims=n_dims,
                k_neighbors=k_neighbors,
                seed=6100 + n_entities + n_dims,
            )
            vector_field = points.square()
            yield label, (points, vector_field, offsets, indices), {}

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        """Yield representative backward benchmark and parity input cases."""
        for label, args, kwargs in cls.make_inputs_forward(device=device):
            points, vector_field, offsets, indices = args
            yield (
                f"{label}-backward",
                (
                    points.detach().clone().requires_grad_(True),
                    vector_field.detach().clone().requires_grad_(True),
                    offsets,
                    indices,
                ),
                kwargs,
            )

    @classmethod
    def compare_forward(cls, output: torch.Tensor, reference: torch.Tensor) -> None:
        """Compare forward outputs across implementations."""
        torch.testing.assert_close(
            output,
            reference,
            atol=cls._COMPARE_ATOL,
            rtol=cls._COMPARE_RTOL,
        )

    @classmethod
    def compare_backward(cls, output: torch.Tensor, reference: torch.Tensor) -> None:
        """Compare backward gradients across implementations."""
        torch.testing.assert_close(
            output,
            reference,
            atol=cls._COMPARE_ATOL,
            rtol=cls._COMPARE_RTOL,
        )


mesh_lsq_divergence = MeshLSQDivergence.make_function("mesh_lsq_divergence")


__all__ = ["MeshLSQDivergence", "mesh_lsq_divergence"]
