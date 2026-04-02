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

from collections.abc import Sequence

import torch
from jaxtyping import Float
from torch import Tensor

from physicsnemo.core.function_spec import FunctionSpec

from ._torch_impl import (
    meshless_fd_derivatives_torch,
    meshless_fd_stencil_points_torch,
)


class MeshlessFDDerivatives(FunctionSpec):
    """Compute meshless finite-difference derivatives from stencil values.

    Parameters
    ----------
    stencil_values : torch.Tensor
        Values sampled on a canonical ``{-1,0,1}`` stencil with shape
        ``(num_points, stencil_size)`` or ``(num_points, stencil_size, channels)``.
        Stencil sizes must be ``3``, ``9``, or ``27``.
    spacing : float | Sequence[float], optional
        Stencil spacing per axis.
    order : int, optional
        Derivative order (``1`` or ``2``).
    return_mixed_derivs : bool, optional
        Include mixed second derivatives when ``order=2``.
    implementation : {"torch"} or None
        Implementation to use. When ``None``, dispatch selects the available
        implementation.

    Returns
    -------
    torch.Tensor
        Stacked derivatives with shape ``(num_derivatives, num_points)`` for scalar
        input or ``(num_derivatives, num_points, channels)`` for vector input.
    """

    _BENCHMARK_CASES = (
        ("1d-scalar-n4096", 4096, 1, 0.01, 1, False, 1),
        ("2d-scalar-n4096-o1", 4096, 2, (0.01, 0.02), 1, False, 1),
        ("2d-vector-n4096-o2", 4096, 2, (0.01, 0.02), 2, True, 2),
        ("3d-scalar-n2048-o2", 2048, 3, (0.01, 0.015, 0.02), 2, True, 1),
    )

    @FunctionSpec.register(name="torch", rank=0, baseline=True)
    def torch_forward(
        stencil_values: Float[Tensor, "num_points stencil_size channels"],
        spacing: float | Sequence[float] = 1.0,
        order: int = 1,
        return_mixed_derivs: bool = False,
    ) -> Float[Tensor, "num_derivs num_points channels"]:
        """Dispatch meshless finite-difference derivatives to the torch backend."""
        return meshless_fd_derivatives_torch(
            stencil_values=stencil_values,
            spacing=spacing,
            order=order,
            return_mixed_derivs=return_mixed_derivs,
        )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        """Yield representative forward benchmark and parity input cases."""
        device = torch.device(device)
        for (
            label,
            num_points,
            dim,
            spacing,
            order,
            return_mixed_derivs,
            channels,
        ) in cls._BENCHMARK_CASES:
            points = torch.rand(num_points, dim, device=device, dtype=torch.float32)
            stencil_points = meshless_fd_stencil_points_torch(points, spacing=spacing)
            stencil_values = cls._evaluate_stencil(stencil_points, channels=channels)
            yield (
                label,
                (stencil_values,),
                {
                    "spacing": spacing,
                    "order": order,
                    "return_mixed_derivs": return_mixed_derivs,
                },
            )

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        """Yield representative backward benchmark and parity input cases."""
        device = torch.device(device)
        backward_cases = (
            ("1d-grad-n2048", 2048, 1, 0.01, 1, False, 2),
            ("2d-grad-n2048-o2", 2048, 2, (0.01, 0.02), 2, True, 2),
            ("3d-grad-n1024-o2", 1024, 3, (0.01, 0.015, 0.02), 2, True, 1),
        )
        for (
            label,
            num_points,
            dim,
            spacing,
            order,
            return_mixed_derivs,
            channels,
        ) in backward_cases:
            points = torch.rand(num_points, dim, device=device, dtype=torch.float32)
            stencil_points = meshless_fd_stencil_points_torch(points, spacing=spacing)
            stencil_values = (
                cls._evaluate_stencil(stencil_points, channels=channels)
                .detach()
                .clone()
                .requires_grad_(True)
            )
            yield (
                label,
                (stencil_values,),
                {
                    "spacing": spacing,
                    "order": order,
                    "return_mixed_derivs": return_mixed_derivs,
                },
            )

    @staticmethod
    def _evaluate_stencil(
        stencil_points: torch.Tensor,
        channels: int,
    ) -> torch.Tensor:
        """Generate smooth multi-channel stencil values for benchmark inputs."""
        x = stencil_points[..., 0]
        if stencil_points.shape[-1] == 1:
            values = [torch.sin(2.0 * x) + 0.3 * x.square()]
        elif stencil_points.shape[-1] == 2:
            y = stencil_points[..., 1]
            values = [
                torch.sin(1.4 * x) * torch.cos(0.7 * y) + 0.2 * x * y,
                x.square() + y.pow(3),
            ]
        else:
            y = stencil_points[..., 1]
            z = stencil_points[..., 2]
            values = [
                torch.sin(1.2 * x) * torch.cos(0.8 * y) * torch.sin(0.6 * z)
                + 0.1 * x * y * z,
                x.square() + 0.5 * y.square() - z,
            ]

        stacked = torch.stack(values[:channels], dim=-1)
        if channels == 1:
            return stacked[..., 0]
        return stacked


meshless_fd_derivatives = MeshlessFDDerivatives.make_function("meshless_fd_derivatives")


__all__ = [
    "MeshlessFDDerivatives",
    "meshless_fd_derivatives",
]
