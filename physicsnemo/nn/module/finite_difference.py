# SPDX-FileCopyrightText: Copyright (c) 2023 - 2025 NVIDIA CORPORATION & AFFILIATES.
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

from typing import Any, Iterable, Sequence, Tuple

import torch
from torch import Tensor

from physicsnemo.core import Module
from physicsnemo.nn.functional.finite_difference import (
    FiniteDifference,
    _normalize_spacing,
)


class FiniteDifferenceNd(Module):
    """Finite-difference stencil implemented with Warp-backed functionals.

    Parameters
    ----------
    spacing:
        Grid spacing for each spatial dimension. Provide a single value to
        reuse across all axes.
    has_batch:
        Whether the input tensors include a batch dimension as the first axis.
    """

    def __init__(
        self,
        spacing: Sequence[float] | float,
        has_batch: bool = True,
    ) -> None:
        super().__init__()
        if isinstance(spacing, Sequence) and not isinstance(spacing, (str, bytes)):
            self.spacing: Sequence[float] | float = tuple(float(s) for s in spacing)
        else:
            self.spacing = float(spacing)
        self.has_batch = has_batch

    def forward(self, values: Tensor) -> Tensor:
        """Apply the finite difference stencil."""

        if not torch.is_tensor(values):
            raise TypeError("values must be a torch.Tensor")
        dims = values.dim() - (1 if self.has_batch else 0)
        spacing_tuple = _normalize_spacing(self.spacing, dims)
        return FiniteDifference.apply(values, spacing_tuple, self.has_batch)

    @classmethod
    def make_inputs(
        cls,
    ) -> Iterable[tuple[str, dict[str, Any], Tuple[Tensor]]]:
        configs = {
            "1D": ((1, 64), (1.0,)),
            "2D": ((1, 32, 32), (1.0, 1.2)),
            "3D": ((1, 16, 16, 16), (0.8, 1.0, 1.2)),
        }
        device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
        for label, (shape, spacing) in configs.items():
            values = torch.randn(shape, device=device, dtype=torch.float32)
            init_kwargs: dict[str, Any] = {
                "spacing": spacing,
                "has_batch": True,
            }
            yield label, init_kwargs, (values,)

    @classmethod
    def reference_impl(
        cls, values: Tensor, spacing: Sequence[float] | float, has_batch: bool = True
    ) -> Tensor:
        dims = values.dim() - (1 if has_batch else 0)
        spacing_tuple = _normalize_spacing(spacing, dims)
        return FiniteDifference.reference_impl(values, spacing_tuple, has_batch)

    @classmethod
    def _resolve_reference_runner(cls, instance):
        if instance is None:
            return super()._resolve_reference_runner(instance)

        spacing = instance.spacing
        has_batch = instance.has_batch

        def runner(values: Tensor) -> Tensor:
            return cls.reference_impl(values, spacing, has_batch)

        return runner

    @classmethod
    def check(cls, actual: Tensor, expected: Tensor) -> None:
        FiniteDifference.check(actual, expected)


__all__ = ["FiniteDifferenceNd"]
