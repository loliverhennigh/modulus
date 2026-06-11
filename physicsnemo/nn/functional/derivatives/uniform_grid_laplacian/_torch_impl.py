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

from ..uniform_grid_gradient._torch_impl import uniform_grid_gradient_torch_multi
from .utils import validate_scalar_field


def uniform_grid_laplacian_torch(
    field: torch.Tensor,
    spacing: float | Sequence[float] = 1.0,
    order: int = 2,
) -> torch.Tensor:
    """Compute periodic uniform-grid Laplacian with PyTorch tensor ops."""
    validate_scalar_field(field)
    second_derivatives = uniform_grid_gradient_torch_multi(
        field=field,
        spacing=spacing,
        order=order,
        derivative_orders=2,
        include_mixed=False,
    )
    return second_derivatives.sum(dim=0)
