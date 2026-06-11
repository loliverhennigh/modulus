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
from .utils import validate_vector_field


def uniform_grid_curl_torch(
    vector_field: torch.Tensor,
    spacing: float | Sequence[float] = 1.0,
    order: int = 2,
) -> torch.Tensor:
    """Compute periodic uniform-grid curl with PyTorch tensor ops."""
    grid_ndim = validate_vector_field(vector_field)
    jacobian_rows = [
        uniform_grid_gradient_torch_multi(
            field=vector_field[component],
            spacing=spacing,
            order=order,
            derivative_orders=1,
            include_mixed=False,
        )
        for component in range(grid_ndim)
    ]

    if grid_ndim == 2:
        return jacobian_rows[1][0] - jacobian_rows[0][1]

    curl_x = jacobian_rows[2][1] - jacobian_rows[1][2]
    curl_y = jacobian_rows[0][2] - jacobian_rows[2][0]
    curl_z = jacobian_rows[1][0] - jacobian_rows[0][1]
    return torch.stack((curl_x, curl_y, curl_z), dim=0)
