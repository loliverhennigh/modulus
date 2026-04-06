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

from .utils import (
    axis_central_weights,
    validate_and_normalize_coordinates,
    validate_field,
)


def rectilinear_grid_gradient_torch(
    field: torch.Tensor,
    coordinates: Sequence[torch.Tensor],
    periods: float | Sequence[float] | None = None,
) -> torch.Tensor:
    """Compute periodic rectilinear-grid gradients with PyTorch tensor ops."""
    ### Validate field and coordinate inputs.
    validate_field(field)

    coords_tuple, period_tuple = validate_and_normalize_coordinates(
        field=field,
        coordinates=coordinates,
        periods=periods,
        coordinates_dtype=field.dtype,
        requires_grad_error="coordinate gradients are not supported; pass detached coordinates",
    )

    ### Compute per-axis nonuniform periodic central-difference derivatives.
    gradients: list[torch.Tensor] = []
    for axis in range(field.ndim):
        w_minus, w_center, w_plus = axis_central_weights(
            coords_tuple[axis],
            period_tuple[axis],
        )

        view_shape = [1] * field.ndim
        view_shape[axis] = field.shape[axis]
        w_minus = w_minus.view(view_shape)
        w_center = w_center.view(view_shape)
        w_plus = w_plus.view(view_shape)

        grad_axis = (
            w_minus * torch.roll(field, shifts=1, dims=axis)
            + w_center * field
            + w_plus * torch.roll(field, shifts=-1, dims=axis)
        )
        gradients.append(grad_axis)

    ### Stack axis-wise derivatives into (dims, *field.shape).
    return torch.stack(gradients, dim=0)
