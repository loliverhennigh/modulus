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

from typing import Sequence

import torch

from .utils import _as_spacing_tensor, _material_coefficients, _validate_common_inputs


# Apply one FDTD magnetic-field update step with periodic boundaries.
def magnetic_field_update_torch(
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    mu: float | torch.Tensor,
    sigma_m: float | torch.Tensor,
    spacing: torch.Tensor | Sequence[float],
    dt: float,
    inplace: bool = False,
) -> torch.Tensor:
    _validate_common_inputs(
        electric_field,
        magnetic_field,
        mu,
        sigma_m,
        spacing,
        inplace,
    )

    spacing_tensor = _as_spacing_tensor(
        spacing,
        device=magnetic_field.device,
        dtype=magnetic_field.dtype,
    )
    c_hh, c_he = _material_coefficients(
        mu,
        sigma_m,
        spacing_tensor,
        dt,
        spatial_shape=tuple(magnetic_field.shape[1:]),
        device=magnetic_field.device,
    )

    ex = electric_field[0]
    ey = electric_field[1]
    ez = electric_field[2]

    e_x_0_1_0 = torch.roll(ex, shifts=(0, -1, 0), dims=(0, 1, 2))
    e_x_0_0_1 = torch.roll(ex, shifts=(0, 0, -1), dims=(0, 1, 2))
    e_y_1_0_0 = torch.roll(ey, shifts=(-1, 0, 0), dims=(0, 1, 2))
    e_y_0_0_1 = torch.roll(ey, shifts=(0, 0, -1), dims=(0, 1, 2))
    e_z_1_0_0 = torch.roll(ez, shifts=(-1, 0, 0), dims=(0, 1, 2))
    e_z_0_1_0 = torch.roll(ez, shifts=(0, -1, 0), dims=(0, 1, 2))

    curl_e_x = (e_y_0_0_1 - ey) - (e_z_0_1_0 - ez)
    curl_e_y = (e_z_1_0_0 - ez) - (e_x_0_0_1 - ex)
    curl_e_z = (e_x_0_1_0 - ex) - (e_y_1_0_0 - ey)
    curl_e = torch.stack((curl_e_x, curl_e_y, curl_e_z), dim=0)

    updated = c_hh * magnetic_field + c_he * curl_e

    if inplace:
        magnetic_field.copy_(updated)
        return magnetic_field

    return updated
