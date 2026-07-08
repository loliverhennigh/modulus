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

from .._spectral_grid_utils import (
    normalize_spectral_lengths,
    restore_spectral_dtype,
    spectral_wavenumbers,
    validate_spectral_scalar_field,
)


def spectral_grid_laplacian_torch(
    field: torch.Tensor,
    lengths: float | Sequence[float] = 1.0,
) -> torch.Tensor:
    """Compute a periodic scalar Laplacian directly in Fourier space."""
    grid_shape, field_eval = validate_spectral_scalar_field(
        field,
        function_name="spectral_grid_laplacian",
    )
    grid_ndim = field_eval.ndim
    lengths_tuple = normalize_spectral_lengths(lengths, grid_ndim)
    spatial_dims = tuple(range(grid_ndim))
    field_hat = torch.fft.fftn(field_eval, dim=spatial_dims)
    wavenumbers = spectral_wavenumbers(
        grid_shape,
        lengths_tuple,
        device=field_eval.device,
        dtype=field_eval.dtype,
    )

    squared_wavenumber = wavenumbers[0].square()
    for axis in range(1, grid_ndim):
        squared_wavenumber = squared_wavenumber + wavenumbers[axis].square()
    output = torch.fft.ifftn(
        -squared_wavenumber * field_hat,
        dim=spatial_dims,
    ).real
    return restore_spectral_dtype(output, field.dtype)
