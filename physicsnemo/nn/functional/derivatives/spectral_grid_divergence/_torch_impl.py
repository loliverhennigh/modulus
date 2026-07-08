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
    validate_spectral_vector_field,
)


def spectral_grid_divergence_torch(
    vector_field: torch.Tensor,
    lengths: float | Sequence[float] = 1.0,
) -> torch.Tensor:
    """Compute periodic divergence directly in Fourier space."""
    grid_ndim, grid_shape, vector_eval = validate_spectral_vector_field(
        vector_field,
        function_name="spectral_grid_divergence",
        allowed_dims=(1, 2, 3),
    )
    lengths_tuple = normalize_spectral_lengths(lengths, grid_ndim)
    spatial_dims = tuple(range(1, vector_eval.ndim))
    vector_hat = torch.fft.fftn(vector_eval, dim=spatial_dims)
    wavenumbers = spectral_wavenumbers(
        grid_shape,
        lengths_tuple,
        device=vector_eval.device,
        dtype=vector_eval.dtype,
    )

    divergence_hat = 1j * wavenumbers[0] * vector_hat[0]
    for axis in range(1, grid_ndim):
        divergence_hat = divergence_hat + 1j * wavenumbers[axis] * vector_hat[axis]

    output = torch.fft.ifftn(
        divergence_hat,
        dim=tuple(range(grid_ndim)),
    ).real
    return restore_spectral_dtype(output, vector_field.dtype)
