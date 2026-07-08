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

import math
from collections.abc import Sequence

import torch


def normalize_spectral_lengths(
    lengths: float | Sequence[float], ndim: int
) -> tuple[float, ...]:
    """Normalize periodic lengths into one positive entry per spatial axis."""
    if isinstance(lengths, (float, int)):
        lengths_tuple = tuple(float(lengths) for _ in range(ndim))
    else:
        lengths_tuple = tuple(float(value) for value in lengths)
        if len(lengths_tuple) != ndim:
            raise ValueError(
                f"lengths must have {ndim} entries for a {ndim}D field, "
                f"got {len(lengths_tuple)}"
            )

    for axis, length in enumerate(lengths_tuple):
        if not math.isfinite(length) or length <= 0.0:
            raise ValueError(f"lengths[{axis}] must be finite and strictly positive")
    return lengths_tuple


def promote_spectral_input(field: torch.Tensor) -> torch.Tensor:
    """Promote low-precision real inputs to an FFT-supported evaluation dtype."""
    if field.dtype in (torch.float16, torch.bfloat16):
        return field.to(torch.float32)
    return field


def spectral_wavenumbers(
    shape: Sequence[int],
    lengths: Sequence[float],
    *,
    device: torch.device,
    dtype: torch.dtype,
) -> list[torch.Tensor]:
    """Build broadcastable angular wavenumbers for each spatial axis."""
    wavenumbers: list[torch.Tensor] = []
    for axis, (axis_size, axis_length) in enumerate(zip(shape, lengths)):
        frequency = torch.fft.fftfreq(
            axis_size,
            d=axis_length / float(axis_size),
            device=device,
            dtype=dtype,
        )
        wavenumber = 2.0 * torch.pi * frequency
        view_shape = [1] * len(shape)
        view_shape[axis] = axis_size
        wavenumbers.append(wavenumber.reshape(view_shape))
    return wavenumbers


def validate_spectral_scalar_field(
    field: torch.Tensor,
    *,
    function_name: str,
) -> tuple[tuple[int, ...], torch.Tensor]:
    """Validate an unbatched 1D-3D scalar field and promote its dtype."""
    if field.ndim < 1 or field.ndim > 3:
        raise ValueError(
            f"{function_name} supports 1D-3D fields, "
            f"got field.shape={tuple(field.shape)}"
        )
    if not torch.is_floating_point(field):
        raise TypeError("field must be a floating-point tensor")
    return tuple(field.shape), promote_spectral_input(field)


def validate_spectral_vector_field(
    vector_field: torch.Tensor,
    *,
    function_name: str,
    allowed_dims: tuple[int, ...],
) -> tuple[int, tuple[int, ...], torch.Tensor]:
    """Validate an unbatched channel-first vector field and promote its dtype."""
    grid_ndim = vector_field.ndim - 1
    if grid_ndim not in allowed_dims:
        supported = " or ".join(f"{dim}D" for dim in allowed_dims)
        raise ValueError(
            f"{function_name} supports {supported} vector fields, "
            f"got vector_field.shape={tuple(vector_field.shape)}"
        )
    if vector_field.shape[0] != grid_ndim:
        raise ValueError(
            "vector_field.shape[0] must equal the number of spatial dimensions "
            f"({grid_ndim}), got {vector_field.shape[0]}"
        )
    if not torch.is_floating_point(vector_field):
        raise TypeError("vector_field must be a floating-point tensor")
    return (
        grid_ndim,
        tuple(vector_field.shape[1:]),
        promote_spectral_input(vector_field),
    )


def restore_spectral_dtype(
    output: torch.Tensor, input_dtype: torch.dtype
) -> torch.Tensor:
    """Cast an FFT result back to the public input dtype when it was promoted."""
    if output.dtype != input_dtype:
        return output.to(dtype=input_dtype)
    return output
