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
import warp as wp

from ._kernels import (
    _uniform_grid_derivatives_1d_order2_fused_kernel,
    _uniform_grid_derivatives_2d_order2_fused_kernel,
    _uniform_grid_derivatives_2d_order2_fused_no_mixed_kernel,
    _uniform_grid_derivatives_3d_order2_fused_kernel,
    _uniform_grid_derivatives_3d_order2_fused_no_mixed_kernel,
    _uniform_grid_gradient_1d_kernel,
    _uniform_grid_gradient_1d_order4_kernel,
    _uniform_grid_gradient_2d_kernel,
    _uniform_grid_gradient_2d_order4_kernel,
    _uniform_grid_gradient_3d_kernel,
    _uniform_grid_gradient_3d_order4_kernel,
    _uniform_grid_second_derivative_1d_kernel,
    _uniform_grid_second_derivative_1d_order4_kernel,
    _uniform_grid_second_derivative_2d_kernel,
    _uniform_grid_second_derivative_2d_order4_kernel,
    _uniform_grid_second_derivative_3d_kernel,
    _uniform_grid_second_derivative_3d_order4_kernel,
)
from .utils import _wp_launch

_FORWARD_KERNELS = {
    (1, 1, 2): _uniform_grid_gradient_1d_kernel,
    (1, 1, 4): _uniform_grid_gradient_1d_order4_kernel,
    (1, 2, 2): _uniform_grid_second_derivative_1d_kernel,
    (1, 2, 4): _uniform_grid_second_derivative_1d_order4_kernel,
    (2, 1, 2): _uniform_grid_gradient_2d_kernel,
    (2, 1, 4): _uniform_grid_gradient_2d_order4_kernel,
    (2, 2, 2): _uniform_grid_second_derivative_2d_kernel,
    (2, 2, 4): _uniform_grid_second_derivative_2d_order4_kernel,
    (3, 1, 2): _uniform_grid_gradient_3d_kernel,
    (3, 1, 4): _uniform_grid_gradient_3d_order4_kernel,
    (3, 2, 2): _uniform_grid_second_derivative_3d_kernel,
    (3, 2, 4): _uniform_grid_second_derivative_3d_order4_kernel,
}

_FUSED_FORWARD_KERNELS = {
    (1, False): _uniform_grid_derivatives_1d_order2_fused_kernel,
    (1, True): _uniform_grid_derivatives_1d_order2_fused_kernel,
    (2, False): _uniform_grid_derivatives_2d_order2_fused_no_mixed_kernel,
    (2, True): _uniform_grid_derivatives_2d_order2_fused_kernel,
    (3, False): _uniform_grid_derivatives_3d_order2_fused_no_mixed_kernel,
    (3, True): _uniform_grid_derivatives_3d_order2_fused_kernel,
}


def _launch_dim(shape: torch.Size) -> int | tuple[int, ...]:
    """Return Warp launch dimensions for 1D vs ND kernels."""
    return shape[0] if len(shape) == 1 else tuple(shape)


def _inverse_spacings(spacing_tuple: tuple[float, ...], power: int) -> list[float]:
    """Compute inverse spacing terms with optional square for second derivatives."""
    if power == 1:
        return [1.0 / float(dx) for dx in spacing_tuple]
    return [1.0 / float(dx * dx) for dx in spacing_tuple]


def _mixed_inverse_spacings(spacing_tuple: tuple[float, ...]) -> list[float]:
    """Compute inverse mixed spacing terms in axis-pair order."""
    return [
        1.0 / float(spacing_tuple[i] * spacing_tuple[j])
        for i in range(len(spacing_tuple))
        for j in range(i + 1, len(spacing_tuple))
    ]


def _to_wp_components(components: list[torch.Tensor], count: int) -> list[wp.array]:
    """Convert the leading tensor components to Warp arrays."""
    return [wp.from_torch(components[i], dtype=wp.float32) for i in range(count)]


def _launch_forward(
    *,
    field_fp32: torch.Tensor,
    spacing_tuple: tuple[float, ...],
    order: int,
    derivative_order: int,
    grad_components: list[torch.Tensor],
    wp_device,
    wp_stream,
) -> None:
    ### Launch dimensionality/order-specific forward kernels.
    ndim = field_fp32.ndim
    kernel = _FORWARD_KERNELS[(ndim, derivative_order, order)]
    local_spacing = spacing_tuple[:ndim]
    inv_terms = _inverse_spacings(
        local_spacing,
        power=1 if derivative_order == 1 else 2,
    )

    _wp_launch(
        kernel=kernel,
        dim=_launch_dim(field_fp32.shape),
        inputs=[
            wp.from_torch(field_fp32, dtype=wp.float32),
            *inv_terms,
            *_to_wp_components(grad_components, ndim),
        ],
        device=wp_device,
        stream=wp_stream,
    )


def _launch_forward_fused_order2(
    *,
    field_fp32: torch.Tensor,
    spacing_tuple: tuple[float, ...],
    first_components: list[torch.Tensor],
    second_components: list[torch.Tensor],
    mixed_components: list[torch.Tensor],
    include_mixed: bool,
    wp_device,
    wp_stream,
) -> None:
    """Launch fused first/second/mixed derivative kernels (order=2 only)."""
    ndim = field_fp32.ndim
    kernel = _FUSED_FORWARD_KERNELS[(ndim, include_mixed)]
    local_spacing = spacing_tuple[:ndim]
    inv_first = _inverse_spacings(local_spacing, power=1)
    inv_second = _inverse_spacings(local_spacing, power=2)

    inputs: list = [
        wp.from_torch(field_fp32, dtype=wp.float32),
        *inv_first,
        *inv_second,
    ]
    if include_mixed and ndim > 1:
        inputs.extend(_mixed_inverse_spacings(local_spacing))

    inputs.extend(_to_wp_components(first_components, ndim))
    inputs.extend(_to_wp_components(second_components, ndim))

    if include_mixed and ndim > 1:
        mixed_count = ndim * (ndim - 1) // 2
        inputs.extend(_to_wp_components(mixed_components, mixed_count))

    _wp_launch(
        kernel=kernel,
        dim=_launch_dim(field_fp32.shape),
        inputs=inputs,
        device=wp_device,
        stream=wp_stream,
    )
