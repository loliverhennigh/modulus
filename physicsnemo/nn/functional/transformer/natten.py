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

from typing import Any, Callable

import torch
from torch.overrides import handle_torch_function, has_torch_function

from physicsnemo.core.function_spec import FunctionSpec
from physicsnemo.core.version_check import OptionalImport

_natten = OptionalImport("natten")


class _NeighborhoodAttentionBase(FunctionSpec):
    """Shared FunctionSpec behavior for neighborhood attention wrappers."""

    _public_function: Callable[..., torch.Tensor] | None = None
    _BENCHMARK_CASES: tuple[tuple[str, tuple[int, ...], int, int, int], ...] = ()

    @classmethod
    def dispatch(
        cls,
        q: torch.Tensor,
        k: torch.Tensor,
        v: torch.Tensor,
        kernel_size: int,
        dilation: int = 1,
        **kwargs: Any,
    ) -> torch.Tensor:
        """Dispatch with ``__torch_function__`` interception for tensor subclasses."""
        implementation = kwargs.pop("implementation", None)

        if has_torch_function((q, k, v)):
            if cls._public_function is None:
                raise RuntimeError(
                    f"{cls.__name__} public function is not configured for dispatch"
                )
            return handle_torch_function(
                cls._public_function,
                (q, k, v),
                q,
                k,
                v,
                kernel_size,
                dilation=dilation,
                **kwargs,
            )

        if implementation is not None:
            kwargs["implementation"] = implementation

        return super().dispatch(
            q,
            k,
            v,
            kernel_size,
            dilation=dilation,
            **kwargs,
        )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        for label, spatial_shape, num_heads, head_dim, kernel_size in cls._BENCHMARK_CASES:
            shape = (1, *spatial_shape, num_heads, head_dim)
            q = torch.randn(shape, device=device)
            k = torch.randn_like(q)
            v = torch.randn_like(q)
            yield (
                label,
                (q, k, v, kernel_size),
                {"dilation": 1},
            )

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        for label, spatial_shape, num_heads, head_dim, kernel_size in cls._BENCHMARK_CASES:
            shape = (1, *spatial_shape, num_heads, head_dim)
            q = torch.randn(shape, device=device, requires_grad=True)
            k = torch.randn(shape, device=device, requires_grad=True)
            v = torch.randn(shape, device=device, requires_grad=True)
            yield (
                f"{label}-grad",
                (q, k, v, kernel_size),
                {"dilation": 1},
            )


class NA1D(_NeighborhoodAttentionBase):
    r"""Compute 1D neighborhood attention.

    This is a wrapper around :func:`natten.functional.na1d` with support for
    ``__torch_function__`` dispatch, which enables tensor subclasses (for
    example ``ShardTensor``) to intercept the call.

    Parameters
    ----------
    q : torch.Tensor
        Query tensor of shape :math:`(B, L, \text{heads}, D)`.
    k : torch.Tensor
        Key tensor of shape :math:`(B, L, \text{heads}, D)`.
    v : torch.Tensor
        Value tensor of shape :math:`(B, L, \text{heads}, D)`.
    kernel_size : int
        Attention kernel size.
    dilation : int, optional
        Kernel dilation factor.
    implementation : {"natten"} or None
        Implementation to use. When ``None``, dispatch selects the available
        implementation.
    """

    _BENCHMARK_CASES = (
        ("small-l64-h2-d16-k3", (64,), 2, 16, 3),
        ("medium-l128-h4-d16-k5", (128,), 4, 16, 5),
    )

    @FunctionSpec.register(
        name="natten",
        required_imports=("natten>=0.21.5",),
        rank=0,
        baseline=True,
    )
    def natten_forward(
        q: torch.Tensor,
        k: torch.Tensor,
        v: torch.Tensor,
        kernel_size: int,
        dilation: int = 1,
        **kwargs: Any,
    ) -> torch.Tensor:
        return _natten.functional.na1d(
            q,
            k,
            v,
            kernel_size,
            dilation=dilation,
            **kwargs,
        )


class NA2D(_NeighborhoodAttentionBase):
    r"""Compute 2D neighborhood attention.

    This is a wrapper around :func:`natten.functional.na2d` with support for
    ``__torch_function__`` dispatch, which enables tensor subclasses (for
    example ``ShardTensor``) to intercept the call.

    Parameters
    ----------
    q : torch.Tensor
        Query tensor of shape :math:`(B, H, W, \text{heads}, D)`.
    k : torch.Tensor
        Key tensor of shape :math:`(B, H, W, \text{heads}, D)`.
    v : torch.Tensor
        Value tensor of shape :math:`(B, H, W, \text{heads}, D)`.
    kernel_size : int
        Attention kernel size.
    dilation : int, optional
        Kernel dilation factor.
    implementation : {"natten"} or None
        Implementation to use. When ``None``, dispatch selects the available
        implementation.
    """

    _BENCHMARK_CASES = (
        ("small-h16-w16-h2-d16-k3", (16, 16), 2, 16, 3),
        ("medium-h32-w32-h4-d16-k5", (32, 32), 4, 16, 5),
    )

    @FunctionSpec.register(
        name="natten",
        required_imports=("natten>=0.21.5",),
        rank=0,
        baseline=True,
    )
    def natten_forward(
        q: torch.Tensor,
        k: torch.Tensor,
        v: torch.Tensor,
        kernel_size: int,
        dilation: int = 1,
        **kwargs: Any,
    ) -> torch.Tensor:
        return _natten.functional.na2d(
            q,
            k,
            v,
            kernel_size,
            dilation=dilation,
            **kwargs,
        )


class NA3D(_NeighborhoodAttentionBase):
    r"""Compute 3D neighborhood attention.

    This is a wrapper around :func:`natten.functional.na3d` with support for
    ``__torch_function__`` dispatch, which enables tensor subclasses (for
    example ``ShardTensor``) to intercept the call.

    Parameters
    ----------
    q : torch.Tensor
        Query tensor of shape :math:`(B, X, Y, Z, \text{heads}, D)`.
    k : torch.Tensor
        Key tensor of shape :math:`(B, X, Y, Z, \text{heads}, D)`.
    v : torch.Tensor
        Value tensor of shape :math:`(B, X, Y, Z, \text{heads}, D)`.
    kernel_size : int
        Attention kernel size.
    dilation : int, optional
        Kernel dilation factor.
    implementation : {"natten"} or None
        Implementation to use. When ``None``, dispatch selects the available
        implementation.
    """

    _BENCHMARK_CASES = (
        ("small-x8-y8-z8-h2-d8-k3", (8, 8, 8), 2, 8, 3),
        ("medium-x12-y12-z12-h2-d8-k5", (12, 12, 12), 2, 8, 5),
    )

    @FunctionSpec.register(
        name="natten",
        required_imports=("natten>=0.21.5",),
        rank=0,
        baseline=True,
    )
    def natten_forward(
        q: torch.Tensor,
        k: torch.Tensor,
        v: torch.Tensor,
        kernel_size: int,
        dilation: int = 1,
        **kwargs: Any,
    ) -> torch.Tensor:
        return _natten.functional.na3d(
            q,
            k,
            v,
            kernel_size,
            dilation=dilation,
            **kwargs,
        )


na1d = NA1D.make_function("na1d")
NA1D._public_function = na1d

na2d = NA2D.make_function("na2d")
NA2D._public_function = na2d

na3d = NA3D.make_function("na3d")
NA3D._public_function = na3d


__all__ = [
    "NA1D",
    "NA2D",
    "NA3D",
    "na1d",
    "na2d",
    "na3d",
]
