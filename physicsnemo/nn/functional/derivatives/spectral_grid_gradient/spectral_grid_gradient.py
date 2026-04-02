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
from jaxtyping import Float
from torch import Tensor

from physicsnemo.core.function_spec import FunctionSpec

from ._torch_impl import spectral_grid_gradient_torch


class SpectralGridGradient(FunctionSpec):
    r"""Compute periodic derivatives with Fourier spectral differentiation.

    This functional computes first- or second-order derivatives on 1D/2D/3D
    periodic scalar fields by transforming to Fourier space, applying exact
    derivative multipliers, and transforming back.

    Parameters
    ----------
    field : torch.Tensor
        Scalar field on a periodic uniform grid with shape
        ``(n0,)``, ``(n0, n1)``, or ``(n0, n1, n2)``.
    lengths : float | Sequence[float], optional
        Physical domain lengths per axis. A scalar applies the same length to
        every axis.
    order : int, optional
        Derivative order, either ``1`` (first derivatives) or ``2``
        (second derivatives).
    return_mixed_derivs : bool, optional
        Whether to include mixed second derivatives. This is only valid when
        ``order=2`` and ``field.ndim >= 2``.
    implementation : {"torch"} or None
        Implementation to use. When ``None``, dispatch selects the available
        implementation.

    Returns
    -------
    torch.Tensor
        Stacked derivative tensor with shape ``(num_derivatives, *field.shape)``.
        Derivative ordering is:
        first order: ``[d/dx, d/dy, d/dz]`` (up to dimensionality);
        second order: pure second derivatives first, then mixed derivatives in
        axis-pair order ``(x,y), (x,z), (y,z)``.
    """

    _BENCHMARK_CASES = (
        ("1d-n4096-o1", (4096,), 2.0, 1, False),
        ("2d-512x512-o1", (512, 512), (2.0, 1.5), 1, False),
        ("2d-512x512-o2-mixed", (512, 512), (2.0, 1.5), 2, True),
        ("3d-128x128x128-o2", (128, 128, 128), (2.0, 1.5, 1.25), 2, False),
    )

    @FunctionSpec.register(name="torch", rank=0, baseline=True)
    def torch_forward(
        field: Float[Tensor, "..."],
        lengths: float | Sequence[float] = 1.0,
        order: int = 1,
        return_mixed_derivs: bool = False,
    ) -> Float[Tensor, "..."]:
        """Dispatch spectral derivatives to the PyTorch backend."""
        return spectral_grid_gradient_torch(
            field=field,
            lengths=lengths,
            order=order,
            return_mixed_derivs=return_mixed_derivs,
        )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        """Yield representative forward benchmark and parity input cases."""
        device = torch.device(device)
        for label, shape, lengths, order, return_mixed_derivs in cls._BENCHMARK_CASES:
            field = cls._make_periodic_field(
                shape=shape,
                lengths=lengths,
                device=device,
            )
            yield (
                label,
                (field,),
                {
                    "lengths": lengths,
                    "order": order,
                    "return_mixed_derivs": return_mixed_derivs,
                },
            )

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        """Yield representative backward benchmark and parity input cases."""
        device = torch.device(device)
        backward_cases = (
            ("1d-grad-n1024-o1", (1024,), 2.0, 1, False),
            ("2d-grad-256x256-o2", (256, 256), (2.0, 1.5), 2, False),
            ("3d-grad-64x64x64-o2-mixed", (64, 64, 64), (2.0, 1.5, 1.25), 2, True),
        )
        for label, shape, lengths, order, return_mixed_derivs in backward_cases:
            field = (
                cls._make_periodic_field(
                    shape=shape,
                    lengths=lengths,
                    device=device,
                )
                .detach()
                .clone()
                .requires_grad_(True)
            )
            yield (
                label,
                (field,),
                {
                    "lengths": lengths,
                    "order": order,
                    "return_mixed_derivs": return_mixed_derivs,
                },
            )

    @staticmethod
    def _make_periodic_field(
        shape: tuple[int, ...],
        lengths: float | Sequence[float],
        device: torch.device,
    ) -> torch.Tensor:
        """Construct smooth periodic fields for benchmark and test cases."""
        dim = len(shape)
        if isinstance(lengths, (float, int)):
            lengths_tuple = tuple(float(lengths) for _ in range(dim))
        else:
            lengths_tuple = tuple(float(v) for v in lengths)

        if dim == 1:
            n0 = shape[0]
            l0 = lengths_tuple[0]
            x0 = torch.arange(n0, device=device, dtype=torch.float32) * (l0 / n0)
            k0 = 2.0 * torch.pi / l0
            return torch.sin(k0 * x0) + 0.25 * torch.cos(2.0 * k0 * x0)

        if dim == 2:
            n0, n1 = shape
            l0, l1 = lengths_tuple
            x0 = torch.arange(n0, device=device, dtype=torch.float32) * (l0 / n0)
            x1 = torch.arange(n1, device=device, dtype=torch.float32) * (l1 / n1)
            xx, yy = torch.meshgrid(x0, x1, indexing="ij")
            k0 = 2.0 * torch.pi / l0
            k1 = 2.0 * torch.pi / l1
            return torch.sin(k0 * xx + 0.3) * torch.cos(k1 * yy - 0.2)

        n0, n1, n2 = shape
        l0, l1, l2 = lengths_tuple
        x0 = torch.arange(n0, device=device, dtype=torch.float32) * (l0 / n0)
        x1 = torch.arange(n1, device=device, dtype=torch.float32) * (l1 / n1)
        x2 = torch.arange(n2, device=device, dtype=torch.float32) * (l2 / n2)
        xx, yy, zz = torch.meshgrid(x0, x1, x2, indexing="ij")
        k0 = 2.0 * torch.pi / l0
        k1 = 2.0 * torch.pi / l1
        k2 = 2.0 * torch.pi / l2
        return (
            torch.sin(k0 * xx + 0.2)
            * torch.cos(k1 * yy - 0.4)
            * torch.sin(k2 * zz + 0.1)
        )


spectral_grid_gradient = SpectralGridGradient.make_function("spectral_grid_gradient")


__all__ = ["SpectralGridGradient", "spectral_grid_gradient"]
