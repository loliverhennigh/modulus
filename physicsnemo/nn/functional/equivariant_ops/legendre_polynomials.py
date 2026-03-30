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
from jaxtyping import Float
from tensordict import TensorDict

from physicsnemo.core.function_spec import FunctionSpec

from ._common import TensorLike, _make_tensordict_input


def _legendre_polynomials_impl(x: TensorLike, n: int) -> list[TensorLike]:
    if n < 0:
        raise ValueError(f"n must be non-negative, got {n=}")
    if n == 0:
        return []

    if isinstance(x, TensorDict):
        polynomials: list[TensorDict] = [x.apply(torch.ones_like), x][:n]
        for i in range(2, n):
            p_i = (
                (2 * i - 1) * x * polynomials[i - 1] - (i - 1) * polynomials[i - 2]
            ) / i
            polynomials.append(p_i)
        return polynomials

    polynomials_t: list[Float[torch.Tensor, "..."]] = [torch.ones_like(x), x][:n]
    for i in range(2, n):
        p_i = (
            (2 * i - 1) * x * polynomials_t[i - 1] - (i - 1) * polynomials_t[i - 2]
        ) / i
        polynomials_t.append(p_i)

    return polynomials_t


class LegendrePolynomials(FunctionSpec):
    r"""Compute Legendre polynomials ``P_0`` through ``P_{n-1}`` at ``x``.

    Parameters
    ----------
    x : Float[torch.Tensor, "..."] or TensorDict
        Input tensor-like values.
    n : int
        Number of Legendre polynomials to evaluate.
    implementation : {"torch"} or None
        Implementation to use. When ``None``, dispatch selects the available
        implementation.
    """

    @FunctionSpec.register(name="torch", rank=0, baseline=True)
    def torch_forward(x: TensorLike, n: int) -> list[TensorLike]:
        return _legendre_polynomials_impl(x, n)

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        yield ("tensor-n1024-p8", (torch.rand(1024, device=device), 8), {})
        yield (
            "tensordict-n512-p6",
            (_make_tensordict_input(512, device), 6),
            {},
        )

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        yield (
            "tensor-grad-n1024-p8",
            (torch.rand(1024, device=device, requires_grad=True), 8),
            {},
        )


legendre_polynomials = LegendrePolynomials.make_function("legendre_polynomials")


__all__ = [
    "LegendrePolynomials",
    "legendre_polynomials",
]
