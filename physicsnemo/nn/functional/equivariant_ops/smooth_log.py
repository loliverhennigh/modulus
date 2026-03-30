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

from physicsnemo.core.function_spec import FunctionSpec

from ._common import TensorLike, _make_tensordict_input


def _smooth_log_impl(x: TensorLike) -> TensorLike:
    return (-x).expm1().neg() * x.log1p()


class SmoothLog(FunctionSpec):
    r"""Apply a smooth logarithm-like map elementwise.

    Parameters
    ----------
    x : Float[torch.Tensor, "..."] or TensorDict
        Input tensor-like values.
    implementation : {"torch"} or None
        Implementation to use. When ``None``, dispatch selects the available
        implementation.
    """

    @FunctionSpec.register(name="torch", rank=0, baseline=True)
    def torch_forward(x: TensorLike) -> TensorLike:
        return _smooth_log_impl(x)

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        yield ("tensor-n1024", (torch.rand(1024, device=device),), {})
        yield ("tensordict-n512", (_make_tensordict_input(512, device),), {})

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        yield (
            "tensor-grad-n1024",
            (torch.rand(1024, device=device, requires_grad=True),),
            {},
        )


smooth_log = SmoothLog.make_function("smooth_log")


__all__ = [
    "SmoothLog",
    "smooth_log",
]
