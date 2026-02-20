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

import torch
from torch import Tensor

from physicsnemo.core.function_spec import FunctionSpec


class WeightFact(FunctionSpec):
    """Randomly factorize the weight matrix into a product of vectors and a matrix.

    Parameters
    ----------
    w : torch.Tensor
        Weight tensor to factorize.
    mean : float, optional
        Mean of the normal distribution used to sample the scale factor.
    stddev : float, optional
        Standard deviation of the normal distribution used to sample the scale factor.
    implementation : {"torch"} or None
        Implementation to use. When ``None``, dispatch selects the available
        implementation.
    """

    _BENCHMARK_CASES = (
        ("small-256x256", 256),
        ("medium-512x512", 512),
        ("large-1024x1024", 1024),
    )

    @FunctionSpec.register(name="torch", rank=0, baseline=True)
    def torch_forward(w: Tensor, mean: float = 1.0, stddev: float = 0.1):
        g = torch.normal(mean, stddev, size=(w.shape[0], 1), device=w.device)
        g = torch.exp(g)
        v = w / g
        return g, v

    @classmethod
    def _iter_benchmark_cases(
        cls,
        device: torch.device | str = "cpu",
        *,
        include_backward_grads: bool = False,
    ):
        device = torch.device(device)
        for label, size in cls._BENCHMARK_CASES:
            w = torch.randn(size, size, device=device)
            if include_backward_grads:
                w.requires_grad_(True)
            yield (
                label,
                (w,),
                {"mean": 1.0, "stddev": 0.1},
            )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        yield from cls._iter_benchmark_cases(
            device=device,
            include_backward_grads=False,
        )

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        yield from cls._iter_benchmark_cases(
            device=device,
            include_backward_grads=True,
        )

    @classmethod
    def make_inputs(cls, device: torch.device | str = "cpu"):
        yield from cls.make_inputs_forward(device=device)

    @classmethod
    def make_input_labels_forward(cls, device: torch.device) -> list[str]:
        _ = device
        return [label for label, _ in cls._BENCHMARK_CASES]

    @classmethod
    def make_input_labels_backward(cls, device: torch.device) -> list[str]:
        _ = device
        return [label for label, _ in cls._BENCHMARK_CASES]


weight_fact = WeightFact.make_function("weight_fact")


__all__ = [
    "WeightFact",
    "weight_fact",
]
