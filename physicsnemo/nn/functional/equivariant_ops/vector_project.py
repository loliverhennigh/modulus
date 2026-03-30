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

from physicsnemo.core.function_spec import FunctionSpec

from ._common import _vector_project_impl


class VectorProject(FunctionSpec):
    r"""Project vectors onto the plane orthogonal to a normal vector.

    Parameters
    ----------
    v : Float[torch.Tensor, "... n_dims"]
        Input vectors to project.
    n_hat : Float[torch.Tensor, "... n_dims"]
        Unit normal vectors defining the projection plane.
    implementation : {"torch"} or None
        Implementation to use. When ``None``, dispatch selects the available
        implementation.
    """

    @FunctionSpec.register(name="torch", rank=0, baseline=True)
    def torch_forward(
        v: Float[torch.Tensor, "... n_dims"],
        n_hat: Float[torch.Tensor, "... n_dims"],
    ) -> Float[torch.Tensor, "... n_dims"]:
        return _vector_project_impl(v, n_hat)

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        v = torch.randn(2048, 3, device=device)
        n_hat = torch.randn(2048, 3, device=device)
        n_hat = n_hat / torch.linalg.norm(n_hat, dim=-1, keepdim=True)
        yield ("vectors-n2048-d3", (v, n_hat), {})

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        v = torch.randn(2048, 3, device=device, requires_grad=True)
        n_hat = torch.randn(2048, 3, device=device)
        n_hat = n_hat / torch.linalg.norm(n_hat, dim=-1, keepdim=True)
        n_hat = n_hat.requires_grad_(True)
        yield ("vectors-grad-n2048-d3", (v, n_hat), {})


vector_project = VectorProject.make_function("vector_project")


__all__ = [
    "VectorProject",
    "vector_project",
]
