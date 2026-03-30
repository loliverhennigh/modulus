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

from typing import TypeAlias

import torch
from jaxtyping import Float
from tensordict import TensorDict

TensorLike: TypeAlias = Float[torch.Tensor, "..."] | TensorDict


def _validate_last_dim(x: torch.Tensor, *, dim: int, name: str) -> None:
    """Validate the final dimension for basis-vector inputs."""
    if torch.compiler.is_compiling():
        return
    if x.shape[-1] != dim:
        raise ValueError(
            f"Expected {name} to have shape (..., {dim}), got shape {tuple(x.shape)}."
        )


def _safe_normalize(
    x: Float[torch.Tensor, "... n_dims"],
) -> Float[torch.Tensor, "... n_dims"]:
    """Normalize vectors and keep exact zeros for zero-length inputs."""
    norm = torch.linalg.norm(x, dim=-1, keepdim=True)
    normalized = x / norm.clamp_min(torch.finfo(x.dtype).eps)
    return torch.where(norm > 0, normalized, torch.zeros_like(x))


def _make_tensordict_input(num_elements: int, device: torch.device) -> TensorDict:
    """Create a simple TensorDict input used for benchmarks."""
    return TensorDict(
        {
            "a": torch.rand(num_elements, device=device),
            "b": torch.rand(num_elements, device=device),
        },
        batch_size=[num_elements],
    )


def _vector_project_impl(
    v: Float[torch.Tensor, "... n_dims"],
    n_hat: Float[torch.Tensor, "... n_dims"],
) -> Float[torch.Tensor, "... n_dims"]:
    """Project vectors onto the plane orthogonal to ``n_hat``."""
    return v - (v * n_hat).sum(dim=-1, keepdim=True) * n_hat


__all__ = [
    "TensorLike",
    "_validate_last_dim",
    "_safe_normalize",
    "_make_tensordict_input",
    "_vector_project_impl",
]
