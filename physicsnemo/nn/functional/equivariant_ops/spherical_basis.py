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

from ._common import _safe_normalize, _validate_last_dim, _vector_project_impl


def _spherical_basis_impl(
    r_hat: Float[torch.Tensor, "... 3"],
    n_hat: Float[torch.Tensor, "... 3"],
    normalize_basis_vectors: bool,
) -> tuple[
    Float[torch.Tensor, "... 3"],
    Float[torch.Tensor, "... 3"],
    Float[torch.Tensor, "... 3"],
]:
    _validate_last_dim(r_hat, dim=3, name="r_hat")
    _validate_last_dim(n_hat, dim=3, name="n_hat")

    e_r = r_hat

    e_theta = _vector_project_impl(-n_hat, r_hat)
    r_hat_is_zero = torch.all(r_hat == 0.0, dim=-1, keepdim=True)
    e_theta = torch.where(r_hat_is_zero, torch.zeros_like(e_theta), e_theta)

    if normalize_basis_vectors:
        e_theta = _safe_normalize(e_theta)

    e_phi = torch.cross(e_r, e_theta, dim=-1)
    return e_r, e_theta, e_phi


class SphericalBasis(FunctionSpec):
    r"""Compute a local spherical-like 3D basis aligned with ``n_hat``.

    Parameters
    ----------
    r_hat : Float[torch.Tensor, "... 3"]
        Unit radial direction vectors.
    n_hat : Float[torch.Tensor, "... 3"]
        Axis vectors.
    normalize_basis_vectors : bool, optional
        Whether to normalize ``e_theta`` and ``e_phi``.
    implementation : {"torch"} or None
        Implementation to use. When ``None``, dispatch selects the available
        implementation.
    """

    @FunctionSpec.register(name="torch", rank=0, baseline=True)
    def torch_forward(
        r_hat: Float[torch.Tensor, "... 3"],
        n_hat: Float[torch.Tensor, "... 3"],
        normalize_basis_vectors: bool = True,
    ) -> tuple[
        Float[torch.Tensor, "... 3"],
        Float[torch.Tensor, "... 3"],
        Float[torch.Tensor, "... 3"],
    ]:
        return _spherical_basis_impl(r_hat, n_hat, normalize_basis_vectors)

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        r_hat = torch.randn(1024, 3, device=device)
        r_hat = _safe_normalize(r_hat)
        n_hat = torch.tensor([0.0, 0.0, 1.0], device=device).repeat(1024, 1)
        yield (
            "basis-n1024",
            (r_hat, n_hat),
            {"normalize_basis_vectors": True},
        )

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        r_hat = torch.randn(1024, 3, device=device)
        r_hat = _safe_normalize(r_hat)
        n_hat = torch.tensor([0.0, 0.0, 1.0], device=device).repeat(1024, 1)
        yield (
            "basis-grad-n1024",
            (
                r_hat.requires_grad_(True),
                n_hat.requires_grad_(True),
            ),
            {"normalize_basis_vectors": True},
        )


spherical_basis = SphericalBasis.make_function("spherical_basis")


__all__ = [
    "SphericalBasis",
    "spherical_basis",
]
