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

import torch
from jaxtyping import Float

from physicsnemo.core.function_spec import FunctionSpec

from ._common import _safe_normalize, _validate_last_dim, _vector_project_impl


def _polar_and_dipole_basis_impl(
    r_hat: Float[torch.Tensor, "... 2"],
    n_hat: Float[torch.Tensor, "... 2"],
    normalize_basis_vectors: bool,
) -> tuple[
    Float[torch.Tensor, "... 2"],
    Float[torch.Tensor, "... 2"],
    Float[torch.Tensor, "... 2"],
]:
    _validate_last_dim(r_hat, dim=2, name="r_hat")
    _validate_last_dim(n_hat, dim=2, name="n_hat")

    e_r = r_hat
    e_theta = torch.stack((-r_hat[..., 1], r_hat[..., 0]), dim=-1)

    e_kappa = _vector_project_impl(-n_hat, r_hat)
    r_hat_is_zero = torch.all(r_hat == 0.0, dim=-1, keepdim=True)
    e_kappa = torch.where(r_hat_is_zero, torch.zeros_like(e_kappa), e_kappa)

    if normalize_basis_vectors:
        e_kappa = _safe_normalize(e_kappa)

    return e_r, e_theta, e_kappa


class PolarAndDipoleBasis(FunctionSpec):
    r"""Compute a local 2D basis aligned with ``r_hat`` and conditioned on ``n_hat``.

    Parameters
    ----------
    r_hat : Float[torch.Tensor, "... 2"]
        Unit direction vectors.
    n_hat : Float[torch.Tensor, "... 2"]
        Axis vectors.
    normalize_basis_vectors : bool, optional
        Whether to normalize ``e_kappa`` to unit length.
    implementation : {"torch"} or None
        Implementation to use. When ``None``, dispatch selects the available
        implementation.
    """

    @FunctionSpec.register(name="torch", rank=0, baseline=True)
    def torch_forward(
        r_hat: Float[torch.Tensor, "... 2"],
        n_hat: Float[torch.Tensor, "... 2"],
        normalize_basis_vectors: bool = True,
    ) -> tuple[
        Float[torch.Tensor, "... 2"],
        Float[torch.Tensor, "... 2"],
        Float[torch.Tensor, "... 2"],
    ]:
        return _polar_and_dipole_basis_impl(r_hat, n_hat, normalize_basis_vectors)

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        theta = torch.linspace(0.0, 2.0 * math.pi, 2048, device=device)
        r_hat = torch.stack((torch.cos(theta), torch.sin(theta)), dim=-1)
        n_hat = torch.tensor([1.0, 0.0], device=device).repeat(2048, 1)
        yield (
            "basis-n2048",
            (r_hat, n_hat),
            {"normalize_basis_vectors": True},
        )

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        theta = torch.linspace(0.0, 2.0 * math.pi, 2048, device=device)
        r_hat = torch.stack((torch.cos(theta), torch.sin(theta)), dim=-1)
        n_hat = torch.tensor([1.0, 0.0], device=device).repeat(2048, 1)
        yield (
            "basis-grad-n2048",
            (
                r_hat.requires_grad_(True),
                n_hat.requires_grad_(True),
            ),
            {"normalize_basis_vectors": True},
        )


polar_and_dipole_basis = PolarAndDipoleBasis.make_function("polar_and_dipole_basis")


__all__ = [
    "PolarAndDipoleBasis",
    "polar_and_dipole_basis",
]
