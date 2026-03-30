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

import pytest
import torch
from tensordict import TensorDict

from physicsnemo.nn.functional.equivariant_ops import (
    polar_and_dipole_basis,
    smooth_log,
    spherical_basis,
    vector_project,
)


def test_smooth_log_tensordict_matches_leafwise() -> None:
    """TensorDict outputs should match applying smooth_log to each tensor leaf."""
    x = TensorDict(
        {
            "a": torch.tensor([0.0, 1.0, 2.0], dtype=torch.float32),
            "b": torch.tensor([3.0, 4.0, 5.0], dtype=torch.float32),
        },
        batch_size=[3],
    )

    y = smooth_log(x)

    assert isinstance(y, TensorDict)
    torch.testing.assert_close(y["a"], smooth_log(x["a"]))
    torch.testing.assert_close(y["b"], smooth_log(x["b"]))


@pytest.mark.parametrize("normalize_basis_vectors", [False, True])
def test_polar_basis_zero_vectors_stay_zero(
    normalize_basis_vectors: bool,
) -> None:
    """Zero 2D direction vectors should produce finite zero basis vectors."""
    r_hat = torch.zeros(5, 2, dtype=torch.float32)
    n_hat = torch.tensor([1.0, 0.0], dtype=torch.float32).repeat(5, 1)

    e_r, e_theta, e_kappa = polar_and_dipole_basis(
        r_hat,
        n_hat,
        normalize_basis_vectors=normalize_basis_vectors,
    )

    torch.testing.assert_close(e_r, torch.zeros_like(e_r))
    torch.testing.assert_close(e_theta, torch.zeros_like(e_theta))
    torch.testing.assert_close(e_kappa, torch.zeros_like(e_kappa))
    assert not torch.isnan(e_kappa).any()


@pytest.mark.parametrize("normalize_basis_vectors", [False, True])
def test_spherical_basis_zero_vectors_stay_zero(
    normalize_basis_vectors: bool,
) -> None:
    """Zero 3D direction vectors should produce finite zero basis vectors."""
    r_hat = torch.zeros(4, 3, dtype=torch.float32)
    n_hat = torch.tensor([0.0, 0.0, 1.0], dtype=torch.float32).repeat(4, 1)

    e_r, e_theta, e_phi = spherical_basis(
        r_hat,
        n_hat,
        normalize_basis_vectors=normalize_basis_vectors,
    )

    torch.testing.assert_close(e_r, torch.zeros_like(e_r))
    torch.testing.assert_close(e_theta, torch.zeros_like(e_theta))
    torch.testing.assert_close(e_phi, torch.zeros_like(e_phi))
    assert not torch.isnan(e_theta).any()
    assert not torch.isnan(e_phi).any()


def test_basis_functions_validate_last_dimension() -> None:
    """Basis builders should fail fast on incorrect vector dimensions."""
    with pytest.raises(ValueError, match="r_hat"):
        polar_and_dipole_basis(torch.randn(2, 3), torch.randn(2, 2))

    with pytest.raises(ValueError, match="n_hat"):
        spherical_basis(torch.randn(2, 3), torch.randn(2, 4))


def test_vector_project_is_orthogonal_to_normal() -> None:
    """Projected vectors should be orthogonal to the projection normal."""
    v = torch.randn(8, 3, dtype=torch.float32)
    n_hat = torch.randn(8, 3, dtype=torch.float32)
    n_hat = n_hat / torch.linalg.norm(n_hat, dim=-1, keepdim=True)

    v_projected = vector_project(v, n_hat)
    dot = (v_projected * n_hat).sum(dim=-1)

    torch.testing.assert_close(dot, torch.zeros_like(dot), atol=1e-6, rtol=1e-6)
