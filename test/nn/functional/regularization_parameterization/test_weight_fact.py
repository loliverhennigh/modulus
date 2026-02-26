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

from physicsnemo.nn.functional import weight_fact


# Validate the torch weight-factorization implementation.
def test_weight_fact_torch(device: str):
    w = torch.randn(32, 16, device=device, dtype=torch.float32)
    g, v = weight_fact(w, mean=1.0, stddev=0.1, implementation="torch")

    assert g.shape == (w.shape[0], 1)
    assert v.shape == w.shape
    assert (g > 0).all()
    torch.testing.assert_close(g * v, w, atol=1e-6, rtol=1e-6)


# Validate backward behavior for weight-factorization outputs.
def test_weight_fact_backward(device: str):
    w = torch.randn(16, 8, device=device, dtype=torch.float32, requires_grad=True)
    g, v = weight_fact(w, mean=1.0, stddev=0.1, implementation="torch")

    # g * v reconstructs w, so the gradient wrt w is expected to be ones.
    (g * v).sum().backward()
    torch.testing.assert_close(w.grad, torch.ones_like(w))
