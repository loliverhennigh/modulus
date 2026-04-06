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
from functools import lru_cache

import torch

from physicsnemo.core.function_spec import FunctionSpec

from ._torch_impl import uniform_grid_gradient_torch
from ._warp_impl import uniform_grid_gradient_warp

### Auto-dispatch crossover thresholds for 3D CUDA fields.
### <= TORCH_MAX uses eager torch; <= TORCH_COMPILED_MAX uses torch-compiled.
### Larger fields use warp.
_AUTO_3D_TORCH_MAX_NUMEL = 48 * 48 * 48
_AUTO_3D_TORCH_COMPILED_MAX_NUMEL = 64 * 64 * 64


class UniformGridGradient(FunctionSpec):
    r"""Compute periodic central-difference gradients on a uniform grid.

    This functional computes first-order spatial derivatives of a scalar field
    defined on a 1D/2D/3D uniform Cartesian grid using second-order central
    differences with periodic indexing.

    For each axis :math:`k`, the derivative is:

    .. math::

       \partial_k f(\mathbf{i}) \approx
       \frac{f(\mathbf{i}+\hat{e}_k) - f(\mathbf{i}-\hat{e}_k)}{2\,\Delta x_k}

    with periodic wrap-around at boundaries.

    Parameters
    ----------
    field : torch.Tensor
        Scalar grid field with shape ``(n0,)``, ``(n0,n1)``, or ``(n0,n1,n2)``.
    spacing : float | Sequence[float], optional
        Uniform spacing per axis. Use a scalar for isotropic spacing or a
        sequence matching field dimensionality.
    order : int, optional
        Central-difference accuracy order. Supported values are ``2`` and ``4``.
    implementation : {"warp", "torch_compiled", "torch"} or None
        Explicit backend selection. When ``None``, ``uniform_grid_gradient``
        applies a shape-aware auto-dispatch heuristic.

    Returns
    -------
    torch.Tensor
        Gradient tensor of shape ``(dims, *field.shape)``.
    """

    ### Benchmark input presets (small -> large workload).
    _BENCHMARK_CASES = (
        ("1d-n8192-o2", (8192,), 0.01, 2),
        ("1d-n8192-o4", (8192,), 0.01, 4),
        ("2d-512x512-o2", (512, 512), (0.01, 0.02), 2),
        ("3d-128x128x128-o2", (128, 128, 128), 0.02, 2),
    )

    _COMPARE_ATOL = 1e-5
    _COMPARE_RTOL = 1e-5
    _COMPARE_BACKWARD_ATOL = 1e-5
    _COMPARE_BACKWARD_RTOL = 1e-5

    @FunctionSpec.register(name="warp", required_imports=("warp>=0.6.0",), rank=0)
    def warp_forward(
        field: torch.Tensor,
        spacing: float | Sequence[float] = 1.0,
        order: int = 2,
    ) -> torch.Tensor:
        """Dispatch uniform-grid gradients to the Warp backend."""
        ### Warp backend implementation.
        return uniform_grid_gradient_warp(field=field, spacing=spacing, order=order)

    @FunctionSpec.register(name="torch_compiled", rank=1)
    def torch_compiled_forward(
        field: torch.Tensor,
        spacing: float | Sequence[float] = 1.0,
        order: int = 2,
    ) -> torch.Tensor:
        """Dispatch uniform-grid gradients to torch.compile when available."""
        ### Compiled PyTorch backend implementation with safe fallback.
        if field.device.type != "cuda":
            return uniform_grid_gradient_torch(
                field=field, spacing=spacing, order=order
            )
        return _compiled_uniform_grid_gradient_torch(
            field=field, spacing=spacing, order=order
        )

    @FunctionSpec.register(name="torch", rank=2, baseline=True)
    def torch_forward(
        field: torch.Tensor,
        spacing: float | Sequence[float] = 1.0,
        order: int = 2,
    ) -> torch.Tensor:
        """Dispatch uniform-grid gradients to eager PyTorch."""
        ### PyTorch backend implementation.
        return uniform_grid_gradient_torch(field=field, spacing=spacing, order=order)

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        """Yield representative forward benchmark and parity input cases."""
        device = torch.device(device)

        ### Build periodic analytic fields for benchmark and parity coverage.
        for label, shape, spacing, order in cls._BENCHMARK_CASES:
            if len(shape) == 1:
                x = torch.linspace(0.0, 1.0, shape[0], device=device)
                field = torch.sin(2.0 * torch.pi * x)
            elif len(shape) == 2:
                x0 = torch.linspace(0.0, 1.0, shape[0], device=device)
                x1 = torch.linspace(0.0, 1.0, shape[1], device=device)
                xx, yy = torch.meshgrid(x0, x1, indexing="ij")
                field = torch.sin(2.0 * torch.pi * xx) + 0.5 * torch.cos(
                    2.0 * torch.pi * yy
                )
            else:
                x0 = torch.linspace(0.0, 1.0, shape[0], device=device)
                x1 = torch.linspace(0.0, 1.0, shape[1], device=device)
                x2 = torch.linspace(0.0, 1.0, shape[2], device=device)
                xx, yy, zz = torch.meshgrid(x0, x1, x2, indexing="ij")
                field = (
                    torch.sin(2.0 * torch.pi * xx)
                    + 0.5 * torch.cos(2.0 * torch.pi * yy)
                    + 0.25 * torch.sin(2.0 * torch.pi * zz)
                )

            ### Yield the labeled functional input case.
            yield (
                label,
                (field.to(torch.float32),),
                {"spacing": spacing, "order": order},
            )

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        """Yield representative backward benchmark and parity input cases."""
        device = torch.device(device)

        ### Build representative differentiable fields for backward parity.
        backward_cases = (
            ("1d-grad-n4096-o2", (4096,), 0.01, 2),
            ("2d-grad-256x256-o2", (256, 256), (0.01, 0.02), 2),
            ("2d-grad-256x256-o4", (256, 256), (0.01, 0.02), 4),
            ("3d-grad-96x96x96-o2", (96, 96, 96), 0.02, 2),
        )

        for label, shape, spacing, order in backward_cases:
            if len(shape) == 1:
                x = torch.linspace(0.0, 1.0, shape[0], device=device)
                field = torch.sin(2.0 * torch.pi * x)
            elif len(shape) == 2:
                x0 = torch.linspace(0.0, 1.0, shape[0], device=device)
                x1 = torch.linspace(0.0, 1.0, shape[1], device=device)
                xx, yy = torch.meshgrid(x0, x1, indexing="ij")
                field = torch.sin(2.0 * torch.pi * xx) + 0.5 * torch.cos(
                    2.0 * torch.pi * yy
                )
            else:
                x0 = torch.linspace(0.0, 1.0, shape[0], device=device)
                x1 = torch.linspace(0.0, 1.0, shape[1], device=device)
                x2 = torch.linspace(0.0, 1.0, shape[2], device=device)
                xx, yy, zz = torch.meshgrid(x0, x1, x2, indexing="ij")
                field = (
                    torch.sin(2.0 * torch.pi * xx)
                    + 0.5 * torch.cos(2.0 * torch.pi * yy)
                    + 0.25 * torch.sin(2.0 * torch.pi * zz)
                )

            ### Yield differentiable field inputs for backward dispatch.
            yield (
                label,
                (field.to(torch.float32).detach().clone().requires_grad_(True),),
                {"spacing": spacing, "order": order},
            )

    @classmethod
    def compare_forward(cls, output: torch.Tensor, reference: torch.Tensor) -> None:
        """Compare forward outputs across implementations."""
        ### Validate forward parity across backends.
        torch.testing.assert_close(
            output,
            reference,
            atol=cls._COMPARE_ATOL,
            rtol=cls._COMPARE_RTOL,
        )

    @classmethod
    def compare_backward(cls, output: torch.Tensor, reference: torch.Tensor) -> None:
        """Compare backward gradients across implementations."""
        ### Validate backward parity across backends.
        torch.testing.assert_close(
            output,
            reference,
            atol=cls._COMPARE_BACKWARD_ATOL,
            rtol=cls._COMPARE_BACKWARD_RTOL,
        )


@lru_cache(maxsize=1)
def _get_compiled_uniform_grid_gradient_torch():
    ### Lazily construct and cache the torch-compiled implementation.
    try:
        return torch.compile(
            uniform_grid_gradient_torch,
            mode="max-autotune-no-cudagraphs",
        )
    except Exception:
        return torch.compile(uniform_grid_gradient_torch, mode="default")


def _compiled_uniform_grid_gradient_torch(
    field: torch.Tensor,
    spacing: float | Sequence[float],
    order: int,
) -> torch.Tensor:
    ### Dispatch to torch.compile path while preserving torch fallback behavior.
    try:
        compiled = _get_compiled_uniform_grid_gradient_torch()
    except Exception:
        return uniform_grid_gradient_torch(field=field, spacing=spacing, order=order)
    return compiled(field=field, spacing=spacing, order=order)


def _auto_select_implementation(field: torch.Tensor) -> str:
    ### Select backend by dimensionality/size on CUDA and by capability on CPU.
    available = set(UniformGridGradient.available_implementations())
    if "warp" not in available:
        if "torch_compiled" in available:
            return "torch_compiled"
        return "torch"

    if field.device.type != "cuda":
        return "torch"

    ### Autograd paths should prefer the explicit Warp autograd kernels.
    if field.requires_grad:
        return "warp"

    if "torch_compiled" not in available:
        return "warp"

    ### 1D/2D generally favor eager torch in current measurements.
    if field.ndim in (1, 2):
        if "torch" in available:
            return "torch"
        return "torch_compiled"

    ### 3D uses a two-threshold crossover: torch -> torch_compiled -> warp.
    numel = field.numel()
    if numel <= _AUTO_3D_TORCH_MAX_NUMEL and "torch" in available:
        return "torch"
    if numel <= _AUTO_3D_TORCH_COMPILED_MAX_NUMEL and "torch_compiled" in available:
        return "torch_compiled"
    return "warp"


_uniform_grid_gradient_dispatch = UniformGridGradient.make_function(
    "_uniform_grid_gradient_dispatch"
)


def uniform_grid_gradient(
    field: torch.Tensor,
    spacing: float | Sequence[float] = 1.0,
    order: int = 2,
    implementation: str | None = None,
) -> torch.Tensor:
    """Compute periodic central-difference gradients on a uniform grid.

    When ``implementation`` is ``None``, a shape-aware backend heuristic is
    used: on CUDA, 1D/2D fields prefer ``torch``; 3D fields use a two-threshold
    crossover (``torch`` -> ``torch_compiled`` -> ``warp``) as problem size
    grows. Inputs requiring gradients prefer ``warp`` to use the explicit
    custom backward kernels.
    """
    if implementation is None:
        implementation = _auto_select_implementation(field)
    return _uniform_grid_gradient_dispatch(
        field,
        spacing=spacing,
        order=order,
        implementation=implementation,
    )


__all__ = ["UniformGridGradient", "uniform_grid_gradient"]
