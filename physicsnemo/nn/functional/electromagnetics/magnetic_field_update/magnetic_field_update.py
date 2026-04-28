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

from typing import Sequence

import torch

from physicsnemo.core.function_spec import FunctionSpec

from ._torch_impl import magnetic_field_update_torch
from ._warp_impl import magnetic_field_update_warp


class MagneticFieldUpdate(FunctionSpec):
    r"""Update the magnetic field for one 3D Yee FDTD timestep with periodic boundaries.

    This operator updates the magnetic field :math:`\mathbf{H}^{n+1}` from
    :math:`\mathbf{H}^n`, :math:`\mathbf{E}^n`, and magnetic material parameters.

    The component-wise update is:

    .. math::

       \mathbf{H}^{n+1} = \mathbf{c}_{hh} \odot \mathbf{H}^{n}
       + \mathbf{c}_{he} \odot (\nabla \times \mathbf{E}^{n})

    with Yee-cell effective magnetic coefficients:

    .. math::

       \mathbf{c}_{hh} =
       \frac{2\boldsymbol{\mu} - \boldsymbol{\sigma}_m \Delta t}
       {2\boldsymbol{\mu} + \boldsymbol{\sigma}_m \Delta t}

    .. math::

       \mathbf{c}_{he} =
       \frac{2\Delta t}
       {\Delta\mathbf{x} \odot (2\boldsymbol{\mu} + \boldsymbol{\sigma}_m \Delta t)}

    Parameters
    ----------
    electric_field : torch.Tensor
        Electric field with shape ``(3, nx, ny, nz)`` and dtype ``torch.float32``.
    magnetic_field : torch.Tensor
        Magnetic field with shape ``(3, nx, ny, nz)`` and dtype ``torch.float32``.
    mu : float | torch.Tensor
        Magnetic permeability field. May be a scalar ``float``, a tensor of shape
        ``(nx, ny, nz)``, or a tensor of shape ``(1, nx, ny, nz)``.
    sigma_m : float | torch.Tensor
        Magnetic conductivity field with the same shape rules as ``mu``.
    spacing : torch.Tensor | Sequence[float]
        Cell spacing ``(dx, dy, dz)``.
    dt : float
        Time step.
    inplace : bool, optional
        If ``True``, update ``magnetic_field`` in-place and return it.
    implementation : {"warp", "torch"} or None
        Implementation to use. When ``None``, dispatch selects by rank.

    Returns
    -------
    torch.Tensor
        Updated magnetic field with shape ``(3, nx, ny, nz)``.
    """

    _BENCHMARK_CASES = (
        ("ss-64^3", 64, "scalar-scalar"),
        ("sf-64^3", 64, "scalar-field"),
        ("fs-64^3", 64, "field-scalar"),
        ("ff-64^3", 64, "field-field"),
    )

    @FunctionSpec.register(name="warp", required_imports=("warp>=0.6.0",), rank=0)
    def warp_forward(
        electric_field: torch.Tensor,
        magnetic_field: torch.Tensor,
        mu: float | torch.Tensor,
        sigma_m: float | torch.Tensor,
        spacing: torch.Tensor | Sequence[float],
        dt: float,
        inplace: bool = False,
    ) -> torch.Tensor:
        return magnetic_field_update_warp(
            electric_field,
            magnetic_field,
            mu,
            sigma_m,
            spacing,
            dt,
            inplace=inplace,
        )

    @FunctionSpec.register(name="torch", rank=1, baseline=True)
    def torch_forward(
        electric_field: torch.Tensor,
        magnetic_field: torch.Tensor,
        mu: float | torch.Tensor,
        sigma_m: float | torch.Tensor,
        spacing: torch.Tensor | Sequence[float],
        dt: float,
        inplace: bool = False,
    ) -> torch.Tensor:
        return magnetic_field_update_torch(
            electric_field,
            magnetic_field,
            mu,
            sigma_m,
            spacing,
            dt,
            inplace=inplace,
        )

    @classmethod
    def _iter_benchmark_cases(
        cls,
        device: torch.device | str = "cpu",
        *,
        include_backward_grads: bool = False,
    ):
        device = torch.device(device)

        for seed, (label, grid_n, material_mode) in enumerate(cls._BENCHMARK_CASES):
            generator = torch.Generator(device=device)
            generator.manual_seed(3031 + seed)

            electric_field = torch.randn(
                3,
                grid_n,
                grid_n,
                grid_n,
                generator=generator,
                device=device,
                dtype=torch.float32,
            )
            magnetic_field = torch.randn(
                3,
                grid_n,
                grid_n,
                grid_n,
                generator=generator,
                device=device,
                dtype=torch.float32,
            )
            if include_backward_grads:
                electric_field.requires_grad_(True)
                magnetic_field.requires_grad_(True)

            mu_field = torch.empty(
                grid_n,
                grid_n,
                grid_n,
                device=device,
                dtype=torch.float32,
            ).uniform_(0.5, 3.0, generator=generator)
            sigma_field = torch.empty(
                grid_n,
                grid_n,
                grid_n,
                device=device,
                dtype=torch.float32,
            ).uniform_(0.0, 0.03, generator=generator)

            if material_mode == "scalar-scalar":
                mu = 1.5
                sigma_m = 0.01
            elif material_mode == "scalar-field":
                mu = 1.5
                sigma_m = sigma_field
                if include_backward_grads:
                    sigma_m.requires_grad_(True)
            elif material_mode == "field-scalar":
                mu = mu_field
                sigma_m = 0.01
                if include_backward_grads:
                    mu.requires_grad_(True)
            else:
                mu = mu_field
                sigma_m = sigma_field
                if include_backward_grads:
                    mu.requires_grad_(True)
                    sigma_m.requires_grad_(True)

            spacing = torch.tensor(
                [0.010, 0.011, 0.013],
                device=device,
                dtype=torch.float32,
            )
            dt = 0.00125

            yield (
                label,
                (
                    electric_field,
                    magnetic_field,
                    mu,
                    sigma_m,
                    spacing,
                    dt,
                ),
                {"inplace": False},
            )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        yield from cls._iter_benchmark_cases(
            device=device,
            include_backward_grads=False,
        )

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        yield from cls._iter_benchmark_cases(device=device, include_backward_grads=True)

    @classmethod
    def make_inputs(cls, device: torch.device | str = "cpu"):
        yield from cls.make_inputs_forward(device=device)

    @classmethod
    def make_input_labels_forward(cls, device: torch.device) -> list[str]:
        _ = device
        return [label for label, _, _ in cls._BENCHMARK_CASES]

    @classmethod
    def make_input_labels_backward(cls, device: torch.device) -> list[str]:
        _ = device
        return [label for label, _, _ in cls._BENCHMARK_CASES]

    @classmethod
    def compare_forward(
        cls,
        output: torch.Tensor,
        reference: torch.Tensor,
    ) -> None:
        torch.testing.assert_close(output, reference, atol=5e-5, rtol=1e-4)

    @classmethod
    def compare_backward(
        cls,
        output: torch.Tensor,
        reference: torch.Tensor,
    ) -> None:
        torch.testing.assert_close(output, reference, atol=8e-5, rtol=1e-4)


magnetic_field_update = MagneticFieldUpdate.make_function("magnetic_field_update")


__all__ = ["MagneticFieldUpdate", "magnetic_field_update"]
