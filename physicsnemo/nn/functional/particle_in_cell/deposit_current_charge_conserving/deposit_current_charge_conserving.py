# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from physicsnemo.core.function_spec import FunctionSpec

from ._torch_impl import deposit_current_charge_conserving_torch
from ._warp_impl import deposit_current_charge_conserving_warp


class DepositCurrentChargeConserving(FunctionSpec):
    r"""Deposit charge-conserving current density onto a staggered Yee grid.

    This functional applies segmented charge-conserving (Villasenor-like)
    deposition from particle trajectories ``(x_old -> x_new)`` into current
    components ``(Jx, Jy, Jz)`` with configurable shape order ``1`` or ``3``.

    Parameters
    ----------
    particle_position_old : torch.Tensor
        Particle positions at the start of the step, shape ``(N, 3)``.
    particle_position_new : torch.Tensor
        Particle positions at the end of the step, shape ``(N, 3)``.
    particle_weight : torch.Tensor
        Per-particle macro weights, shape ``(N,)``.
    particle_charge : float
        Physical species charge.
    dt : float
        Step size used to infer particle velocity from displacement.
    grid_shape : Sequence[int] | torch.Tensor
        Current-grid shape ``(nx, ny, nz)``.
    origin : Sequence[float] | torch.Tensor, optional
        Grid origin.
    spacing : Sequence[float] | torch.Tensor, optional
        Grid spacing.
    current_stagger : Sequence[Sequence[float]] | torch.Tensor | None, optional
        Per-component stagger offsets in index-space with shape ``(3, 3)``.
        Defaults to WarpX-style Yee current staggering.
    periodic : bool, optional
        Currently only ``True`` is supported.
    shape_order : int, optional
        Particle-shape deposition order. Supported values are ``1`` and ``3``.
    current_density : torch.Tensor | None, optional
        Optional existing current tensor ``(3, nx, ny, nz)`` to accumulate into.
        When provided, deposition is accumulated in-place and the same tensor is returned.
    implementation : {"warp", "torch"} or None
        Backend implementation. When ``None``, dispatch selects by backend rank.

    Returns
    -------
    torch.Tensor
        Current density tensor with shape ``(3, nx, ny, nz)``.
    """

    _BENCHMARK_CASES = (
        ("small-o1", 16384, 32, True, 1),
        ("medium-o3", 65536, 64, True, 3),
        ("large-o3", 196608, 96, True, 3),
    )

    @FunctionSpec.register(name="warp", required_imports=("warp>=0.6.0",), rank=0)
    def warp_forward(
        particle_position_old: torch.Tensor,
        particle_position_new: torch.Tensor,
        particle_weight: torch.Tensor,
        particle_charge: float,
        dt: float,
        grid_shape: Sequence[int] | torch.Tensor,
        origin: torch.Tensor | Sequence[float] = (0.0, 0.0, 0.0),
        spacing: torch.Tensor | Sequence[float] = (1.0, 1.0, 1.0),
        current_stagger: torch.Tensor | Sequence[Sequence[float]] | None = None,
        periodic: bool = True,
        shape_order: int = 1,
        current_density: torch.Tensor | None = None,
    ) -> torch.Tensor:
        return deposit_current_charge_conserving_warp(
            particle_position_old=particle_position_old,
            particle_position_new=particle_position_new,
            particle_weight=particle_weight,
            particle_charge=particle_charge,
            dt=dt,
            grid_shape=grid_shape,
            origin=origin,
            spacing=spacing,
            current_stagger=current_stagger,
            periodic=periodic,
            shape_order=shape_order,
            current_density=current_density,
        )

    @FunctionSpec.register(name="torch", rank=1, baseline=True)
    def torch_forward(
        particle_position_old: torch.Tensor,
        particle_position_new: torch.Tensor,
        particle_weight: torch.Tensor,
        particle_charge: float,
        dt: float,
        grid_shape: Sequence[int] | torch.Tensor,
        origin: torch.Tensor | Sequence[float] = (0.0, 0.0, 0.0),
        spacing: torch.Tensor | Sequence[float] = (1.0, 1.0, 1.0),
        current_stagger: torch.Tensor | Sequence[Sequence[float]] | None = None,
        periodic: bool = True,
        shape_order: int = 1,
        current_density: torch.Tensor | None = None,
    ) -> torch.Tensor:
        return deposit_current_charge_conserving_torch(
            particle_position_old=particle_position_old,
            particle_position_new=particle_position_new,
            particle_weight=particle_weight,
            particle_charge=particle_charge,
            dt=dt,
            grid_shape=grid_shape,
            origin=origin,
            spacing=spacing,
            current_stagger=current_stagger,
            periodic=periodic,
            shape_order=shape_order,
            current_density=current_density,
        )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)

        origin = (-0.30, 0.25, 0.40)
        spacing = (0.08, 0.06, 0.07)
        particle_charge = -1.0
        dt = 1.0e-2

        for idx, (label, num_particles, grid_n, periodic, shape_order) in enumerate(
            cls._BENCHMARK_CASES
        ):
            generator = torch.Generator(device=device)
            generator.manual_seed(8211 + idx)

            origin_tensor = torch.tensor(origin, device=device, dtype=torch.float32)
            spacing_tensor = torch.tensor(spacing, device=device, dtype=torch.float32)
            extent = spacing_tensor * float(grid_n - 1)

            particle_position_old = origin_tensor.unsqueeze(0) + torch.rand(
                num_particles,
                3,
                generator=generator,
                device=device,
                dtype=torch.float32,
            ) * extent.unsqueeze(0)

            displacement = (torch.rand(
                num_particles,
                3,
                generator=generator,
                device=device,
                dtype=torch.float32,
            ) - 0.5) * (0.6 * spacing_tensor).unsqueeze(0)
            particle_position_new = particle_position_old + displacement

            if periodic:
                particle_position_new_grid = (
                    particle_position_new - origin_tensor.unsqueeze(0)
                ) / spacing_tensor.unsqueeze(0)
                particle_position_new_grid = torch.remainder(
                    particle_position_new_grid, float(grid_n)
                )
                particle_position_new = origin_tensor.unsqueeze(0) + (
                    particle_position_new_grid * spacing_tensor.unsqueeze(0)
                )

            particle_weight = torch.rand(
                num_particles,
                generator=generator,
                device=device,
                dtype=torch.float32,
            ).mul_(0.9).add_(0.1)

            yield (
                f"{label}-n{num_particles}-g{grid_n}",
                (
                    particle_position_old,
                    particle_position_new,
                    particle_weight,
                    particle_charge,
                    dt,
                    (grid_n, grid_n, grid_n),
                    origin,
                    spacing,
                    None,
                    periodic,
                    shape_order,
                ),
                {"current_density": None},
            )

    @classmethod
    def make_inputs(cls, device: torch.device | str = "cpu"):
        yield from cls.make_inputs_forward(device=device)

    @classmethod
    def make_input_labels_forward(cls, device: torch.device) -> list[str]:
        _ = device
        return [
            f"{label}-n{num_particles}-g{grid_n}"
            for label, num_particles, grid_n, _, _ in cls._BENCHMARK_CASES
        ]

    @classmethod
    def compare_forward(
        cls,
        output: torch.Tensor,
        reference: torch.Tensor,
    ) -> None:
        torch.testing.assert_close(
            output,
            reference,
            atol=2e-2,
            rtol=5e-4,
        )


deposit_current_charge_conserving = DepositCurrentChargeConserving.make_function(
    "deposit_current_charge_conserving"
)


__all__ = [
    "DepositCurrentChargeConserving",
    "deposit_current_charge_conserving",
]
