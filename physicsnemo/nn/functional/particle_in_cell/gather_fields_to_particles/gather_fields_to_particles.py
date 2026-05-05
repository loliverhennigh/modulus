# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from physicsnemo.core.function_spec import FunctionSpec

from ._torch_impl import gather_fields_to_particles_torch
from ._warp_impl import gather_fields_to_particles_warp


class GatherFieldsToParticles(FunctionSpec):
    r"""Gather electric and magnetic fields to particle positions.

    This functional performs a WarpX-style periodic gather from staggered field
    grids to particle locations with configurable particle-shape order and mode:

    - Inputs ``electric_field`` and ``magnetic_field`` have shape
      ``(3, nx, ny, nz)``.
    - Output particle fields have shape ``(num_particles, 3)``.
    - Default component stagger offsets correspond to a standard Yee
      energy-conserving gather.

    Parameters
    ----------
    particle_position : torch.Tensor
        Particle positions with shape ``(num_particles, 3)``.
    electric_field : torch.Tensor
        Electric field grid with shape ``(3, nx, ny, nz)``.
    magnetic_field : torch.Tensor
        Magnetic field grid with shape ``(3, nx, ny, nz)``.
    origin : Sequence[float], optional
        Physical origin of the grid (x0, y0, z0).
    spacing : Sequence[float], optional
        Grid spacing (dx, dy, dz).
    electric_stagger : Sequence[Sequence[float]] | torch.Tensor | None, optional
        Per-component stagger offsets for electric field gather in index units,
        shape ``(3, 3)``.
    magnetic_stagger : Sequence[Sequence[float]] | torch.Tensor | None, optional
        Per-component stagger offsets for magnetic field gather in index units,
        shape ``(3, 3)``.
    periodic : bool, optional
        Currently only ``True`` is supported.
    shape_order : int, optional
        Particle-shape interpolation order. Supported values are ``1`` and ``3``.
    gather_mode : {"momentum-conserving", "energy-conserving"}, optional
        WarpX-compatible gather mode. ``energy-conserving`` lowers the
        interpolation order by one on mode-specific component axes.
    implementation : {"warp", "torch"} or None
        Backend implementation. When ``None``, dispatch selects by backend rank.

    Returns
    -------
    tuple[torch.Tensor, torch.Tensor]
        Tuple ``(electric_particle, magnetic_particle)``, both shaped
        ``(num_particles, 3)``.
    """

    _BENCHMARK_CASES = (
        ("small-o1-momentum", 4096, 32, 1, "momentum-conserving"),
        ("medium-o3-momentum", 32768, 64, 3, "momentum-conserving"),
        ("large-o3-energy", 131072, 96, 3, "energy-conserving"),
    )

    @FunctionSpec.register(name="warp", required_imports=("warp>=0.6.0",), rank=0)
    def warp_forward(
        particle_position: torch.Tensor,
        electric_field: torch.Tensor,
        magnetic_field: torch.Tensor,
        origin: torch.Tensor | Sequence[float] = (0.0, 0.0, 0.0),
        spacing: torch.Tensor | Sequence[float] = (1.0, 1.0, 1.0),
        electric_stagger: torch.Tensor | Sequence[Sequence[float]] | None = None,
        magnetic_stagger: torch.Tensor | Sequence[Sequence[float]] | None = None,
        periodic: bool = True,
        shape_order: int = 1,
        gather_mode: str = "momentum-conserving",
    ) -> tuple[torch.Tensor, torch.Tensor]:
        return gather_fields_to_particles_warp(
            particle_position=particle_position,
            electric_field=electric_field,
            magnetic_field=magnetic_field,
            origin=origin,
            spacing=spacing,
            electric_stagger=electric_stagger,
            magnetic_stagger=magnetic_stagger,
            periodic=periodic,
            shape_order=shape_order,
            gather_mode=gather_mode,
        )

    @FunctionSpec.register(name="torch", rank=1, baseline=True)
    def torch_forward(
        particle_position: torch.Tensor,
        electric_field: torch.Tensor,
        magnetic_field: torch.Tensor,
        origin: torch.Tensor | Sequence[float] = (0.0, 0.0, 0.0),
        spacing: torch.Tensor | Sequence[float] = (1.0, 1.0, 1.0),
        electric_stagger: torch.Tensor | Sequence[Sequence[float]] | None = None,
        magnetic_stagger: torch.Tensor | Sequence[Sequence[float]] | None = None,
        periodic: bool = True,
        shape_order: int = 1,
        gather_mode: str = "momentum-conserving",
    ) -> tuple[torch.Tensor, torch.Tensor]:
        return gather_fields_to_particles_torch(
            particle_position=particle_position,
            electric_field=electric_field,
            magnetic_field=magnetic_field,
            origin=origin,
            spacing=spacing,
            electric_stagger=electric_stagger,
            magnetic_stagger=magnetic_stagger,
            periodic=periodic,
            shape_order=shape_order,
            gather_mode=gather_mode,
        )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)

        origin = (-0.75, 0.20, 1.10)
        spacing = (0.05, 0.07, 0.09)

        for idx, (label, num_particles, grid_n, shape_order, gather_mode) in enumerate(
            cls._BENCHMARK_CASES
        ):
            generator = torch.Generator(device=device)
            generator.manual_seed(8091 + idx)

            electric_field = torch.randn(
                3,
                grid_n,
                grid_n,
                grid_n,
                device=device,
                generator=generator,
                dtype=torch.float32,
            )
            magnetic_field = torch.randn(
                3,
                grid_n,
                grid_n,
                grid_n,
                device=device,
                generator=generator,
                dtype=torch.float32,
            )

            extent = torch.tensor(
                [
                    spacing[0] * float(grid_n - 1),
                    spacing[1] * float(grid_n - 1),
                    spacing[2] * float(grid_n - 1),
                ],
                device=device,
                dtype=torch.float32,
            )
            origin_tensor = torch.tensor(origin, device=device, dtype=torch.float32)
            particle_position = origin_tensor.unsqueeze(0) + torch.rand(
                num_particles,
                3,
                device=device,
                generator=generator,
                dtype=torch.float32,
            ) * extent.unsqueeze(0)

            yield (
                f"{label}-n{num_particles}-g{grid_n}",
                (
                    particle_position,
                    electric_field,
                    magnetic_field,
                    origin,
                    spacing,
                    None,
                    None,
                    True,
                    shape_order,
                    gather_mode,
                ),
                {},
            )

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        for label, args, kwargs in cls.make_inputs_forward(device=device):
            (
                particle_position,
                electric_field,
                magnetic_field,
                origin,
                spacing,
                electric_stagger,
                magnetic_stagger,
                periodic,
                shape_order,
                gather_mode,
            ) = args
            yield (
                f"{label}-bwd",
                (
                    particle_position.detach().requires_grad_(True),
                    electric_field.detach().requires_grad_(True),
                    magnetic_field.detach().requires_grad_(True),
                    origin,
                    spacing,
                    electric_stagger,
                    magnetic_stagger,
                    periodic,
                    shape_order,
                    gather_mode,
                ),
                dict(kwargs),
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
    def make_input_labels_backward(cls, device: torch.device) -> list[str]:
        _ = device
        return [
            f"{label}-n{num_particles}-g{grid_n}-bwd"
            for label, num_particles, grid_n, _, _ in cls._BENCHMARK_CASES
        ]

    @classmethod
    def compare_forward(
        cls,
        output: tuple[torch.Tensor, torch.Tensor],
        reference: tuple[torch.Tensor, torch.Tensor],
    ) -> None:
        output_electric, output_magnetic = output
        reference_electric, reference_magnetic = reference
        torch.testing.assert_close(
            output_electric,
            reference_electric,
            atol=2e-6,
            rtol=1e-5,
        )
        torch.testing.assert_close(
            output_magnetic,
            reference_magnetic,
            atol=2e-6,
            rtol=1e-5,
        )

    @classmethod
    def compare_backward(
        cls,
        output: torch.Tensor,
        reference: torch.Tensor,
    ) -> None:
        torch.testing.assert_close(
            output,
            reference,
            atol=1e-5,
            rtol=5e-5,
        )


gather_fields_to_particles = GatherFieldsToParticles.make_function(
    "gather_fields_to_particles"
)


__all__ = ["GatherFieldsToParticles", "gather_fields_to_particles"]
