# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch

from physicsnemo.core.function_spec import FunctionSpec

from ._torch_impl import particle_push_boris_torch
from ._warp_impl import particle_push_boris_warp


class ParticlePushBoris(FunctionSpec):
    r"""Advance particle states by one relativistic momentum-form Boris push.

    This operator updates particle momentum and position from sampled electric and
    magnetic fields:

    .. math::

       \mathbf{u}^{n+\frac{1}{2}-} = \mathbf{u}^{n} + \frac{q}{m}\frac{\Delta t}{2}\mathbf{E}

    .. math::

       \gamma^{-} = \sqrt{1 + \frac{\lVert\mathbf{u}^{n+\frac{1}{2}-}\rVert^2}{c^2}}

    .. math::

       \mathbf{t} = \frac{1}{\gamma^-}\frac{q}{m}\frac{\Delta t}{2}\mathbf{B},\quad
       \mathbf{s} = \frac{2\mathbf{t}}{1+\lVert\mathbf{t}\rVert^2}

    .. math::

       \mathbf{u}^{n+\frac{1}{2}+} =
       \mathbf{u}^{n+\frac{1}{2}-} +
       (\mathbf{u}^{n+\frac{1}{2}-} + \mathbf{u}^{n+\frac{1}{2}-}\times\mathbf{t}) \times \mathbf{s}

    .. math::

       \mathbf{u}^{n+1} = \mathbf{u}^{n+\frac{1}{2}+} + \frac{q}{m}\frac{\Delta t}{2}\mathbf{E}

    .. math::

       \gamma^{n+1} = \sqrt{1 + \frac{\lVert\mathbf{u}^{n+1}\rVert^2}{c^2}},\quad
       \mathbf{v}^{n+1} = \mathbf{u}^{n+1}/\gamma^{n+1}

    .. math::

       \mathbf{x}^{n+1} = \mathbf{x}^{n} + \Delta t\,\mathbf{v}^{n+1}

    Parameters
    ----------
    particle_position : torch.Tensor
        Particle positions with shape ``(num_particles, 3)``.
    particle_momentum : torch.Tensor
        Particle momentum-like state :math:`\mathbf{u} = \gamma\mathbf{v}` with
        shape ``(num_particles, 3)``.
    electric_field : torch.Tensor
        Electric field sampled at particle locations with shape
        ``(num_particles, 3)``.
    magnetic_field : torch.Tensor
        Magnetic field sampled at particle locations with shape
        ``(num_particles, 3)``.
    charge_to_mass : float
        Species charge-to-mass ratio :math:`q/m`.
    dt : float
        Integration timestep.
    inplace : bool, optional
        If ``True``, update ``particle_position`` and ``particle_momentum`` in
        place and return them.
    implementation : {"warp", "torch"} or None
        Backend implementation. When ``None``, dispatch selects by backend rank.

    Returns
    -------
    tuple[torch.Tensor, torch.Tensor]
        Updated ``(particle_position, particle_momentum)``.
    """

    _BENCHMARK_CASES = (
        ("small", 4096, -1.0, 1.0e-3),
        ("medium", 65536, -1.0, 5.0e-4),
        ("large", 262144, -1.0, 2.0e-4),
    )

    @FunctionSpec.register(name="warp", required_imports=("warp>=0.6.0",), rank=0)
    def warp_forward(
        particle_position: torch.Tensor,
        particle_momentum: torch.Tensor,
        electric_field: torch.Tensor,
        magnetic_field: torch.Tensor,
        charge_to_mass: float,
        dt: float,
        inplace: bool = False,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        return particle_push_boris_warp(
            particle_position=particle_position,
            particle_momentum=particle_momentum,
            electric_field=electric_field,
            magnetic_field=magnetic_field,
            charge_to_mass=charge_to_mass,
            dt=dt,
            inplace=inplace,
        )

    @FunctionSpec.register(name="torch", rank=1, baseline=True)
    def torch_forward(
        particle_position: torch.Tensor,
        particle_momentum: torch.Tensor,
        electric_field: torch.Tensor,
        magnetic_field: torch.Tensor,
        charge_to_mass: float,
        dt: float,
        inplace: bool = False,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        return particle_push_boris_torch(
            particle_position=particle_position,
            particle_momentum=particle_momentum,
            electric_field=electric_field,
            magnetic_field=magnetic_field,
            charge_to_mass=charge_to_mass,
            dt=dt,
            inplace=inplace,
        )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)

        for idx, (label, num_particles, charge_to_mass, dt) in enumerate(
            cls._BENCHMARK_CASES
        ):
            generator = torch.Generator(device=device)
            generator.manual_seed(8011 + idx)

            particle_position = torch.randn(
                num_particles,
                3,
                device=device,
                generator=generator,
                dtype=torch.float32,
            )
            particle_momentum = torch.randn(
                num_particles,
                3,
                device=device,
                generator=generator,
                dtype=torch.float32,
            )
            electric_field = torch.randn(
                num_particles,
                3,
                device=device,
                generator=generator,
                dtype=torch.float32,
            )
            magnetic_field = torch.randn(
                num_particles,
                3,
                device=device,
                generator=generator,
                dtype=torch.float32,
            )

            yield (
                label,
                (
                    particle_position,
                    particle_momentum,
                    electric_field,
                    magnetic_field,
                    charge_to_mass,
                    dt,
                ),
                {"inplace": False},
            )

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        for label, args, kwargs in cls.make_inputs_forward(device=device):
            (
                particle_position,
                particle_momentum,
                electric_field,
                magnetic_field,
                charge_to_mass,
                dt,
            ) = args
            yield (
                f"{label}-bwd",
                (
                    particle_position.detach().requires_grad_(True),
                    particle_momentum.detach().requires_grad_(True),
                    electric_field.detach().requires_grad_(True),
                    magnetic_field.detach().requires_grad_(True),
                    charge_to_mass,
                    dt,
                ),
                dict(kwargs),
            )

    @classmethod
    def make_inputs(cls, device: torch.device | str = "cpu"):
        yield from cls.make_inputs_forward(device=device)

    @classmethod
    def make_input_labels_forward(cls, device: torch.device) -> list[str]:
        _ = device
        return [label for label, _, _, _ in cls._BENCHMARK_CASES]

    @classmethod
    def compare_forward(
        cls,
        output: tuple[torch.Tensor, torch.Tensor],
        reference: tuple[torch.Tensor, torch.Tensor],
    ) -> None:
        output_position, output_momentum = output
        reference_position, reference_momentum = reference
        torch.testing.assert_close(
            output_position,
            reference_position,
            atol=2e-6,
            rtol=1e-5,
        )
        torch.testing.assert_close(
            output_momentum,
            reference_momentum,
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


particle_push_boris = ParticlePushBoris.make_function("particle_push_boris")


__all__ = ["ParticlePushBoris", "particle_push_boris"]
