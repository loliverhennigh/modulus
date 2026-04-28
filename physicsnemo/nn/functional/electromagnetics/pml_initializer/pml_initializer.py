# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch

from physicsnemo.core.function_spec import FunctionSpec

from ._torch_impl import pml_initializer_torch
from ._warp_impl import pml_initializer_warp


class PMLInitializer(FunctionSpec):
    """Initialize PML coefficient channels for one boundary slab."""

    _CASES = (
        ("x-plus", (1.0, 0.0, 0.0), (8, 16, 12), 8),
        ("x-minus", (-1.0, 0.0, 0.0), (8, 16, 12), 8),
        ("y-plus", (0.0, 1.0, 0.0), (16, 6, 12), 6),
        ("z-minus", (0.0, 0.0, -1.0), (16, 12, 6), 6),
    )

    @FunctionSpec.register(name="warp", required_imports=("warp>=0.6.0",), rank=0)
    def warp_forward(
        pml_layer: torch.Tensor,
        direction: torch.Tensor | tuple[float, float, float],
        thickness: int,
        courant_number: float,
        kappa: float = 1.0,
        a: float = 1.0e-8,
        inplace: bool = False,
    ) -> torch.Tensor:
        return pml_initializer_warp(
            pml_layer,
            direction,
            thickness,
            courant_number,
            kappa=kappa,
            a=a,
            inplace=inplace,
        )

    @FunctionSpec.register(name="torch", rank=1, baseline=True)
    def torch_forward(
        pml_layer: torch.Tensor,
        direction: torch.Tensor | tuple[float, float, float],
        thickness: int,
        courant_number: float,
        kappa: float = 1.0,
        a: float = 1.0e-8,
        inplace: bool = False,
    ) -> torch.Tensor:
        return pml_initializer_torch(
            pml_layer,
            direction,
            thickness,
            courant_number,
            kappa=kappa,
            a=a,
            inplace=inplace,
        )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)

        for idx, (label, direction, pml_shape, thickness) in enumerate(cls._CASES):
            generator = torch.Generator(device=device)
            generator.manual_seed(711 + idx)

            pml_layer = torch.zeros(
                36,
                pml_shape[0],
                pml_shape[1],
                pml_shape[2],
                device=device,
                dtype=torch.float32,
            )
            pml_layer[0:24] = torch.randn(
                24,
                pml_shape[0],
                pml_shape[1],
                pml_shape[2],
                generator=generator,
                device=device,
                dtype=torch.float32,
            )

            yield (
                label,
                (pml_layer, direction, thickness, 0.5),
                {
                    "kappa": 1.0 + 0.2 * idx,
                    "a": 1.0e-8,
                    "inplace": False,
                },
            )

    @classmethod
    def make_inputs(cls, device: torch.device | str = "cpu"):
        yield from cls.make_inputs_forward(device=device)

    @classmethod
    def make_input_labels_forward(cls, device: torch.device) -> list[str]:
        _ = device
        return [label for label, _, _, _ in cls._CASES]

    @classmethod
    def compare_forward(
        cls,
        output: torch.Tensor,
        reference: torch.Tensor,
    ) -> None:
        torch.testing.assert_close(output, reference, atol=5e-5, rtol=1e-4)


pml_initializer = PMLInitializer.make_function("pml_initializer")


__all__ = ["PMLInitializer", "pml_initializer"]
