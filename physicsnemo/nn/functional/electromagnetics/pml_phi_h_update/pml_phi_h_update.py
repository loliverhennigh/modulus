# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch

from physicsnemo.core.function_spec import FunctionSpec

from ._torch_impl import pml_phi_h_update_torch
from ._warp_impl import pml_phi_h_update_warp


class PMLPhiHUpdate(FunctionSpec):
    """Update PML magnetic auxiliary fields from electric-field stencils."""

    _CASES = (
        ("x-left", (6, 12, 10), (0, 0, 0)),
        ("x-right", (6, 12, 10), (10, 0, 0)),
        ("y-left", (16, 6, 10), (0, 0, 0)),
        ("z-right", (16, 12, 6), (0, 0, 10)),
    )

    @FunctionSpec.register(name="warp", required_imports=("warp>=0.6.0",), rank=0)
    def warp_forward(
        electric_field: torch.Tensor,
        pml_layer: torch.Tensor,
        pml_layer_offset: torch.Tensor | tuple[int, int, int] = (0, 0, 0),
        inplace: bool = False,
    ) -> torch.Tensor:
        return pml_phi_h_update_warp(
            electric_field,
            pml_layer,
            pml_layer_offset=pml_layer_offset,
            inplace=inplace,
        )

    @FunctionSpec.register(name="torch", rank=1, baseline=True)
    def torch_forward(
        electric_field: torch.Tensor,
        pml_layer: torch.Tensor,
        pml_layer_offset: torch.Tensor | tuple[int, int, int] = (0, 0, 0),
        inplace: bool = False,
    ) -> torch.Tensor:
        return pml_phi_h_update_torch(
            electric_field,
            pml_layer,
            pml_layer_offset=pml_layer_offset,
            inplace=inplace,
        )

    @classmethod
    def _iter_benchmark_cases(
        cls,
        device: torch.device | str = "cpu",
        include_backward_grads: bool = False,
    ):
        device = torch.device(device)

        nx = ny = nz = 16
        for idx, (label, pml_shape, offset) in enumerate(cls._CASES):
            generator = torch.Generator(device=device)
            generator.manual_seed(10011 + idx)

            electric_field = torch.randn(
                3,
                nx,
                ny,
                nz,
                generator=generator,
                device=device,
                dtype=torch.float32,
            )
            pml_layer = torch.zeros(
                36,
                pml_shape[0],
                pml_shape[1],
                pml_shape[2],
                device=device,
                dtype=torch.float32,
            )
            pml_layer[15:24] = torch.randn(
                9,
                pml_shape[0],
                pml_shape[1],
                pml_shape[2],
                generator=generator,
                device=device,
                dtype=torch.float32,
            )
            pml_layer[30:36] = torch.empty(
                6,
                pml_shape[0],
                pml_shape[1],
                pml_shape[2],
                device=device,
                dtype=torch.float32,
            ).uniform_(0.05, 0.95, generator=generator)

            if include_backward_grads:
                electric_field = electric_field.detach().requires_grad_(True)
                pml_layer = pml_layer.detach().requires_grad_(True)

            yield (
                label,
                (electric_field, pml_layer),
                {
                    "pml_layer_offset": offset,
                    "inplace": False,
                },
            )

    @classmethod
    def make_inputs_forward(cls, device: torch.device | str = "cpu"):
        yield from cls._iter_benchmark_cases(
            device=device,
            include_backward_grads=False,
        )

    @classmethod
    def make_inputs_backward(cls, device: torch.device | str = "cpu"):
        yield from cls._iter_benchmark_cases(
            device=device,
            include_backward_grads=True,
        )

    @classmethod
    def make_inputs(cls, device: torch.device | str = "cpu"):
        yield from cls.make_inputs_forward(device=device)

    @classmethod
    def make_input_labels_forward(cls, device: torch.device) -> list[str]:
        _ = device
        return [label for label, _, _ in cls._CASES]

    @classmethod
    def make_input_labels_backward(cls, device: torch.device) -> list[str]:
        _ = device
        return [label for label, _, _ in cls._CASES]

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


pml_phi_h_update = PMLPhiHUpdate.make_function("pml_phi_h_update")


__all__ = ["PMLPhiHUpdate", "pml_phi_h_update"]
