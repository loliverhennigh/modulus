# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from ..utils import _prepare_inputs_warp
from .launch_backward import _launch_warp_backward
from .launch_forward import _launch_warp_forward


@torch.library.custom_op(
    "physicsnemo::pml_electric_field_update_warp",
    mutates_args=(),
)
def pml_electric_field_update_impl(
    electric_field: torch.Tensor,
    pml_layer: torch.Tensor,
    eps_field: torch.Tensor,
    eps_scalar: float,
    eps_is_scalar: bool,
    spacing: torch.Tensor,
    offset_x: int,
    offset_y: int,
    offset_z: int,
    dt: float,
) -> torch.Tensor:
    output = electric_field.clone()

    _launch_warp_forward(
        electric_field=output,
        pml_layer=pml_layer,
        eps_field=eps_field,
        eps_scalar=float(eps_scalar),
        eps_is_scalar=bool(eps_is_scalar),
        spacing=spacing,
        pml_layer_offset=(int(offset_x), int(offset_y), int(offset_z)),
        dt=dt,
    )
    return output


@pml_electric_field_update_impl.register_fake
def _(
    electric_field: torch.Tensor,
    pml_layer: torch.Tensor,
    eps_field: torch.Tensor,
    eps_scalar: float,
    eps_is_scalar: bool,
    spacing: torch.Tensor,
    offset_x: int,
    offset_y: int,
    offset_z: int,
    dt: float,
) -> torch.Tensor:
    _ = (
        pml_layer,
        eps_field,
        eps_scalar,
        eps_is_scalar,
        spacing,
        offset_x,
        offset_y,
        offset_z,
        dt,
    )
    return torch.empty_like(electric_field)


def setup_pml_electric_field_update_context(
    ctx: torch.autograd.function.FunctionCtx,
    inputs: tuple,
    output: torch.Tensor,
) -> None:
    (
        electric_field,
        pml_layer,
        eps_field,
        eps_scalar,
        eps_is_scalar,
        spacing,
        offset_x,
        offset_y,
        offset_z,
        dt,
    ) = inputs
    _ = (electric_field, output)

    ctx.save_for_backward(
        pml_layer,
        eps_field,
        spacing,
    )
    ctx.pml_layer_offset = (int(offset_x), int(offset_y), int(offset_z))
    ctx.eps_scalar = float(eps_scalar)
    ctx.eps_is_scalar = bool(eps_is_scalar)
    ctx.dt = float(dt)


def backward_pml_electric_field_update(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
]:
    if grad_output is None:
        return (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )

    pml_layer, eps_field, spacing = ctx.saved_tensors

    grad_pml, grad_eps = _launch_warp_backward(
        pml_layer=pml_layer,
        eps_field=eps_field,
        eps_scalar=ctx.eps_scalar,
        eps_is_scalar=ctx.eps_is_scalar,
        grad_output=grad_output,
        spacing=spacing,
        pml_layer_offset=ctx.pml_layer_offset,
        dt=ctx.dt,
        needs_input_grad=ctx.needs_input_grad,
    )

    grad_electric = grad_output if ctx.needs_input_grad[0] else None

    return (
        grad_electric,
        grad_pml,
        grad_eps,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )


pml_electric_field_update_impl.register_autograd(
    backward_pml_electric_field_update,
    setup_context=setup_pml_electric_field_update_context,
)


def pml_electric_field_update_warp(
    electric_field: torch.Tensor,
    pml_layer: torch.Tensor,
    eps: float | torch.Tensor,
    spacing: torch.Tensor | Sequence[float],
    pml_layer_offset: torch.Tensor | Sequence[int],
    dt: float,
    inplace: bool = False,
) -> torch.Tensor:
    spacing_tensor, offset, eps_field, eps_scalar, eps_is_scalar = _prepare_inputs_warp(
        electric_field,
        pml_layer,
        eps,
        spacing,
        pml_layer_offset,
        inplace,
    )

    if not electric_field.is_contiguous():
        raise ValueError(
            "electric_field must be contiguous for the warp implementation"
        )
    if not pml_layer.is_contiguous():
        raise ValueError("pml_layer must be contiguous for the warp implementation")
    if eps_field is not None and not eps_field.is_contiguous():
        raise ValueError("eps tensor must be contiguous for the warp implementation")
    if isinstance(spacing, torch.Tensor) and not spacing.is_contiguous():
        raise ValueError(
            "spacing tensor must be contiguous for the warp implementation"
        )

    if inplace:
        _launch_warp_forward(
            electric_field=electric_field,
            pml_layer=pml_layer,
            eps_field=eps_field,
            eps_scalar=eps_scalar,
            eps_is_scalar=eps_is_scalar,
            spacing=spacing_tensor,
            pml_layer_offset=offset,
            dt=dt,
        )
        return electric_field

    eps_field_tensor = (
        eps_field.contiguous()
        if eps_field is not None
        else torch.empty((0, 0, 0), device=electric_field.device, dtype=torch.float32)
    )

    return pml_electric_field_update_impl(
        electric_field,
        pml_layer,
        eps_field_tensor,
        float(eps_scalar),
        bool(eps_is_scalar),
        spacing_tensor.contiguous(),
        int(offset[0]),
        int(offset[1]),
        int(offset[2]),
        float(dt),
    )
