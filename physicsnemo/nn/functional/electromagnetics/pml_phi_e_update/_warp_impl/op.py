# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from ..._pml_common import _normalize_offset
from ..utils import _validate_inputs
from .launch_backward import _launch_warp_backward
from .launch_forward import _launch_warp_forward


@torch.library.custom_op("physicsnemo::pml_phi_e_update_warp", mutates_args=())
def pml_phi_e_update_impl(
    magnetic_field: torch.Tensor,
    pml_layer: torch.Tensor,
    offset_x: int,
    offset_y: int,
    offset_z: int,
) -> torch.Tensor:
    output = pml_layer.clone()

    _launch_warp_forward(
        magnetic_field=magnetic_field,
        pml_layer_in=pml_layer,
        pml_layer_out=output,
        pml_layer_offset=(int(offset_x), int(offset_y), int(offset_z)),
    )
    return output


@pml_phi_e_update_impl.register_fake
def _(
    magnetic_field: torch.Tensor,
    pml_layer: torch.Tensor,
    offset_x: int,
    offset_y: int,
    offset_z: int,
) -> torch.Tensor:
    _ = (magnetic_field, offset_x, offset_y, offset_z)
    return torch.empty_like(pml_layer)


def setup_pml_phi_e_update_context(
    ctx: torch.autograd.function.FunctionCtx,
    inputs: tuple,
    output: torch.Tensor,
) -> None:
    magnetic_field, pml_layer, offset_x, offset_y, offset_z = inputs
    _ = output

    ctx.save_for_backward(
        magnetic_field,
        pml_layer,
    )
    ctx.pml_layer_offset = (int(offset_x), int(offset_y), int(offset_z))


def backward_pml_phi_e_update(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[
    torch.Tensor | None,
    torch.Tensor | None,
    None,
    None,
    None,
]:
    if grad_output is None:
        return (None, None, None, None, None)

    magnetic_field, pml_layer = ctx.saved_tensors

    grad_magnetic, grad_pml = _launch_warp_backward(
        magnetic_field=magnetic_field,
        pml_layer=pml_layer,
        grad_output=grad_output,
        pml_layer_offset=ctx.pml_layer_offset,
        needs_input_grad=ctx.needs_input_grad,
    )

    return (
        grad_magnetic,
        grad_pml,
        None,
        None,
        None,
    )


pml_phi_e_update_impl.register_autograd(
    backward_pml_phi_e_update,
    setup_context=setup_pml_phi_e_update_context,
)


def pml_phi_e_update_warp(
    magnetic_field: torch.Tensor,
    pml_layer: torch.Tensor,
    pml_layer_offset: torch.Tensor | Sequence[int] = (0, 0, 0),
    inplace: bool = False,
) -> torch.Tensor:
    _validate_inputs(magnetic_field, pml_layer, pml_layer_offset, inplace)

    if not magnetic_field.is_contiguous():
        raise ValueError(
            "magnetic_field must be contiguous for the warp implementation"
        )
    if not pml_layer.is_contiguous():
        raise ValueError("pml_layer must be contiguous for the warp implementation")

    offset = _normalize_offset(pml_layer_offset)

    if inplace:
        _launch_warp_forward(
            magnetic_field=magnetic_field,
            pml_layer_in=pml_layer,
            pml_layer_out=pml_layer,
            pml_layer_offset=offset,
        )
        return pml_layer

    return pml_phi_e_update_impl(
        magnetic_field,
        pml_layer,
        int(offset[0]),
        int(offset[1]),
        int(offset[2]),
    )
