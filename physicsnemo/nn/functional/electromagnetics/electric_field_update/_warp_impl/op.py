# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from ..utils import (
    _as_spacing_tensor,
    _normalize_material_field,
    _normalize_offset,
    _validate_common_inputs,
)
from .launch_backward import _launch_warp_backward
from .launch_forward import _launch_warp_forward


@torch.library.custom_op("physicsnemo::electric_field_update_warp", mutates_args=())
def electric_field_update_impl(
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    eps_field: torch.Tensor,
    sigma_e_field: torch.Tensor,
    eps_scalar: float,
    sigma_e_scalar: float,
    eps_is_scalar: bool,
    sigma_is_scalar: bool,
    spacing: torch.Tensor,
    dt: float,
    impressed_current: torch.Tensor,
    impressed_current_offset: torch.Tensor,
) -> torch.Tensor:
    # Compute an output buffer and normalize metadata to Python scalars.
    output = torch.empty_like(electric_field)
    offset = tuple(
        int(v) for v in impressed_current_offset.detach().cpu().flatten().tolist()
    )
    eps_input: float | torch.Tensor = float(eps_scalar) if eps_is_scalar else eps_field
    sigma_input: float | torch.Tensor = (
        float(sigma_e_scalar) if sigma_is_scalar else sigma_e_field
    )

    # Delegate to the warp forward launcher; this custom-op stays thin by design.
    _launch_warp_forward(
        electric_field=electric_field,
        magnetic_field=magnetic_field,
        eps=eps_input,
        sigma_e=sigma_input,
        spacing=spacing,
        dt=dt,
        impressed_current=impressed_current,
        impressed_current_offset=offset,
        output=output,
    )
    return output


# Provide fake-mode output metadata for torch compile.
@electric_field_update_impl.register_fake
def _(
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    eps_field: torch.Tensor,
    sigma_e_field: torch.Tensor,
    eps_scalar: float,
    sigma_e_scalar: float,
    eps_is_scalar: bool,
    sigma_is_scalar: bool,
    spacing: torch.Tensor,
    dt: float,
    impressed_current: torch.Tensor,
    impressed_current_offset: torch.Tensor,
) -> torch.Tensor:
    _ = (
        magnetic_field,
        eps_field,
        sigma_e_field,
        eps_scalar,
        sigma_e_scalar,
        eps_is_scalar,
        sigma_is_scalar,
        spacing,
        dt,
        impressed_current,
        impressed_current_offset,
    )
    return torch.empty_like(electric_field)


# Save forward context used by autograd backward.
def setup_electric_field_update_context(
    ctx: torch.autograd.function.FunctionCtx,
    inputs: tuple,
    output: torch.Tensor,
) -> None:
    (
        electric_field,
        magnetic_field,
        eps_field,
        sigma_e_field,
        eps_scalar,
        sigma_e_scalar,
        eps_is_scalar,
        sigma_is_scalar,
        spacing,
        dt,
        impressed_current,
        impressed_current_offset,
    ) = inputs
    _ = output

    # Save tensors required to reconstruct local coefficients during backward.
    ctx.save_for_backward(
        electric_field,
        magnetic_field,
        eps_field,
        sigma_e_field,
        spacing,
        impressed_current,
        impressed_current_offset,
    )

    # Persist scalar metadata on the autograd context.
    ctx.eps_scalar = float(eps_scalar)
    ctx.sigma_e_scalar = float(sigma_e_scalar)
    ctx.eps_is_scalar = bool(eps_is_scalar)
    ctx.sigma_is_scalar = bool(sigma_is_scalar)
    ctx.dt = float(dt)


# Warp-native backward for the custom op.
def backward_electric_field_update(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
    None,
    None,
    None,
    None,
    torch.Tensor | None,
    None,
    torch.Tensor | None,
    None,
]:
    # Restore tensors and metadata captured during forward setup.
    (
        electric_field,
        magnetic_field,
        eps_field,
        sigma_e_field,
        spacing,
        impressed_current,
        impressed_current_offset,
    ) = ctx.saved_tensors

    # Torch may call backward with no gradient signal; mirror autograd convention.
    if grad_output is None:
        return (None, None, None, None, None, None, None, None, None, None, None, None)

    # Launch warp-native backward and map gradients to custom-op argument order.
    (
        grad_electric,
        grad_magnetic,
        grad_eps,
        grad_sigma,
        grad_current,
    ) = _launch_warp_backward(
        electric_field=electric_field,
        magnetic_field=magnetic_field,
        eps_field=eps_field,
        sigma_e_field=sigma_e_field,
        impressed_current=impressed_current,
        grad_output=grad_output,
        spacing=spacing,
        dt=ctx.dt,
        eps_scalar=ctx.eps_scalar,
        sigma_e_scalar=ctx.sigma_e_scalar,
        eps_is_scalar=ctx.eps_is_scalar,
        sigma_is_scalar=ctx.sigma_is_scalar,
        impressed_current_offset=impressed_current_offset,
        needs_input_grad=ctx.needs_input_grad,
    )

    return (
        grad_electric,
        grad_magnetic,
        grad_eps,
        grad_sigma,
        None,
        None,
        None,
        None,
        None,
        None,
        grad_current,
        None,
    )


electric_field_update_impl.register_autograd(
    backward_electric_field_update,
    setup_context=setup_electric_field_update_context,
)


# Public warp entry point used by the FunctionSpec.
def electric_field_update_warp(
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    eps: float | torch.Tensor,
    sigma_e: float | torch.Tensor,
    spacing: torch.Tensor | Sequence[float],
    dt: float,
    impressed_current: torch.Tensor | None = None,
    impressed_current_offset: torch.Tensor | Sequence[int] = (0, 0, 0),
    inplace: bool = False,
) -> torch.Tensor:
    # Validate shape/dtype/device contracts shared by torch and warp backends.
    _validate_common_inputs(
        electric_field,
        magnetic_field,
        eps,
        sigma_e,
        spacing,
        impressed_current,
        inplace,
    )

    # Enforce contiguous tensors because warp kernels assume linear memory layout.
    if not electric_field.is_contiguous():
        raise ValueError(
            "electric_field must be contiguous for the warp implementation"
        )
    if not magnetic_field.is_contiguous():
        raise ValueError(
            "magnetic_field must be contiguous for the warp implementation"
        )
    if isinstance(eps, torch.Tensor) and not eps.is_contiguous():
        raise ValueError("eps tensor must be contiguous for the warp implementation")
    if isinstance(sigma_e, torch.Tensor) and not sigma_e.is_contiguous():
        raise ValueError(
            "sigma_e tensor must be contiguous for the warp implementation"
        )
    if isinstance(spacing, torch.Tensor) and not spacing.is_contiguous():
        raise ValueError(
            "spacing tensor must be contiguous for the warp implementation"
        )
    if impressed_current is not None and not impressed_current.is_contiguous():
        raise ValueError(
            "impressed_current must be contiguous for the warp implementation"
        )

    # Normalize metadata to canonical tensor/tuple forms before dispatch.
    spacing_tensor = _as_spacing_tensor(
        spacing,
        device=electric_field.device,
        dtype=electric_field.dtype,
    ).contiguous()
    offset = _normalize_offset(impressed_current_offset)

    if impressed_current is None:
        impressed_current_tensor = torch.empty(
            (3, 0, 0, 0),
            device=electric_field.device,
            dtype=torch.float32,
        )
    else:
        impressed_current_tensor = impressed_current

    # In-place path updates electric_field directly and returns the same tensor.
    if inplace:
        _launch_warp_forward(
            electric_field=electric_field,
            magnetic_field=magnetic_field,
            eps=eps,
            sigma_e=sigma_e,
            spacing=spacing_tensor,
            dt=dt,
            impressed_current=impressed_current_tensor,
            impressed_current_offset=offset,
            output=electric_field,
        )
        return electric_field

    # Out-of-place path packs scalar/material metadata for the custom op.
    spatial_shape = tuple(electric_field.shape[1:])
    empty_material = torch.empty(
        (0, 0, 0),
        device=electric_field.device,
        dtype=torch.float32,
    )
    eps_is_scalar = isinstance(eps, (int, float))
    sigma_is_scalar = isinstance(sigma_e, (int, float))
    eps_field = (
        empty_material
        if eps_is_scalar
        else _normalize_material_field(
            eps, "eps", spatial_shape, electric_field.device
        ).contiguous()
    )
    sigma_field = (
        empty_material
        if sigma_is_scalar
        else _normalize_material_field(
            sigma_e, "sigma_e", spatial_shape, electric_field.device
        ).contiguous()
    )
    offset_tensor = torch.tensor(
        offset, device=electric_field.device, dtype=torch.int32
    )

    # Dispatch through the registered torch custom op wrapper.
    return electric_field_update_impl(
        electric_field,
        magnetic_field,
        eps_field,
        sigma_field,
        float(eps) if eps_is_scalar else 0.0,
        float(sigma_e) if sigma_is_scalar else 0.0,
        eps_is_scalar,
        sigma_is_scalar,
        spacing_tensor,
        float(dt),
        impressed_current_tensor,
        offset_tensor,
    )
