# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

from ..utils import (
    _as_spacing_tensor,
    _normalize_material_field,
    _validate_common_inputs,
)
from .launch_backward import _launch_warp_backward
from .launch_forward import _launch_warp_forward


@torch.library.custom_op("physicsnemo::magnetic_field_update_warp", mutates_args=())
def magnetic_field_update_impl(
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    mu_field: torch.Tensor,
    sigma_m_field: torch.Tensor,
    mu_scalar: float,
    sigma_m_scalar: float,
    mu_is_scalar: bool,
    sigma_is_scalar: bool,
    spacing: torch.Tensor,
    dt: float,
) -> torch.Tensor:
    output = torch.empty_like(magnetic_field)
    mu_input: float | torch.Tensor = float(mu_scalar) if mu_is_scalar else mu_field
    sigma_input: float | torch.Tensor = (
        float(sigma_m_scalar) if sigma_is_scalar else sigma_m_field
    )

    _launch_warp_forward(
        electric_field=electric_field,
        magnetic_field=magnetic_field,
        mu=mu_input,
        sigma_m=sigma_input,
        spacing=spacing,
        dt=dt,
        output=output,
    )
    return output


@magnetic_field_update_impl.register_fake
def _(
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    mu_field: torch.Tensor,
    sigma_m_field: torch.Tensor,
    mu_scalar: float,
    sigma_m_scalar: float,
    mu_is_scalar: bool,
    sigma_is_scalar: bool,
    spacing: torch.Tensor,
    dt: float,
) -> torch.Tensor:
    _ = (
        electric_field,
        mu_field,
        sigma_m_field,
        mu_scalar,
        sigma_m_scalar,
        mu_is_scalar,
        sigma_is_scalar,
        spacing,
        dt,
    )
    return torch.empty_like(magnetic_field)


def setup_magnetic_field_update_context(
    ctx: torch.autograd.function.FunctionCtx,
    inputs: tuple,
    output: torch.Tensor,
) -> None:
    (
        electric_field,
        magnetic_field,
        mu_field,
        sigma_m_field,
        mu_scalar,
        sigma_m_scalar,
        mu_is_scalar,
        sigma_is_scalar,
        spacing,
        dt,
    ) = inputs
    _ = output

    ctx.save_for_backward(
        electric_field,
        magnetic_field,
        mu_field,
        sigma_m_field,
        spacing,
    )
    ctx.mu_scalar = float(mu_scalar)
    ctx.sigma_m_scalar = float(sigma_m_scalar)
    ctx.mu_is_scalar = bool(mu_is_scalar)
    ctx.sigma_is_scalar = bool(sigma_is_scalar)
    ctx.dt = float(dt)


def backward_magnetic_field_update(
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
    None,
    None,
]:
    (
        electric_field,
        magnetic_field,
        mu_field,
        sigma_m_field,
        spacing,
    ) = ctx.saved_tensors

    if grad_output is None:
        return (None, None, None, None, None, None, None, None, None, None)

    grad_electric, grad_magnetic, grad_mu, grad_sigma = _launch_warp_backward(
        electric_field=electric_field,
        magnetic_field=magnetic_field,
        mu_field=mu_field,
        sigma_m_field=sigma_m_field,
        spacing=spacing,
        grad_output=grad_output,
        dt=ctx.dt,
        mu_scalar=ctx.mu_scalar,
        sigma_m_scalar=ctx.sigma_m_scalar,
        mu_is_scalar=ctx.mu_is_scalar,
        sigma_is_scalar=ctx.sigma_is_scalar,
        needs_input_grad=ctx.needs_input_grad,
    )

    return (
        grad_electric,
        grad_magnetic,
        grad_mu,
        grad_sigma,
        None,
        None,
        None,
        None,
        None,
        None,
    )


magnetic_field_update_impl.register_autograd(
    backward_magnetic_field_update,
    setup_context=setup_magnetic_field_update_context,
)


def magnetic_field_update_warp(
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    mu: float | torch.Tensor,
    sigma_m: float | torch.Tensor,
    spacing: torch.Tensor | Sequence[float],
    dt: float,
    inplace: bool = False,
) -> torch.Tensor:
    _validate_common_inputs(
        electric_field,
        magnetic_field,
        mu,
        sigma_m,
        spacing,
        inplace,
    )

    if not electric_field.is_contiguous():
        raise ValueError(
            "electric_field must be contiguous for the warp implementation"
        )
    if not magnetic_field.is_contiguous():
        raise ValueError(
            "magnetic_field must be contiguous for the warp implementation"
        )
    if isinstance(mu, torch.Tensor) and not mu.is_contiguous():
        raise ValueError("mu tensor must be contiguous for the warp implementation")
    if isinstance(sigma_m, torch.Tensor) and not sigma_m.is_contiguous():
        raise ValueError(
            "sigma_m tensor must be contiguous for the warp implementation"
        )
    if isinstance(spacing, torch.Tensor) and not spacing.is_contiguous():
        raise ValueError(
            "spacing tensor must be contiguous for the warp implementation"
        )

    spacing_tensor = _as_spacing_tensor(
        spacing,
        device=magnetic_field.device,
        dtype=magnetic_field.dtype,
    ).contiguous()

    if inplace:
        _launch_warp_forward(
            electric_field=electric_field,
            magnetic_field=magnetic_field,
            mu=mu,
            sigma_m=sigma_m,
            spacing=spacing_tensor,
            dt=dt,
            output=magnetic_field,
        )
        return magnetic_field

    spatial_shape = tuple(magnetic_field.shape[1:])
    empty_material = torch.empty(
        (0, 0, 0),
        device=magnetic_field.device,
        dtype=torch.float32,
    )
    mu_is_scalar = isinstance(mu, (int, float))
    sigma_is_scalar = isinstance(sigma_m, (int, float))
    mu_field = (
        empty_material
        if mu_is_scalar
        else _normalize_material_field(mu, "mu", spatial_shape, magnetic_field.device)
        .contiguous()
    )
    sigma_field = (
        empty_material
        if sigma_is_scalar
        else _normalize_material_field(
            sigma_m,
            "sigma_m",
            spatial_shape,
            magnetic_field.device,
        ).contiguous()
    )

    return magnetic_field_update_impl(
        electric_field,
        magnetic_field,
        mu_field,
        sigma_field,
        float(mu) if mu_is_scalar else 0.0,
        float(sigma_m) if sigma_is_scalar else 0.0,
        mu_is_scalar,
        sigma_is_scalar,
        spacing_tensor,
        float(dt),
    )
