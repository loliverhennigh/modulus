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


# Normalize one material input to a spatial float32 field on the target device.
def _normalize_material_field(
    material: float | torch.Tensor,
    name: str,
    spatial_shape: tuple[int, int, int],
    device: torch.device,
) -> torch.Tensor:
    if isinstance(material, (int, float)):
        return torch.full(
            spatial_shape,
            float(material),
            device=device,
            dtype=torch.float32,
        )

    if not isinstance(material, torch.Tensor):
        raise TypeError(f"{name} must be a float or torch.Tensor")
    if material.dtype != torch.float32:
        raise TypeError(f"{name} tensor must be float32")
    if material.device != device:
        raise ValueError(f"{name} tensor must be on the same device as magnetic_field")

    if material.ndim == 4:
        if material.shape[0] != 1:
            raise ValueError(
                f"{name} with 4 dimensions must have shape (1, nx, ny, nz)"
            )
        field = material[0]
    elif material.ndim == 3:
        field = material
    else:
        raise ValueError(f"{name} must have shape (nx, ny, nz) or (1, nx, ny, nz)")

    if tuple(field.shape) != spatial_shape:
        raise ValueError(f"{name} spatial shape must match magnetic_field")
    return field


# Validate tensor layout and guard unsupported in-place autograd usage.
def _validate_common_inputs(
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    mu: float | torch.Tensor,
    sigma_m: float | torch.Tensor,
    spacing: torch.Tensor | Sequence[float],
    inplace: bool,
) -> None:
    if electric_field.dtype != torch.float32:
        raise TypeError("electric_field must be float32")
    if magnetic_field.dtype != torch.float32:
        raise TypeError("magnetic_field must be float32")

    if electric_field.ndim != 4 or electric_field.shape[0] != 3:
        raise ValueError("electric_field must have shape (3, nx, ny, nz)")
    if magnetic_field.ndim != 4 or magnetic_field.shape[0] != 3:
        raise ValueError("magnetic_field must have shape (3, nx, ny, nz)")
    if electric_field.shape[1:] != magnetic_field.shape[1:]:
        raise ValueError("electric_field and magnetic_field must share spatial shape")

    if electric_field.device != magnetic_field.device:
        raise ValueError("electric_field and magnetic_field must be on the same device")

    spatial_shape = tuple(magnetic_field.shape[1:])
    for material, name in ((mu, "mu"), (sigma_m, "sigma_m")):
        if isinstance(material, (int, float)):
            continue
        if not isinstance(material, torch.Tensor):
            raise TypeError(f"{name} must be a float or torch.Tensor")
        if material.dtype != torch.float32:
            raise TypeError(f"{name} tensor must be float32")
        if material.device != magnetic_field.device:
            raise ValueError(
                f"{name} tensor must be on the same device as magnetic_field"
            )
        if material.ndim == 4:
            if material.shape[0] != 1:
                raise ValueError(
                    f"{name} with 4 dimensions must have shape (1, nx, ny, nz)"
                )
            if tuple(material.shape[1:]) != spatial_shape:
                raise ValueError(f"{name} spatial shape must match magnetic_field")
        elif material.ndim == 3:
            if tuple(material.shape) != spatial_shape:
                raise ValueError(f"{name} spatial shape must match magnetic_field")
        else:
            raise ValueError(f"{name} must have shape (nx, ny, nz) or (1, nx, ny, nz)")

    if isinstance(spacing, torch.Tensor):
        if spacing.device != magnetic_field.device:
            raise ValueError(
                "spacing tensor must be on the same device as magnetic_field"
            )
        if spacing.requires_grad:
            raise ValueError("spacing gradients are not supported")

    if inplace:
        needs_grad = [
            electric_field.requires_grad,
            magnetic_field.requires_grad,
            isinstance(mu, torch.Tensor) and mu.requires_grad,
            isinstance(sigma_m, torch.Tensor) and sigma_m.requires_grad,
        ]
        if any(needs_grad):
            raise ValueError(
                "inplace=True is not supported when any input requires gradients"
            )


# Normalize spacing to a device tensor of shape (3,).
def _as_spacing_tensor(
    spacing: torch.Tensor | Sequence[float],
    device: torch.device,
    dtype: torch.dtype,
) -> torch.Tensor:
    if isinstance(spacing, torch.Tensor):
        spacing_tensor = spacing.to(device=device, dtype=dtype)
    else:
        spacing_tensor = torch.tensor(tuple(spacing), device=device, dtype=dtype)

    if spacing_tensor.numel() != 3:
        raise ValueError("spacing must contain exactly 3 values")
    return spacing_tensor.reshape(3)


# Compute a safe harmonic average that returns 0 where the denominator is 0.
def _harmonic_average(left: torch.Tensor, right: torch.Tensor) -> torch.Tensor:
    denom = left + right
    return torch.where(denom == 0.0, torch.zeros_like(denom), (2.0 * left * right) / denom)


# Compute Yee-cell magnetic update coefficients from spatial material fields.
def _material_coefficients(
    mu: float | torch.Tensor,
    sigma_m: float | torch.Tensor,
    spacing: torch.Tensor,
    dt: float,
    spatial_shape: tuple[int, int, int],
    device: torch.device,
    expand_scalar_to_spatial: bool = False,
) -> tuple[torch.Tensor, torch.Tensor]:
    mu_is_scalar = isinstance(mu, (int, float))
    sigma_is_scalar = isinstance(sigma_m, (int, float))

    mu_x: torch.Tensor | float
    mu_y: torch.Tensor | float
    mu_z: torch.Tensor | float
    sigma_x: torch.Tensor | float
    sigma_y: torch.Tensor | float
    sigma_z: torch.Tensor | float

    if mu_is_scalar:
        mu_x = float(mu)
        mu_y = float(mu)
        mu_z = float(mu)
    else:
        mu_field = _normalize_material_field(mu, "mu", spatial_shape, device)
        mu_0_1_1 = torch.roll(mu_field, shifts=1, dims=0)
        mu_1_0_1 = torch.roll(mu_field, shifts=1, dims=1)
        mu_1_1_0 = torch.roll(mu_field, shifts=1, dims=2)
        mu_x = _harmonic_average(mu_field, mu_0_1_1)
        mu_y = _harmonic_average(mu_field, mu_1_0_1)
        mu_z = _harmonic_average(mu_field, mu_1_1_0)

    if sigma_is_scalar:
        sigma_x = float(sigma_m)
        sigma_y = float(sigma_m)
        sigma_z = float(sigma_m)
    else:
        sigma_m_field = _normalize_material_field(
            sigma_m, "sigma_m", spatial_shape, device
        )
        sigma_0_1_1 = torch.roll(sigma_m_field, shifts=1, dims=0)
        sigma_1_0_1 = torch.roll(sigma_m_field, shifts=1, dims=1)
        sigma_1_1_0 = torch.roll(sigma_m_field, shifts=1, dims=2)
        sigma_x = _harmonic_average(sigma_m_field, sigma_0_1_1)
        sigma_y = _harmonic_average(sigma_m_field, sigma_1_0_1)
        sigma_z = _harmonic_average(sigma_m_field, sigma_1_1_0)

    if mu_is_scalar and sigma_is_scalar:
        mu_tensor = torch.tensor(
            [mu_x, mu_y, mu_z],  # type: ignore[list-item]
            device=device,
            dtype=torch.float32,
        ).view(3, 1, 1, 1)
        sigma_tensor = torch.tensor(
            [sigma_x, sigma_y, sigma_z],  # type: ignore[list-item]
            device=device,
            dtype=torch.float32,
        ).view(3, 1, 1, 1)
    else:
        if mu_is_scalar:
            if not isinstance(sigma_x, torch.Tensor):
                raise TypeError("internal error: sigma_x must be tensor")
            if not isinstance(sigma_y, torch.Tensor):
                raise TypeError("internal error: sigma_y must be tensor")
            if not isinstance(sigma_z, torch.Tensor):
                raise TypeError("internal error: sigma_z must be tensor")
            mu_x = torch.full_like(sigma_x, float(mu_x))
            mu_y = torch.full_like(sigma_y, float(mu_y))
            mu_z = torch.full_like(sigma_z, float(mu_z))

        if sigma_is_scalar:
            if not isinstance(mu_x, torch.Tensor):
                raise TypeError("internal error: mu_x must be tensor")
            if not isinstance(mu_y, torch.Tensor):
                raise TypeError("internal error: mu_y must be tensor")
            if not isinstance(mu_z, torch.Tensor):
                raise TypeError("internal error: mu_z must be tensor")
            sigma_x = torch.full_like(mu_x, float(sigma_x))
            sigma_y = torch.full_like(mu_y, float(sigma_y))
            sigma_z = torch.full_like(mu_z, float(sigma_z))

        mu_tensor = torch.stack((mu_x, mu_y, mu_z), dim=0)  # type: ignore[arg-type]
        sigma_tensor = torch.stack((sigma_x, sigma_y, sigma_z), dim=0)  # type: ignore[arg-type]

    dt_tensor = torch.tensor(
        dt,
        device=mu_tensor.device,
        dtype=mu_tensor.dtype,
    )
    denom = 2.0 * mu_tensor + sigma_tensor * dt_tensor
    c_hh = (2.0 * mu_tensor - sigma_tensor * dt_tensor) / denom

    spacing_view = spacing.view(3, 1, 1, 1)
    c_he = (2.0 * dt_tensor) / (spacing_view * denom)

    if mu_is_scalar and sigma_is_scalar and expand_scalar_to_spatial:
        nx, ny, nz = spatial_shape
        c_hh = c_hh.expand(3, nx, ny, nz).contiguous()
        c_he = c_he.expand(3, nx, ny, nz).contiguous()

    return c_hh, c_he
