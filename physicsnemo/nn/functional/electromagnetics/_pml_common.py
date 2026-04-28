# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Sequence

import torch

PML_NUM_CHANNELS = 36

PML_PHI_E = slice(0, 3)
PML_PHI_H = slice(3, 6)
PML_PSI_E_X = slice(6, 9)
PML_PSI_E_Y = slice(9, 12)
PML_PSI_E_Z = slice(12, 15)
PML_PSI_H_X = slice(15, 18)
PML_PSI_H_Y = slice(18, 21)
PML_PSI_H_Z = slice(21, 24)
PML_BE = slice(24, 27)
PML_CE = slice(27, 30)
PML_BH = slice(30, 33)
PML_CH = slice(33, 36)


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


# Normalize offset to a 3-tuple of ints.
def _normalize_offset(offset: torch.Tensor | Sequence[int]) -> tuple[int, int, int]:
    if isinstance(offset, torch.Tensor):
        if offset.numel() != 3:
            raise ValueError("pml_layer_offset must have 3 elements")
        values = offset.detach().cpu().flatten().tolist()
    else:
        values = list(offset)

    if len(values) != 3:
        raise ValueError("pml_layer_offset must have 3 elements")
    return int(values[0]), int(values[1]), int(values[2])


# Normalize direction to a float32 tensor with exactly one non-zero axis.
def _normalize_direction(
    direction: torch.Tensor | Sequence[float],
    device: torch.device,
) -> torch.Tensor:
    if isinstance(direction, torch.Tensor):
        direction_tensor = direction.to(device=device, dtype=torch.float32)
    else:
        direction_tensor = torch.tensor(
            tuple(direction), device=device, dtype=torch.float32
        )

    if direction_tensor.numel() != 3:
        raise ValueError("direction must contain exactly 3 values")
    direction_tensor = direction_tensor.reshape(3)

    non_zero = torch.count_nonzero(direction_tensor)
    if int(non_zero.item()) != 1:
        raise ValueError("direction must have exactly one non-zero component")

    non_zero_value = direction_tensor[direction_tensor != 0.0][0]
    if not torch.isclose(non_zero_value.abs(), torch.tensor(1.0, device=device)):
        raise ValueError("direction non-zero component must be +1 or -1")

    return direction_tensor


# Validate the common PML layer tensor contract.
def _validate_pml_layer_tensor(pml_layer: torch.Tensor) -> None:
    if pml_layer.dtype != torch.float32:
        raise TypeError("pml_layer must be float32")
    if pml_layer.ndim != 4 or pml_layer.shape[0] != PML_NUM_CHANNELS:
        raise ValueError("pml_layer must have shape (36, nx, ny, nz)")
    if pml_layer.shape[1] <= 0 or pml_layer.shape[2] <= 0 or pml_layer.shape[3] <= 0:
        raise ValueError("pml_layer spatial dimensions must be greater than zero")


# Validate one electromagnetic vector field tensor.
def _validate_vector_field(name: str, field: torch.Tensor) -> None:
    if field.dtype != torch.float32:
        raise TypeError(f"{name} must be float32")
    if field.ndim != 4 or field.shape[0] != 3:
        raise ValueError(f"{name} must have shape (3, nx, ny, nz)")


# Normalize scalar-or-field material input to a spatial float32 tensor.
def _normalize_material_field(
    material: float | torch.Tensor,
    name: str,
    spatial_shape: tuple[int, int, int],
    device: torch.device,
    *,
    field_name: str,
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
        raise ValueError(
            f"{name} tensor must be on the same device as {field_name}"
        )

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
        raise ValueError(f"{name} spatial shape must match {field_name}")

    return field


# Gather values from a periodic 3D tensor at broadcastable index grids.
def _periodic_gather(
    field: torch.Tensor,
    i: torch.Tensor,
    j: torch.Tensor,
    k: torch.Tensor,
) -> torch.Tensor:
    nx, ny, nz = field.shape
    ii = torch.remainder(i, nx)
    jj = torch.remainder(j, ny)
    kk = torch.remainder(k, nz)
    return field[ii, jj, kk]


# Scatter-add updates into a periodic 3D target tensor.
def _periodic_scatter_add_(
    target: torch.Tensor,
    i: torch.Tensor,
    j: torch.Tensor,
    k: torch.Tensor,
    values: torch.Tensor,
) -> None:
    nx, ny, nz = target.shape
    ii = torch.remainder(i, nx)
    jj = torch.remainder(j, ny)
    kk = torch.remainder(k, nz)
    linear = ((ii * ny) + jj) * nz + kk
    target_flat = target.reshape(-1)
    target_flat.scatter_add_(0, linear.reshape(-1), values.reshape(-1))


# Build broadcasted index grids for a PML layer region and offset.
def _pml_region_indices(
    pml_shape: tuple[int, int, int],
    offset: tuple[int, int, int],
    device: torch.device,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    px, py, pz = pml_shape
    ox, oy, oz = offset
    i = torch.arange(px, device=device, dtype=torch.int64).view(px, 1, 1) + ox
    j = torch.arange(py, device=device, dtype=torch.int64).view(1, py, 1) + oy
    k = torch.arange(pz, device=device, dtype=torch.int64).view(1, 1, pz) + oz
    return i, j, k


# Disallow warp backend usage when autograd is requested.
def _validate_no_autograd_warp(*tensors: torch.Tensor | None) -> None:
    for tensor in tensors:
        if tensor is not None and tensor.requires_grad:
            raise ValueError(
                "warp implementation does not support autograd for this functional"
            )
