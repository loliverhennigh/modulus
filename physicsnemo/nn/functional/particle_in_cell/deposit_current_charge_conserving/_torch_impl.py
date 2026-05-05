# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import math
from typing import Sequence

import torch

from .utils import _prepare_inputs


def _index_apply(index: int, size: int) -> int:
    return index % size


def _shape_factor_order0(xmid: float) -> tuple[int, tuple[float]]:
    j = int(math.floor(xmid + 0.5))
    return j, (1.0,)


def _shape_factor_order2(xmid: float) -> tuple[int, tuple[float, float, float]]:
    j = int(math.floor(xmid + 0.5))
    xint = xmid - float(j)
    w0 = 0.5 * (0.5 - xint) * (0.5 - xint)
    w1 = 0.75 - xint * xint
    w2 = 0.5 * (0.5 + xint) * (0.5 + xint)
    return j - 1, (w0, w1, w2)


def _shape_factor_pair_order1(
    x_old: float,
    x_new: float,
) -> tuple[int, tuple[float, float], tuple[float, float]]:
    xmid = 0.5 * (x_old + x_new)
    j = int(math.floor(xmid))
    xint_old = x_old - float(j)
    xint_new = x_new - float(j)
    old_weights = (1.0 - xint_old, xint_old)
    new_weights = (1.0 - xint_new, xint_new)
    return j, old_weights, new_weights


def _shape_factor_pair_order2(
    x_old: float,
    x_new: float,
) -> tuple[int, tuple[float, float, float], tuple[float, float, float]]:
    xmid = 0.5 * (x_old + x_new)
    j = int(math.floor(xmid + 0.5))
    xint_old = x_old - float(j)
    xint_new = x_new - float(j)

    old_w0 = 0.5 * (0.5 - xint_old) * (0.5 - xint_old)
    old_w1 = 0.75 - xint_old * xint_old
    old_w2 = 0.5 * (0.5 + xint_old) * (0.5 + xint_old)

    new_w0 = 0.5 * (0.5 - xint_new) * (0.5 - xint_new)
    new_w1 = 0.75 - xint_new * xint_new
    new_w2 = 0.5 * (0.5 + xint_new) * (0.5 + xint_new)

    return j - 1, (old_w0, old_w1, old_w2), (new_w0, new_w1, new_w2)


def _shape_factor_pair_order3(
    x_old: float,
    x_new: float,
) -> tuple[int, tuple[float, float, float, float], tuple[float, float, float, float]]:
    xmid = 0.5 * (x_old + x_new)
    j = int(math.floor(xmid))

    xint_old = x_old - float(j)
    one_minus_old = 1.0 - xint_old
    old_w0 = (1.0 / 6.0) * one_minus_old * one_minus_old * one_minus_old
    old_w1 = (2.0 / 3.0) - xint_old * xint_old * (1.0 - 0.5 * xint_old)
    old_w2 = (2.0 / 3.0) - one_minus_old * one_minus_old * (
        1.0 - 0.5 * one_minus_old
    )
    old_w3 = (1.0 / 6.0) * xint_old * xint_old * xint_old

    xint_new = x_new - float(j)
    one_minus_new = 1.0 - xint_new
    new_w0 = (1.0 / 6.0) * one_minus_new * one_minus_new * one_minus_new
    new_w1 = (2.0 / 3.0) - xint_new * xint_new * (1.0 - 0.5 * xint_new)
    new_w2 = (2.0 / 3.0) - one_minus_new * one_minus_new * (
        1.0 - 0.5 * one_minus_new
    )
    new_w3 = (1.0 / 6.0) * xint_new * xint_new * xint_new

    return j - 1, (old_w0, old_w1, old_w2, old_w3), (new_w0, new_w1, new_w2, new_w3)


def _shape_pair_order1(
    x_old: float,
    x_new: float,
) -> tuple[int, tuple[float, float], tuple[float, float]]:
    return _shape_factor_pair_order1(x_old, x_new)


def _shape_pair_order3(
    x_old: float,
    x_new: float,
) -> tuple[int, tuple[float, float, float, float], tuple[float, float, float, float]]:
    return _shape_factor_pair_order3(x_old, x_new)


def _shape_cell_order1(x_bar: float) -> tuple[int, tuple[float]]:
    return _shape_factor_order0(x_bar)


def _shape_cell_order3(
    x_old: float,
    x_new: float,
    x_bar: float,
) -> tuple[int, tuple[float, float, float]]:
    i0_cell, mid_weights = _shape_factor_order2(x_bar)
    i0_pair, old_weights, new_weights = _shape_factor_pair_order2(x_old, x_new)
    if i0_pair != i0_cell:
        raise RuntimeError("shape support mismatch while computing order-3 cell weights")
    corrected = (
        (4.0 * mid_weights[0] + old_weights[0] + new_weights[0]) / 6.0,
        (4.0 * mid_weights[1] + old_weights[1] + new_weights[1]) / 6.0,
        (4.0 * mid_weights[2] + old_weights[2] + new_weights[2]) / 6.0,
    )
    return i0_cell, corrected


def _deposit_particle_villasenor_shape(
    current_density: torch.Tensor,
    particle_old_grid: tuple[float, float, float],
    particle_new_grid: tuple[float, float, float],
    wqx: float,
    wqy: float,
    wqz: float,
    current_stagger: torch.Tensor,
    shape_order: int,
) -> None:
    nx, ny, nz = (
        int(current_density.shape[1]),
        int(current_density.shape[2]),
        int(current_density.shape[3]),
    )

    x_old, y_old, z_old = particle_old_grid
    x_new, y_new, z_new = particle_new_grid

    dxp = x_new - x_old
    dyp = y_new - y_old
    dzp = z_new - z_old
    if dxp == 0.0 and dyp == 0.0 and dzp == 0.0:
        return

    i_old = int(math.floor(x_old))
    i_new = int(math.floor(x_new))
    j_old = int(math.floor(y_old))
    j_new = int(math.floor(y_new))
    k_old = int(math.floor(z_old))
    k_new = int(math.floor(z_new))

    num_segments = 1 + abs(i_new - i_old) + abs(j_new - j_old) + abs(k_new - k_old)

    dir_x = -1.0 if dxp < 0.0 else 1.0
    dir_y = -1.0 if dyp < 0.0 else 1.0
    dir_z = -1.0 if dzp < 0.0 else 1.0

    x_cell = float(i_old) + 0.5 * (1.0 - dir_x)
    y_cell = float(j_old) + 0.5 * (1.0 - dir_y)
    z_cell = float(k_old) + 0.5 * (1.0 - dir_z)

    node_len = 2 if shape_order == 1 else 4
    cell_len = 1 if shape_order == 1 else 3

    x0_old = x_old
    y0_old = y_old
    z0_old = z_old

    one_third = 1.0 / 3.0
    one_sixth = 1.0 / 6.0

    for segment_index in range(num_segments):
        if segment_index == num_segments - 1:
            x0_new = x_new
            y0_new = y_new
            z0_new = z_new
            dxp_seg = x0_new - x0_old
            dyp_seg = y0_new - y0_old
            dzp_seg = z0_new - z0_old
        else:
            x0_new = x_cell + dir_x
            y0_new = y_cell + dir_y
            z0_new = z_cell + dir_z
            dxp_seg = x0_new - x0_old
            dyp_seg = y0_new - y0_old
            dzp_seg = z0_new - z0_old

            cond_x = (dyp == 0.0 or abs(dxp_seg) < abs(dxp / dyp * dyp_seg)) and (
                dzp == 0.0 or abs(dxp_seg) < abs(dxp / dzp * dzp_seg)
            )
            cond_y = dzp == 0.0 or abs(dyp_seg) < abs(dyp / dzp * dzp_seg)

            if cond_x:
                x_cell = x0_new
                if dxp != 0.0:
                    dyp_seg = dyp / dxp * dxp_seg
                    dzp_seg = dzp / dxp * dxp_seg
                y0_new = y0_old + dyp_seg
                z0_new = z0_old + dzp_seg
            elif cond_y:
                y_cell = y0_new
                if dyp != 0.0:
                    dxp_seg = dxp / dyp * dyp_seg
                    dzp_seg = dzp / dyp * dyp_seg
                x0_new = x0_old + dxp_seg
                z0_new = z0_old + dzp_seg
            else:
                z_cell = z0_new
                if dzp != 0.0:
                    dxp_seg = dxp / dzp * dzp_seg
                    dyp_seg = dyp / dzp * dzp_seg
                x0_new = x0_old + dxp_seg
                y0_new = y0_old + dyp_seg

        seg_factor_x = 1.0 if dxp == 0.0 else dxp_seg / dxp
        seg_factor_y = 1.0 if dyp == 0.0 else dyp_seg / dyp
        seg_factor_z = 1.0 if dzp == 0.0 else dzp_seg / dzp

        sx = float(current_stagger[0, 0].item())
        sy = float(current_stagger[0, 1].item())
        sz = float(current_stagger[0, 2].item())
        x_bar = 0.5 * (x0_old + x0_new)
        if shape_order == 1:
            i0_cell, sx_cell = _shape_cell_order1(x_bar - sx)
            j0_node, sy_old, sy_new = _shape_pair_order1(y0_old - sy, y0_new - sy)
            k0_node, sz_old, sz_new = _shape_pair_order1(z0_old - sz, z0_new - sz)
        else:
            i0_cell, sx_cell = _shape_cell_order3(x0_old - sx, x0_new - sx, x_bar - sx)
            j0_node, sy_old, sy_new = _shape_pair_order3(y0_old - sy, y0_new - sy)
            k0_node, sz_old, sz_new = _shape_pair_order3(z0_old - sz, z0_new - sz)

        for i_offset in range(cell_len):
            ix = _index_apply(i0_cell + i_offset, nx)
            wx_cell = sx_cell[i_offset]
            for j_offset in range(node_len):
                iy = _index_apply(j0_node + j_offset, ny)
                wy_old = sy_old[j_offset]
                wy_new = sy_new[j_offset]
                for k_offset in range(node_len):
                    iz = _index_apply(k0_node + k_offset, nz)
                    wz_old = sz_old[k_offset]
                    wz_new = sz_new[k_offset]
                    weight = wx_cell * (
                        wy_old * wz_old * one_third
                        + wy_old * wz_new * one_sixth
                        + wy_new * wz_old * one_sixth
                        + wy_new * wz_new * one_third
                    ) * seg_factor_x
                    current_density[0, ix, iy, iz] += float(wqx * weight)

        sx = float(current_stagger[1, 0].item())
        sy = float(current_stagger[1, 1].item())
        sz = float(current_stagger[1, 2].item())
        y_bar = 0.5 * (y0_old + y0_new)
        if shape_order == 1:
            j0_cell, sy_cell = _shape_cell_order1(y_bar - sy)
            i0_node, sx_old, sx_new = _shape_pair_order1(x0_old - sx, x0_new - sx)
            k0_node, sz_old, sz_new = _shape_pair_order1(z0_old - sz, z0_new - sz)
        else:
            j0_cell, sy_cell = _shape_cell_order3(y0_old - sy, y0_new - sy, y_bar - sy)
            i0_node, sx_old, sx_new = _shape_pair_order3(x0_old - sx, x0_new - sx)
            k0_node, sz_old, sz_new = _shape_pair_order3(z0_old - sz, z0_new - sz)

        for i_offset in range(node_len):
            ix = _index_apply(i0_node + i_offset, nx)
            wx_old = sx_old[i_offset]
            wx_new = sx_new[i_offset]
            for j_offset in range(cell_len):
                iy = _index_apply(j0_cell + j_offset, ny)
                wy_cell = sy_cell[j_offset]
                for k_offset in range(node_len):
                    iz = _index_apply(k0_node + k_offset, nz)
                    wz_old = sz_old[k_offset]
                    wz_new = sz_new[k_offset]
                    weight = wy_cell * (
                        wx_old * wz_old * one_third
                        + wx_old * wz_new * one_sixth
                        + wx_new * wz_old * one_sixth
                        + wx_new * wz_new * one_third
                    ) * seg_factor_y
                    current_density[1, ix, iy, iz] += float(wqy * weight)

        sx = float(current_stagger[2, 0].item())
        sy = float(current_stagger[2, 1].item())
        sz = float(current_stagger[2, 2].item())
        z_bar = 0.5 * (z0_old + z0_new)
        if shape_order == 1:
            k0_cell, sz_cell = _shape_cell_order1(z_bar - sz)
            i0_node, sx_old, sx_new = _shape_pair_order1(x0_old - sx, x0_new - sx)
            j0_node, sy_old, sy_new = _shape_pair_order1(y0_old - sy, y0_new - sy)
        else:
            k0_cell, sz_cell = _shape_cell_order3(z0_old - sz, z0_new - sz, z_bar - sz)
            i0_node, sx_old, sx_new = _shape_pair_order3(x0_old - sx, x0_new - sx)
            j0_node, sy_old, sy_new = _shape_pair_order3(y0_old - sy, y0_new - sy)

        for i_offset in range(node_len):
            ix = _index_apply(i0_node + i_offset, nx)
            wx_old = sx_old[i_offset]
            wx_new = sx_new[i_offset]
            for j_offset in range(node_len):
                iy = _index_apply(j0_node + j_offset, ny)
                wy_old = sy_old[j_offset]
                wy_new = sy_new[j_offset]
                for k_offset in range(cell_len):
                    iz = _index_apply(k0_cell + k_offset, nz)
                    wz_cell = sz_cell[k_offset]
                    weight = wz_cell * (
                        wx_old * wy_old * one_third
                        + wx_old * wy_new * one_sixth
                        + wx_new * wy_old * one_sixth
                        + wx_new * wy_new * one_third
                    ) * seg_factor_z
                    current_density[2, ix, iy, iz] += float(wqz * weight)

        x0_old = x0_new
        y0_old = y0_new
        z0_old = z0_new


def _deposit_current_charge_conserving_step(
    particle_position_old: torch.Tensor,
    particle_position_new: torch.Tensor,
    particle_weight: torch.Tensor,
    particle_charge: float,
    dt: float,
    grid_shape: tuple[int, int, int],
    origin: torch.Tensor,
    spacing: torch.Tensor,
    current_stagger: torch.Tensor,
    shape_order: int,
    current_density: torch.Tensor,
) -> torch.Tensor:
    nx, ny, nz = grid_shape

    invvol = 1.0 / float(spacing[0] * spacing[1] * spacing[2])

    num_particles = int(particle_position_old.shape[0])
    nx_float = float(nx)
    ny_float = float(ny)
    nz_float = float(nz)
    half_nx = 0.5 * nx_float
    half_ny = 0.5 * ny_float
    half_nz = 0.5 * nz_float

    for particle_index in range(num_particles):
        old_grid = (
            float((particle_position_old[particle_index, 0] - origin[0]) / spacing[0]),
            float((particle_position_old[particle_index, 1] - origin[1]) / spacing[1]),
            float((particle_position_old[particle_index, 2] - origin[2]) / spacing[2]),
        )
        new_grid = (
            float((particle_position_new[particle_index, 0] - origin[0]) / spacing[0]),
            float((particle_position_new[particle_index, 1] - origin[1]) / spacing[1]),
            float((particle_position_new[particle_index, 2] - origin[2]) / spacing[2]),
        )

        x_old, y_old, z_old = old_grid
        x_new, y_new, z_new = new_grid

        dx_wrap = x_new - x_old
        dy_wrap = y_new - y_old
        dz_wrap = z_new - z_old
        if dx_wrap > half_nx:
            x_new = x_new - nx_float
        elif dx_wrap < -half_nx:
            x_new = x_new + nx_float
        if dy_wrap > half_ny:
            y_new = y_new - ny_float
        elif dy_wrap < -half_ny:
            y_new = y_new + ny_float
        if dz_wrap > half_nz:
            z_new = z_new - nz_float
        elif dz_wrap < -half_nz:
            z_new = z_new + nz_float

        if x_new == x_old and y_new == y_old and z_new == z_old:
            continue

        wq = float(particle_charge * particle_weight[particle_index])
        vx = float((x_new - x_old) * spacing[0] / dt)
        vy = float((y_new - y_old) * spacing[1] / dt)
        vz = float((z_new - z_old) * spacing[2] / dt)

        wqx = wq * vx * invvol
        wqy = wq * vy * invvol
        wqz = wq * vz * invvol

        _deposit_particle_villasenor_shape(
            current_density=current_density,
            particle_old_grid=(x_old, y_old, z_old),
            particle_new_grid=(x_new, y_new, z_new),
            wqx=wqx,
            wqy=wqy,
            wqz=wqz,
            current_stagger=current_stagger,
            shape_order=shape_order,
        )

    return current_density


def deposit_current_charge_conserving_torch(
    particle_position_old: torch.Tensor,
    particle_position_new: torch.Tensor,
    particle_weight: torch.Tensor,
    particle_charge: float,
    dt: float,
    grid_shape: Sequence[int] | torch.Tensor,
    origin: torch.Tensor | Sequence[float] = (0.0, 0.0, 0.0),
    spacing: torch.Tensor | Sequence[float] = (1.0, 1.0, 1.0),
    current_stagger: torch.Tensor | Sequence[Sequence[float]] | None = None,
    periodic: bool = True,
    shape_order: int = 1,
    current_density: torch.Tensor | None = None,
) -> torch.Tensor:
    (
        grid_shape_tuple,
        origin_tensor,
        spacing_tensor,
        current_stagger_tensor,
        shape_order_value,
    ) = _prepare_inputs(
        particle_position_old=particle_position_old,
        particle_position_new=particle_position_new,
        particle_weight=particle_weight,
        particle_charge=particle_charge,
        dt=dt,
        grid_shape=grid_shape,
        origin=origin,
        spacing=spacing,
        current_stagger=current_stagger,
        periodic=periodic,
        shape_order=shape_order,
    )

    if current_density is None:
        current_density_out = torch.zeros(
            (3, *grid_shape_tuple),
            device=particle_position_old.device,
            dtype=torch.float32,
        )
    else:
        if not isinstance(current_density, torch.Tensor):
            raise TypeError("current_density must be a torch.Tensor when provided")
        if current_density.dtype != torch.float32:
            raise TypeError("current_density must use torch.float32 dtype")
        if current_density.shape != (3, *grid_shape_tuple):
            raise ValueError("current_density must have shape (3, nx, ny, nz)")
        if current_density.device != particle_position_old.device:
            raise ValueError("current_density must be on the same device as particle inputs")
        if current_density.requires_grad:
            raise ValueError(
                "deposit_current_charge_conserving does not support gradients through current_density"
            )
        if not current_density.is_contiguous():
            raise ValueError("current_density must be contiguous")
        current_density_out = current_density

    return _deposit_current_charge_conserving_step(
        particle_position_old=particle_position_old,
        particle_position_new=particle_position_new,
        particle_weight=particle_weight,
        particle_charge=particle_charge,
        dt=float(dt),
        grid_shape=grid_shape_tuple,
        origin=origin_tensor,
        spacing=spacing_tensor,
        current_stagger=current_stagger_tensor,
        shape_order=shape_order_value,
        current_density=current_density_out,
    )


__all__ = [
    "_deposit_current_charge_conserving_step",
    "deposit_current_charge_conserving_torch",
]
