# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warp as wp


@wp.func
def _wrap_index_periodic_local(index: wp.int32, size: wp.int32) -> wp.int32:
    if index < 0:
        return index + size
    if index >= size:
        return index - size
    return index


@wp.kernel
def _deposit_current_charge_conserving_kernel_jx_scalar_scalar(
    particle_position_old: wp.array2d(dtype=wp.float32),
    particle_position_new: wp.array2d(dtype=wp.float32),
    particle_weight: wp.array(dtype=wp.float32),
    current_stagger: wp.array2d(dtype=wp.float32),
    current_density: wp.array4d(dtype=wp.float32),
    particle_charge: wp.float32,
    dt: wp.float32,
    origin: wp.vec3f,
    spacing: wp.vec3f,
):
    particle_index = wp.tid()

    nx = wp.int32(current_density.shape[1])
    ny = wp.int32(current_density.shape[2])
    nz = wp.int32(current_density.shape[3])
    nx_f = wp.float32(nx)
    ny_f = wp.float32(ny)
    nz_f = wp.float32(nz)

    x_old = (particle_position_old[particle_index, 0] - origin[0]) / spacing[0]
    y_old = (particle_position_old[particle_index, 1] - origin[1]) / spacing[1]
    z_old = (particle_position_old[particle_index, 2] - origin[2]) / spacing[2]
    x_new = (particle_position_new[particle_index, 0] - origin[0]) / spacing[0]
    y_new = (particle_position_new[particle_index, 1] - origin[1]) / spacing[1]
    z_new = (particle_position_new[particle_index, 2] - origin[2]) / spacing[2]

    dx_wrap = x_new - x_old
    dy_wrap = y_new - y_old
    dz_wrap = z_new - z_old
    if dx_wrap > wp.float32(0.5) * nx_f:
        x_new = x_new - nx_f
    elif dx_wrap < -wp.float32(0.5) * nx_f:
        x_new = x_new + nx_f
    if dy_wrap > wp.float32(0.5) * ny_f:
        y_new = y_new - ny_f
    elif dy_wrap < -wp.float32(0.5) * ny_f:
        y_new = y_new + ny_f
    if dz_wrap > wp.float32(0.5) * nz_f:
        z_new = z_new - nz_f
    elif dz_wrap < -wp.float32(0.5) * nz_f:
        z_new = z_new + nz_f

    dxp = x_new - x_old
    dyp = y_new - y_old
    dzp = z_new - z_old
    if dxp == wp.float32(0.0) and dyp == wp.float32(0.0) and dzp == wp.float32(0.0):
        return

    i_old = wp.int32(wp.floor(x_old))
    i_new = wp.int32(wp.floor(x_new))
    j_old = wp.int32(wp.floor(y_old))
    j_new = wp.int32(wp.floor(y_new))
    k_old = wp.int32(wp.floor(z_old))
    k_new = wp.int32(wp.floor(z_new))

    num_segments = wp.int32(1) + wp.abs(i_new - i_old) + wp.abs(j_new - j_old) + wp.abs(
        k_new - k_old
    )

    dir_x = wp.float32(-1.0) if dxp < wp.float32(0.0) else wp.float32(1.0)
    dir_y = wp.float32(-1.0) if dyp < wp.float32(0.0) else wp.float32(1.0)
    dir_z = wp.float32(-1.0) if dzp < wp.float32(0.0) else wp.float32(1.0)

    x_cell = wp.float32(i_old) + wp.float32(0.5) * (wp.float32(1.0) - dir_x)
    y_cell = wp.float32(j_old) + wp.float32(0.5) * (wp.float32(1.0) - dir_y)
    z_cell = wp.float32(k_old) + wp.float32(0.5) * (wp.float32(1.0) - dir_z)

    invvol = wp.float32(1.0) / (spacing[0] * spacing[1] * spacing[2])
    wq = particle_charge * particle_weight[particle_index]
    vx = (x_new - x_old) * spacing[0] / dt
    wqx = wq * vx * invvol

    x0_old = x_old
    y0_old = y_old
    z0_old = z_old

    segment_index = wp.int32(0)
    while segment_index < num_segments:
        x0_new = wp.float32(0.0)
        y0_new = wp.float32(0.0)
        z0_new = wp.float32(0.0)
        dxp_seg = wp.float32(0.0)
        dyp_seg = wp.float32(0.0)
        dzp_seg = wp.float32(0.0)

        if segment_index == (num_segments - 1):
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

            cond_x = True
            if dyp != wp.float32(0.0):
                cond_x = cond_x and (
                    wp.abs(dxp_seg) < wp.abs((dxp / dyp) * dyp_seg)
                )
            if dzp != wp.float32(0.0):
                cond_x = cond_x and (
                    wp.abs(dxp_seg) < wp.abs((dxp / dzp) * dzp_seg)
                )

            cond_y = True
            if dzp != wp.float32(0.0):
                cond_y = wp.abs(dyp_seg) < wp.abs((dyp / dzp) * dzp_seg)

            if cond_x:
                x_cell = x0_new
                if dxp != wp.float32(0.0):
                    dyp_seg = (dyp / dxp) * dxp_seg
                    dzp_seg = (dzp / dxp) * dxp_seg
                y0_new = y0_old + dyp_seg
                z0_new = z0_old + dzp_seg
            elif cond_y:
                y_cell = y0_new
                if dyp != wp.float32(0.0):
                    dxp_seg = (dxp / dyp) * dyp_seg
                    dzp_seg = (dzp / dyp) * dyp_seg
                x0_new = x0_old + dxp_seg
                z0_new = z0_old + dzp_seg
            else:
                z_cell = z0_new
                if dzp != wp.float32(0.0):
                    dxp_seg = (dxp / dzp) * dzp_seg
                    dyp_seg = (dyp / dzp) * dzp_seg
                x0_new = x0_old + dxp_seg
                y0_new = y0_old + dyp_seg

        seg_factor_x = wp.float32(1.0) if dxp == wp.float32(0.0) else dxp_seg / dxp

        sx = current_stagger[0, 0]
        sy = current_stagger[0, 1]
        sz = current_stagger[0, 2]
        x_bar = wp.float32(0.5) * (x0_old + x0_new)
        i0_cell = wp.int32(wp.floor((x_bar - sx) + wp.float32(0.5)))

        y_old_s = y0_old - sy
        y_new_s = y0_new - sy
        j0_node = wp.int32(wp.floor(wp.float32(0.5) * (y_old_s + y_new_s)))
        fy_old = y_old_s - wp.float32(j0_node)
        fy_new = y_new_s - wp.float32(j0_node)
        sy_old0 = wp.float32(1.0) - fy_old
        sy_old1 = fy_old
        sy_new0 = wp.float32(1.0) - fy_new
        sy_new1 = fy_new

        z_old_s = z0_old - sz
        z_new_s = z0_new - sz
        k0_node = wp.int32(wp.floor(wp.float32(0.5) * (z_old_s + z_new_s)))
        fz_old = z_old_s - wp.float32(k0_node)
        fz_new = z_new_s - wp.float32(k0_node)
        sz_old0 = wp.float32(1.0) - fz_old
        sz_old1 = fz_old
        sz_new0 = wp.float32(1.0) - fz_new
        sz_new1 = fz_new

        ix = _wrap_index_periodic_local(i0_cell, nx)

        j_offset = wp.int32(0)
        while j_offset < 2:
            wy_old = sy_old0 if j_offset == 0 else sy_old1
            wy_new = sy_new0 if j_offset == 0 else sy_new1
            iy = _wrap_index_periodic_local(j0_node + j_offset, ny)
            k_offset = wp.int32(0)
            while k_offset < 2:
                wz_old = sz_old0 if k_offset == 0 else sz_old1
                wz_new = sz_new0 if k_offset == 0 else sz_new1
                iz = _wrap_index_periodic_local(k0_node + k_offset, nz)
                weight_x = (
                    wy_old * wz_old / wp.float32(3.0)
                    + wy_old * wz_new / wp.float32(6.0)
                    + wy_new * wz_old / wp.float32(6.0)
                    + wy_new * wz_new / wp.float32(3.0)
                ) * seg_factor_x
                wp.atomic_add(current_density, 0, ix, iy, iz, wqx * weight_x)
                k_offset = k_offset + 1
            j_offset = j_offset + 1

        x0_old = x0_new
        y0_old = y0_new
        z0_old = z0_new
        segment_index = segment_index + 1


@wp.kernel
def _deposit_current_charge_conserving_kernel_jy_scalar_scalar(
    particle_position_old: wp.array2d(dtype=wp.float32),
    particle_position_new: wp.array2d(dtype=wp.float32),
    particle_weight: wp.array(dtype=wp.float32),
    current_stagger: wp.array2d(dtype=wp.float32),
    current_density: wp.array4d(dtype=wp.float32),
    particle_charge: wp.float32,
    dt: wp.float32,
    origin: wp.vec3f,
    spacing: wp.vec3f,
):
    particle_index = wp.tid()

    nx = wp.int32(current_density.shape[1])
    ny = wp.int32(current_density.shape[2])
    nz = wp.int32(current_density.shape[3])
    nx_f = wp.float32(nx)
    ny_f = wp.float32(ny)
    nz_f = wp.float32(nz)

    x_old = (particle_position_old[particle_index, 0] - origin[0]) / spacing[0]
    y_old = (particle_position_old[particle_index, 1] - origin[1]) / spacing[1]
    z_old = (particle_position_old[particle_index, 2] - origin[2]) / spacing[2]
    x_new = (particle_position_new[particle_index, 0] - origin[0]) / spacing[0]
    y_new = (particle_position_new[particle_index, 1] - origin[1]) / spacing[1]
    z_new = (particle_position_new[particle_index, 2] - origin[2]) / spacing[2]

    dx_wrap = x_new - x_old
    dy_wrap = y_new - y_old
    dz_wrap = z_new - z_old
    if dx_wrap > wp.float32(0.5) * nx_f:
        x_new = x_new - nx_f
    elif dx_wrap < -wp.float32(0.5) * nx_f:
        x_new = x_new + nx_f
    if dy_wrap > wp.float32(0.5) * ny_f:
        y_new = y_new - ny_f
    elif dy_wrap < -wp.float32(0.5) * ny_f:
        y_new = y_new + ny_f
    if dz_wrap > wp.float32(0.5) * nz_f:
        z_new = z_new - nz_f
    elif dz_wrap < -wp.float32(0.5) * nz_f:
        z_new = z_new + nz_f

    dxp = x_new - x_old
    dyp = y_new - y_old
    dzp = z_new - z_old
    if dxp == wp.float32(0.0) and dyp == wp.float32(0.0) and dzp == wp.float32(0.0):
        return

    i_old = wp.int32(wp.floor(x_old))
    i_new = wp.int32(wp.floor(x_new))
    j_old = wp.int32(wp.floor(y_old))
    j_new = wp.int32(wp.floor(y_new))
    k_old = wp.int32(wp.floor(z_old))
    k_new = wp.int32(wp.floor(z_new))

    num_segments = wp.int32(1) + wp.abs(i_new - i_old) + wp.abs(j_new - j_old) + wp.abs(
        k_new - k_old
    )

    dir_x = wp.float32(-1.0) if dxp < wp.float32(0.0) else wp.float32(1.0)
    dir_y = wp.float32(-1.0) if dyp < wp.float32(0.0) else wp.float32(1.0)
    dir_z = wp.float32(-1.0) if dzp < wp.float32(0.0) else wp.float32(1.0)

    x_cell = wp.float32(i_old) + wp.float32(0.5) * (wp.float32(1.0) - dir_x)
    y_cell = wp.float32(j_old) + wp.float32(0.5) * (wp.float32(1.0) - dir_y)
    z_cell = wp.float32(k_old) + wp.float32(0.5) * (wp.float32(1.0) - dir_z)

    invvol = wp.float32(1.0) / (spacing[0] * spacing[1] * spacing[2])
    wq = particle_charge * particle_weight[particle_index]
    vy = (y_new - y_old) * spacing[1] / dt
    wqy = wq * vy * invvol

    x0_old = x_old
    y0_old = y_old
    z0_old = z_old

    segment_index = wp.int32(0)
    while segment_index < num_segments:
        x0_new = wp.float32(0.0)
        y0_new = wp.float32(0.0)
        z0_new = wp.float32(0.0)
        dxp_seg = wp.float32(0.0)
        dyp_seg = wp.float32(0.0)
        dzp_seg = wp.float32(0.0)

        if segment_index == (num_segments - 1):
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

            cond_x = True
            if dyp != wp.float32(0.0):
                cond_x = cond_x and (
                    wp.abs(dxp_seg) < wp.abs((dxp / dyp) * dyp_seg)
                )
            if dzp != wp.float32(0.0):
                cond_x = cond_x and (
                    wp.abs(dxp_seg) < wp.abs((dxp / dzp) * dzp_seg)
                )

            cond_y = True
            if dzp != wp.float32(0.0):
                cond_y = wp.abs(dyp_seg) < wp.abs((dyp / dzp) * dzp_seg)

            if cond_x:
                x_cell = x0_new
                if dxp != wp.float32(0.0):
                    dyp_seg = (dyp / dxp) * dxp_seg
                    dzp_seg = (dzp / dxp) * dxp_seg
                y0_new = y0_old + dyp_seg
                z0_new = z0_old + dzp_seg
            elif cond_y:
                y_cell = y0_new
                if dyp != wp.float32(0.0):
                    dxp_seg = (dxp / dyp) * dyp_seg
                    dzp_seg = (dzp / dyp) * dyp_seg
                x0_new = x0_old + dxp_seg
                z0_new = z0_old + dzp_seg
            else:
                z_cell = z0_new
                if dzp != wp.float32(0.0):
                    dxp_seg = (dxp / dzp) * dzp_seg
                    dyp_seg = (dyp / dzp) * dzp_seg
                x0_new = x0_old + dxp_seg
                y0_new = y0_old + dyp_seg

        seg_factor_y = wp.float32(1.0) if dyp == wp.float32(0.0) else dyp_seg / dyp

        sx = current_stagger[1, 0]
        sy = current_stagger[1, 1]
        sz = current_stagger[1, 2]
        y_bar = wp.float32(0.5) * (y0_old + y0_new)
        j0_cell = wp.int32(wp.floor((y_bar - sy) + wp.float32(0.5)))

        x_old_s = x0_old - sx
        x_new_s = x0_new - sx
        i0_node = wp.int32(wp.floor(wp.float32(0.5) * (x_old_s + x_new_s)))
        fx_old = x_old_s - wp.float32(i0_node)
        fx_new = x_new_s - wp.float32(i0_node)
        sx_old0 = wp.float32(1.0) - fx_old
        sx_old1 = fx_old
        sx_new0 = wp.float32(1.0) - fx_new
        sx_new1 = fx_new

        z_old_s = z0_old - sz
        z_new_s = z0_new - sz
        k0_node = wp.int32(wp.floor(wp.float32(0.5) * (z_old_s + z_new_s)))
        fz_old = z_old_s - wp.float32(k0_node)
        fz_new = z_new_s - wp.float32(k0_node)
        sz_old0 = wp.float32(1.0) - fz_old
        sz_old1 = fz_old
        sz_new0 = wp.float32(1.0) - fz_new
        sz_new1 = fz_new

        iy = _wrap_index_periodic_local(j0_cell, ny)

        i_offset = wp.int32(0)
        while i_offset < 2:
            wx_old = sx_old0 if i_offset == 0 else sx_old1
            wx_new = sx_new0 if i_offset == 0 else sx_new1
            ix = _wrap_index_periodic_local(i0_node + i_offset, nx)
            k_offset = wp.int32(0)
            while k_offset < 2:
                wz_old = sz_old0 if k_offset == 0 else sz_old1
                wz_new = sz_new0 if k_offset == 0 else sz_new1
                iz = _wrap_index_periodic_local(k0_node + k_offset, nz)
                weight_y = (
                    wx_old * wz_old / wp.float32(3.0)
                    + wx_old * wz_new / wp.float32(6.0)
                    + wx_new * wz_old / wp.float32(6.0)
                    + wx_new * wz_new / wp.float32(3.0)
                ) * seg_factor_y
                wp.atomic_add(current_density, 1, ix, iy, iz, wqy * weight_y)
                k_offset = k_offset + 1
            i_offset = i_offset + 1

        x0_old = x0_new
        y0_old = y0_new
        z0_old = z0_new
        segment_index = segment_index + 1


@wp.kernel
def _deposit_current_charge_conserving_kernel_jz_scalar_scalar(
    particle_position_old: wp.array2d(dtype=wp.float32),
    particle_position_new: wp.array2d(dtype=wp.float32),
    particle_weight: wp.array(dtype=wp.float32),
    current_stagger: wp.array2d(dtype=wp.float32),
    current_density: wp.array4d(dtype=wp.float32),
    particle_charge: wp.float32,
    dt: wp.float32,
    origin: wp.vec3f,
    spacing: wp.vec3f,
):
    particle_index = wp.tid()

    nx = wp.int32(current_density.shape[1])
    ny = wp.int32(current_density.shape[2])
    nz = wp.int32(current_density.shape[3])
    nx_f = wp.float32(nx)
    ny_f = wp.float32(ny)
    nz_f = wp.float32(nz)

    x_old = (particle_position_old[particle_index, 0] - origin[0]) / spacing[0]
    y_old = (particle_position_old[particle_index, 1] - origin[1]) / spacing[1]
    z_old = (particle_position_old[particle_index, 2] - origin[2]) / spacing[2]
    x_new = (particle_position_new[particle_index, 0] - origin[0]) / spacing[0]
    y_new = (particle_position_new[particle_index, 1] - origin[1]) / spacing[1]
    z_new = (particle_position_new[particle_index, 2] - origin[2]) / spacing[2]

    dx_wrap = x_new - x_old
    dy_wrap = y_new - y_old
    dz_wrap = z_new - z_old
    if dx_wrap > wp.float32(0.5) * nx_f:
        x_new = x_new - nx_f
    elif dx_wrap < -wp.float32(0.5) * nx_f:
        x_new = x_new + nx_f
    if dy_wrap > wp.float32(0.5) * ny_f:
        y_new = y_new - ny_f
    elif dy_wrap < -wp.float32(0.5) * ny_f:
        y_new = y_new + ny_f
    if dz_wrap > wp.float32(0.5) * nz_f:
        z_new = z_new - nz_f
    elif dz_wrap < -wp.float32(0.5) * nz_f:
        z_new = z_new + nz_f

    dxp = x_new - x_old
    dyp = y_new - y_old
    dzp = z_new - z_old
    if dxp == wp.float32(0.0) and dyp == wp.float32(0.0) and dzp == wp.float32(0.0):
        return

    i_old = wp.int32(wp.floor(x_old))
    i_new = wp.int32(wp.floor(x_new))
    j_old = wp.int32(wp.floor(y_old))
    j_new = wp.int32(wp.floor(y_new))
    k_old = wp.int32(wp.floor(z_old))
    k_new = wp.int32(wp.floor(z_new))

    num_segments = wp.int32(1) + wp.abs(i_new - i_old) + wp.abs(j_new - j_old) + wp.abs(
        k_new - k_old
    )

    dir_x = wp.float32(-1.0) if dxp < wp.float32(0.0) else wp.float32(1.0)
    dir_y = wp.float32(-1.0) if dyp < wp.float32(0.0) else wp.float32(1.0)
    dir_z = wp.float32(-1.0) if dzp < wp.float32(0.0) else wp.float32(1.0)

    x_cell = wp.float32(i_old) + wp.float32(0.5) * (wp.float32(1.0) - dir_x)
    y_cell = wp.float32(j_old) + wp.float32(0.5) * (wp.float32(1.0) - dir_y)
    z_cell = wp.float32(k_old) + wp.float32(0.5) * (wp.float32(1.0) - dir_z)

    invvol = wp.float32(1.0) / (spacing[0] * spacing[1] * spacing[2])
    wq = particle_charge * particle_weight[particle_index]
    vz = (z_new - z_old) * spacing[2] / dt
    wqz = wq * vz * invvol

    x0_old = x_old
    y0_old = y_old
    z0_old = z_old

    segment_index = wp.int32(0)
    while segment_index < num_segments:
        x0_new = wp.float32(0.0)
        y0_new = wp.float32(0.0)
        z0_new = wp.float32(0.0)
        dxp_seg = wp.float32(0.0)
        dyp_seg = wp.float32(0.0)
        dzp_seg = wp.float32(0.0)

        if segment_index == (num_segments - 1):
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

            cond_x = True
            if dyp != wp.float32(0.0):
                cond_x = cond_x and (
                    wp.abs(dxp_seg) < wp.abs((dxp / dyp) * dyp_seg)
                )
            if dzp != wp.float32(0.0):
                cond_x = cond_x and (
                    wp.abs(dxp_seg) < wp.abs((dxp / dzp) * dzp_seg)
                )

            cond_y = True
            if dzp != wp.float32(0.0):
                cond_y = wp.abs(dyp_seg) < wp.abs((dyp / dzp) * dzp_seg)

            if cond_x:
                x_cell = x0_new
                if dxp != wp.float32(0.0):
                    dyp_seg = (dyp / dxp) * dxp_seg
                    dzp_seg = (dzp / dxp) * dxp_seg
                y0_new = y0_old + dyp_seg
                z0_new = z0_old + dzp_seg
            elif cond_y:
                y_cell = y0_new
                if dyp != wp.float32(0.0):
                    dxp_seg = (dxp / dyp) * dyp_seg
                    dzp_seg = (dzp / dyp) * dyp_seg
                x0_new = x0_old + dxp_seg
                z0_new = z0_old + dzp_seg
            else:
                z_cell = z0_new
                if dzp != wp.float32(0.0):
                    dxp_seg = (dxp / dzp) * dzp_seg
                    dyp_seg = (dyp / dzp) * dzp_seg
                x0_new = x0_old + dxp_seg
                y0_new = y0_old + dyp_seg

        seg_factor_z = wp.float32(1.0) if dzp == wp.float32(0.0) else dzp_seg / dzp

        sx = current_stagger[2, 0]
        sy = current_stagger[2, 1]
        sz = current_stagger[2, 2]
        z_bar = wp.float32(0.5) * (z0_old + z0_new)
        k0_cell = wp.int32(wp.floor((z_bar - sz) + wp.float32(0.5)))

        x_old_s = x0_old - sx
        x_new_s = x0_new - sx
        i0_node = wp.int32(wp.floor(wp.float32(0.5) * (x_old_s + x_new_s)))
        fx_old = x_old_s - wp.float32(i0_node)
        fx_new = x_new_s - wp.float32(i0_node)
        sx_old0 = wp.float32(1.0) - fx_old
        sx_old1 = fx_old
        sx_new0 = wp.float32(1.0) - fx_new
        sx_new1 = fx_new

        y_old_s = y0_old - sy
        y_new_s = y0_new - sy
        j0_node = wp.int32(wp.floor(wp.float32(0.5) * (y_old_s + y_new_s)))
        fy_old = y_old_s - wp.float32(j0_node)
        fy_new = y_new_s - wp.float32(j0_node)
        sy_old0 = wp.float32(1.0) - fy_old
        sy_old1 = fy_old
        sy_new0 = wp.float32(1.0) - fy_new
        sy_new1 = fy_new

        iz = _wrap_index_periodic_local(k0_cell, nz)

        i_offset = wp.int32(0)
        while i_offset < 2:
            wx_old = sx_old0 if i_offset == 0 else sx_old1
            wx_new = sx_new0 if i_offset == 0 else sx_new1
            ix = _wrap_index_periodic_local(i0_node + i_offset, nx)
            j_offset = wp.int32(0)
            while j_offset < 2:
                wy_old = sy_old0 if j_offset == 0 else sy_old1
                wy_new = sy_new0 if j_offset == 0 else sy_new1
                iy = _wrap_index_periodic_local(j0_node + j_offset, ny)
                weight_z = (
                    wx_old * wy_old / wp.float32(3.0)
                    + wx_old * wy_new / wp.float32(6.0)
                    + wx_new * wy_old / wp.float32(6.0)
                    + wx_new * wy_new / wp.float32(3.0)
                ) * seg_factor_z
                wp.atomic_add(current_density, 2, ix, iy, iz, wqz * weight_z)
                j_offset = j_offset + 1
            i_offset = i_offset + 1

        x0_old = x0_new
        y0_old = y0_new
        z0_old = z0_new
        segment_index = segment_index + 1



__all__ = [
    "_deposit_current_charge_conserving_kernel_jx_scalar_scalar",
    "_deposit_current_charge_conserving_kernel_jy_scalar_scalar",
    "_deposit_current_charge_conserving_kernel_jz_scalar_scalar",
]
