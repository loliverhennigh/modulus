# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warp as wp


@wp.func
def _wrap_index_periodic_fast(index: wp.int32, size: wp.int32) -> wp.int32:
    # Fast path for the expected local stencil offsets.
    if index >= 0 and index < size:
        return index
    if index < 0 and index >= -size:
        return index + size
    if index >= size and index < size + size:
        return index - size

    # Fallback for particles whose periodic image is multiple domains away.
    wrapped = index - (index // size) * size
    if wrapped < 0:
        wrapped = wrapped + size
    return wrapped


@wp.func
def _shape_weights_order0(
    coord: wp.float32,
) -> tuple[wp.int32, wp.float32, wp.float32, wp.float32, wp.float32, wp.int32]:
    center = wp.int32(wp.floor(coord + wp.float32(0.5)))
    return (
        center,
        wp.float32(1.0),
        wp.float32(0.0),
        wp.float32(0.0),
        wp.float32(0.0),
        wp.int32(1),
    )


@wp.func
def _shape_weights_order1(
    coord: wp.float32,
) -> tuple[wp.int32, wp.float32, wp.float32, wp.float32, wp.float32, wp.int32]:
    center = wp.int32(wp.floor(coord))
    frac = coord - wp.float32(center)
    return (
        center,
        wp.float32(1.0) - frac,
        frac,
        wp.float32(0.0),
        wp.float32(0.0),
        wp.int32(2),
    )


@wp.func
def _shape_weights_order2(
    coord: wp.float32,
) -> tuple[wp.int32, wp.float32, wp.float32, wp.float32, wp.float32, wp.int32]:
    center = wp.int32(wp.floor(coord + wp.float32(0.5)))
    frac = coord - wp.float32(center)
    weight_0 = wp.float32(0.5) * (wp.float32(0.5) - frac) * (wp.float32(0.5) - frac)
    weight_1 = wp.float32(0.75) - frac * frac
    weight_2 = wp.float32(0.5) * (wp.float32(0.5) + frac) * (wp.float32(0.5) + frac)
    return (
        center - 1,
        weight_0,
        weight_1,
        weight_2,
        wp.float32(0.0),
        wp.int32(3),
    )


@wp.func
def _shape_weights_order3(
    coord: wp.float32,
) -> tuple[wp.int32, wp.float32, wp.float32, wp.float32, wp.float32, wp.int32]:
    center = wp.int32(wp.floor(coord))
    frac = coord - wp.float32(center)
    one_minus_frac = wp.float32(1.0) - frac
    weight_0 = (
        wp.float32(1.0)
        / wp.float32(6.0)
        * one_minus_frac
        * one_minus_frac
        * one_minus_frac
    )
    weight_1 = wp.float32(2.0) / wp.float32(3.0) - frac * frac * (
        wp.float32(1.0) - wp.float32(0.5) * frac
    )
    weight_2 = wp.float32(2.0) / wp.float32(3.0) - one_minus_frac * one_minus_frac * (
        wp.float32(1.0) - wp.float32(0.5) * one_minus_frac
    )
    weight_3 = (
        wp.float32(1.0) / wp.float32(6.0) * frac * frac * frac
    )
    return (center - 1, weight_0, weight_1, weight_2, weight_3, wp.int32(4))


@wp.func
def _weight_at(
    index: wp.int32,
    weight_0: wp.float32,
    weight_1: wp.float32,
    weight_2: wp.float32,
    weight_3: wp.float32,
) -> wp.float32:
    if index == 0:
        return weight_0
    if index == 1:
        return weight_1
    if index == 2:
        return weight_2
    return weight_3


@wp.func
def _sample_component_with_orders(
    field: wp.array4d(dtype=wp.float32),
    component: wp.int32,
    coord_x: wp.float32,
    coord_y: wp.float32,
    coord_z: wp.float32,
    order_x: wp.int32,
    order_y: wp.int32,
    order_z: wp.int32,
) -> wp.float32:
    nx = wp.int32(field.shape[1])
    ny = wp.int32(field.shape[2])
    nz = wp.int32(field.shape[3])

    base_x = wp.int32(0)
    base_y = wp.int32(0)
    base_z = wp.int32(0)
    wx0 = wp.float32(0.0)
    wx1 = wp.float32(0.0)
    wx2 = wp.float32(0.0)
    wx3 = wp.float32(0.0)
    wy0 = wp.float32(0.0)
    wy1 = wp.float32(0.0)
    wy2 = wp.float32(0.0)
    wy3 = wp.float32(0.0)
    wz0 = wp.float32(0.0)
    wz1 = wp.float32(0.0)
    wz2 = wp.float32(0.0)
    wz3 = wp.float32(0.0)
    support_x = wp.int32(1)
    support_y = wp.int32(1)
    support_z = wp.int32(1)

    if order_x == 0:
        base_x, wx0, wx1, wx2, wx3, support_x = _shape_weights_order0(coord_x)
    elif order_x == 1:
        base_x, wx0, wx1, wx2, wx3, support_x = _shape_weights_order1(coord_x)
    elif order_x == 2:
        base_x, wx0, wx1, wx2, wx3, support_x = _shape_weights_order2(coord_x)
    else:
        base_x, wx0, wx1, wx2, wx3, support_x = _shape_weights_order3(coord_x)

    if order_y == 0:
        base_y, wy0, wy1, wy2, wy3, support_y = _shape_weights_order0(coord_y)
    elif order_y == 1:
        base_y, wy0, wy1, wy2, wy3, support_y = _shape_weights_order1(coord_y)
    elif order_y == 2:
        base_y, wy0, wy1, wy2, wy3, support_y = _shape_weights_order2(coord_y)
    else:
        base_y, wy0, wy1, wy2, wy3, support_y = _shape_weights_order3(coord_y)

    if order_z == 0:
        base_z, wz0, wz1, wz2, wz3, support_z = _shape_weights_order0(coord_z)
    elif order_z == 1:
        base_z, wz0, wz1, wz2, wz3, support_z = _shape_weights_order1(coord_z)
    elif order_z == 2:
        base_z, wz0, wz1, wz2, wz3, support_z = _shape_weights_order2(coord_z)
    else:
        base_z, wz0, wz1, wz2, wz3, support_z = _shape_weights_order3(coord_z)

    accumulated = wp.float32(0.0)
    for iz in range(4):
        if iz >= support_z:
            continue
        z_index = _wrap_index_periodic_fast(base_z + iz, nz)
        weight_z = _weight_at(iz, wz0, wz1, wz2, wz3)
        for iy in range(4):
            if iy >= support_y:
                continue
            y_index = _wrap_index_periodic_fast(base_y + iy, ny)
            weight_y = _weight_at(iy, wy0, wy1, wy2, wy3)
            for ix in range(4):
                if ix >= support_x:
                    continue
                x_index = _wrap_index_periodic_fast(base_x + ix, nx)
                weight_x = _weight_at(ix, wx0, wx1, wx2, wx3)
                accumulated = accumulated + (
                    weight_x
                    * weight_y
                    * weight_z
                    * field[component, x_index, y_index, z_index]
                )

    return accumulated


__all__ = ["_sample_component_with_orders"]
