# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warp as wp

from ._common import _sample_component_with_orders


@wp.kernel
def _gather_fields_to_particles_kernel_order1_energy_conserving(
    particle_position: wp.array2d(dtype=wp.float32),
    electric_field: wp.array4d(dtype=wp.float32),
    magnetic_field: wp.array4d(dtype=wp.float32),
    electric_stagger: wp.array2d(dtype=wp.float32),
    magnetic_stagger: wp.array2d(dtype=wp.float32),
    electric_particle: wp.array2d(dtype=wp.float32),
    magnetic_particle: wp.array2d(dtype=wp.float32),
    origin: wp.vec3f,
    spacing: wp.vec3f,
):
    particle_index = wp.tid()
    grid_x = (particle_position[particle_index, 0] - origin[0]) / spacing[0]
    grid_y = (particle_position[particle_index, 1] - origin[1]) / spacing[1]
    grid_z = (particle_position[particle_index, 2] - origin[2]) / spacing[2]

    for component in range(3):
        ex = grid_x - electric_stagger[component, 0]
        ey = grid_y - electric_stagger[component, 1]
        ez = grid_z - electric_stagger[component, 2]
        bx = grid_x - magnetic_stagger[component, 0]
        by = grid_y - magnetic_stagger[component, 1]
        bz = grid_z - magnetic_stagger[component, 2]
        order_ex = wp.int32(1)
        order_ey = wp.int32(1)
        order_ez = wp.int32(1)
        order_bx = wp.int32(0)
        order_by = wp.int32(0)
        order_bz = wp.int32(0)

        if component == 0:
            order_ex = wp.int32(0)
            order_bx = wp.int32(1)
        elif component == 1:
            order_ey = wp.int32(0)
            order_by = wp.int32(1)
        else:
            order_ez = wp.int32(0)
            order_bz = wp.int32(1)

        electric_particle[particle_index, component] = _sample_component_with_orders(
            field=electric_field,
            component=wp.int32(component),
            coord_x=ex,
            coord_y=ey,
            coord_z=ez,
            order_x=order_ex,
            order_y=order_ey,
            order_z=order_ez,
        )
        magnetic_particle[particle_index, component] = _sample_component_with_orders(
            field=magnetic_field,
            component=wp.int32(component),
            coord_x=bx,
            coord_y=by,
            coord_z=bz,
            order_x=order_bx,
            order_y=order_by,
            order_z=order_bz,
        )
