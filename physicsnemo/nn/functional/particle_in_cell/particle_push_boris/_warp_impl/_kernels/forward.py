# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import warp as wp


@wp.func
def _load_vec3(field: wp.array2d(dtype=wp.float32), particle_index: wp.int32) -> wp.vec3f:
    return wp.vec3f(
        field[particle_index, 0],
        field[particle_index, 1],
        field[particle_index, 2],
    )


@wp.func
def _store_vec3(
    field: wp.array2d(dtype=wp.float32),
    particle_index: wp.int32,
    value: wp.vec3f,
):
    field[particle_index, 0] = value[0]
    field[particle_index, 1] = value[1]
    field[particle_index, 2] = value[2]


@wp.func
def _gamma_from_momentum(momentum: wp.vec3f) -> wp.float32:
    c = wp.float32(299_792_458.0)
    return wp.sqrt(wp.float32(1.0) + wp.dot(momentum, momentum) / (c * c))


@wp.func
def _boris_momentum_update(
    momentum: wp.vec3f,
    electric: wp.vec3f,
    magnetic: wp.vec3f,
    charge_to_mass: wp.float32,
    dt: wp.float32,
) -> wp.vec3f:
    half_qmdt = wp.float32(0.5) * charge_to_mass * dt

    momentum_minus = momentum + half_qmdt * electric
    gamma_minus = _gamma_from_momentum(momentum_minus)
    t = (half_qmdt / gamma_minus) * magnetic
    s = (wp.float32(2.0) / (wp.float32(1.0) + wp.dot(t, t))) * t

    momentum_prime = momentum_minus + wp.cross(momentum_minus, t)
    momentum_plus = momentum_minus + wp.cross(momentum_prime, s)
    return momentum_plus + half_qmdt * electric


@wp.kernel
def _particle_push_boris_kernel_scalar_scalar(
    particle_position_in: wp.array2d(dtype=wp.float32),
    particle_momentum_in: wp.array2d(dtype=wp.float32),
    electric_field: wp.array2d(dtype=wp.float32),
    magnetic_field: wp.array2d(dtype=wp.float32),
    particle_position_out: wp.array2d(dtype=wp.float32),
    particle_momentum_out: wp.array2d(dtype=wp.float32),
    charge_to_mass: wp.float32,
    dt: wp.float32,
):
    particle_index = wp.tid()

    position = _load_vec3(particle_position_in, particle_index)
    momentum = _load_vec3(particle_momentum_in, particle_index)
    electric = _load_vec3(electric_field, particle_index)
    magnetic = _load_vec3(magnetic_field, particle_index)

    momentum_new = _boris_momentum_update(
        momentum=momentum,
        electric=electric,
        magnetic=magnetic,
        charge_to_mass=charge_to_mass,
        dt=dt,
    )
    gamma_new = _gamma_from_momentum(momentum_new)
    velocity_new = momentum_new / gamma_new
    position_new = position + dt * velocity_new

    _store_vec3(particle_momentum_out, particle_index, momentum_new)
    _store_vec3(particle_position_out, particle_index, position_new)
