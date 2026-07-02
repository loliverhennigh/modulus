# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Warp kernels for dense displacement and compact Shepard morphing."""

import warp as wp


@wp.func
def _normalized_component_f32(
    point: wp.float32,
    control: wp.float32,
    radius: wp.float32,
) -> wp.float32:
    delta = point - control
    value = delta / radius
    if not wp.isfinite(delta):
        value = point / radius - control / radius
    if wp.isnan(value):
        value = wp.float32(1.0)
    return wp.clamp(value, wp.float32(-1.0), wp.float32(1.0))


@wp.func
def _normalized_distance_f32(
    points: wp.array3d(dtype=wp.float32),
    controls: wp.array3d(dtype=wp.float32),
    radii: wp.array2d(dtype=wp.float32),
    b: int,
    i: int,
    j: int,
    n_dims: int,
) -> wp.float32:
    radius = radii[b, j]
    maximum = wp.float32(0.0)
    for d in range(n_dims):
        value = _normalized_component_f32(points[b, i, d], controls[b, j, d], radius)
        maximum = wp.max(maximum, wp.abs(value))
    if maximum == wp.float32(0.0):
        return wp.float32(0.0)
    norm_squared = wp.float32(0.0)
    for d in range(n_dims):
        value = _normalized_component_f32(points[b, i, d], controls[b, j, d], radius)
        scaled = value / maximum
        norm_squared = norm_squared + scaled * scaled
    return maximum * wp.sqrt(norm_squared)


@wp.func
def _normalized_component_f64(
    point: wp.float64,
    control: wp.float64,
    radius: wp.float64,
) -> wp.float64:
    delta = point - control
    value = delta / radius
    if not wp.isfinite(delta):
        value = point / radius - control / radius
    if wp.isnan(value):
        value = wp.float64(1.0)
    return wp.clamp(value, wp.float64(-1.0), wp.float64(1.0))


@wp.func
def _normalized_distance_f64(
    points: wp.array3d(dtype=wp.float64),
    controls: wp.array3d(dtype=wp.float64),
    radii: wp.array2d(dtype=wp.float64),
    b: int,
    i: int,
    j: int,
    n_dims: int,
) -> wp.float64:
    radius = radii[b, j]
    maximum = wp.float64(0.0)
    for d in range(n_dims):
        value = _normalized_component_f64(points[b, i, d], controls[b, j, d], radius)
        maximum = wp.max(maximum, wp.abs(value))
    if maximum == wp.float64(0.0):
        return wp.float64(0.0)
    norm_squared = wp.float64(0.0)
    for d in range(n_dims):
        value = _normalized_component_f64(points[b, i, d], controls[b, j, d], radius)
        scaled = value / maximum
        norm_squared = norm_squared + scaled * scaled
    return maximum * wp.sqrt(norm_squared)


@wp.kernel
def displace_forward_f32(
    points: wp.array3d(dtype=wp.float32),
    displacement: wp.array3d(dtype=wp.float32),
    amount: wp.array(dtype=wp.float32),
    weights: wp.array2d(dtype=wp.float32),
    has_weights: int,
    output: wp.array3d(dtype=wp.float32),
):
    """Apply weighted dense displacement in single precision."""

    b, i, d = wp.tid()
    weight = wp.float32(1.0)
    if has_weights != 0:
        weight = weights[b, i]
    output[b, i, d] = points[b, i, d] + amount[0] * weight * displacement[b, i, d]


@wp.kernel
def displace_backward_f32(
    displacement: wp.array3d(dtype=wp.float32),
    amount: wp.array(dtype=wp.float32),
    weights: wp.array2d(dtype=wp.float32),
    grad_output: wp.array3d(dtype=wp.float32),
    grad_points: wp.array3d(dtype=wp.float32),
    grad_displacement: wp.array3d(dtype=wp.float32),
    grad_amount: wp.array(dtype=wp.float32),
    grad_weights: wp.array2d(dtype=wp.float32),
    n_dims: int,
    has_weights: int,
    need_points: int,
    need_displacement: int,
    need_amount: int,
    need_weights: int,
):
    """Compute the weighted dense-displacement pullback in single precision."""

    b, i = wp.tid()
    w = wp.float32(1.0)
    if has_weights != 0:
        w = weights[b, i]
    scale = amount[0]
    amount_sum = wp.float32(0.0)
    weight_sum = wp.float32(0.0)
    for d in range(n_dims):
        g = grad_output[b, i, d]
        if need_points != 0:
            grad_points[b, i, d] = g
        if need_displacement != 0:
            grad_displacement[b, i, d] = g * scale * w
        if need_amount != 0 or need_weights != 0:
            disp = displacement[b, i, d]
            if need_amount != 0:
                amount_sum = amount_sum + g * w * disp
            if need_weights != 0:
                weight_sum = weight_sum + g * scale * disp
    if need_amount != 0:
        wp.atomic_add(grad_amount, 0, amount_sum)
    if need_weights != 0:
        grad_weights[b, i] = weight_sum


@wp.kernel
def displace_forward_f64(
    points: wp.array3d(dtype=wp.float64),
    displacement: wp.array3d(dtype=wp.float64),
    amount: wp.array(dtype=wp.float64),
    weights: wp.array2d(dtype=wp.float64),
    has_weights: int,
    output: wp.array3d(dtype=wp.float64),
):
    """Apply weighted dense displacement in double precision."""

    b, i, d = wp.tid()
    weight = wp.float64(1.0)
    if has_weights != 0:
        weight = weights[b, i]
    output[b, i, d] = points[b, i, d] + amount[0] * weight * displacement[b, i, d]


@wp.kernel
def displace_backward_f64(
    displacement: wp.array3d(dtype=wp.float64),
    amount: wp.array(dtype=wp.float64),
    weights: wp.array2d(dtype=wp.float64),
    grad_output: wp.array3d(dtype=wp.float64),
    grad_points: wp.array3d(dtype=wp.float64),
    grad_displacement: wp.array3d(dtype=wp.float64),
    grad_amount: wp.array(dtype=wp.float64),
    grad_weights: wp.array2d(dtype=wp.float64),
    n_dims: int,
    has_weights: int,
    need_points: int,
    need_displacement: int,
    need_amount: int,
    need_weights: int,
):
    """Compute the weighted dense-displacement pullback in double precision."""

    b, i = wp.tid()
    w = wp.float64(1.0)
    if has_weights != 0:
        w = weights[b, i]
    scale = amount[0]
    amount_sum = wp.float64(0.0)
    weight_sum = wp.float64(0.0)
    for d in range(n_dims):
        g = grad_output[b, i, d]
        if need_points != 0:
            grad_points[b, i, d] = g
        if need_displacement != 0:
            grad_displacement[b, i, d] = g * scale * w
        if need_amount != 0 or need_weights != 0:
            disp = displacement[b, i, d]
            if need_amount != 0:
                amount_sum = amount_sum + g * w * disp
            if need_weights != 0:
                weight_sum = weight_sum + g * scale * disp
    if need_amount != 0:
        wp.atomic_add(grad_amount, 0, amount_sum)
    if need_weights != 0:
        grad_weights[b, i] = weight_sum


@wp.kernel
def displace_masked_forward_f32(
    points: wp.array3d(dtype=wp.float32),
    displacement: wp.array3d(dtype=wp.float32),
    amount: wp.array(dtype=wp.float32),
    mask: wp.array2d(dtype=wp.bool),
    output: wp.array3d(dtype=wp.float32),
):
    """Apply masked dense displacement in single precision."""

    b, i, d = wp.tid()
    if mask[b, i]:
        output[b, i, d] = points[b, i, d] + amount[0] * displacement[b, i, d]
    else:
        output[b, i, d] = points[b, i, d]


@wp.kernel
def displace_masked_forward_f64(
    points: wp.array3d(dtype=wp.float64),
    displacement: wp.array3d(dtype=wp.float64),
    amount: wp.array(dtype=wp.float64),
    mask: wp.array2d(dtype=wp.bool),
    output: wp.array3d(dtype=wp.float64),
):
    """Apply masked dense displacement in double precision."""

    b, i, d = wp.tid()
    if mask[b, i]:
        output[b, i, d] = points[b, i, d] + amount[0] * displacement[b, i, d]
    else:
        output[b, i, d] = points[b, i, d]


@wp.kernel
def displace_masked_backward_f32(
    displacement: wp.array3d(dtype=wp.float32),
    amount: wp.array(dtype=wp.float32),
    mask: wp.array2d(dtype=wp.bool),
    grad_output: wp.array3d(dtype=wp.float32),
    grad_points: wp.array3d(dtype=wp.float32),
    grad_displacement: wp.array3d(dtype=wp.float32),
    grad_amount: wp.array(dtype=wp.float32),
    n_dims: int,
    need_points: int,
    need_displacement: int,
    need_amount: int,
):
    """Compute the masked dense-displacement pullback in single precision."""

    b, i = wp.tid()
    active = mask[b, i]
    amount_sum = wp.float32(0.0)
    for d in range(n_dims):
        g = grad_output[b, i, d]
        if need_points != 0:
            grad_points[b, i, d] = g
        if need_displacement != 0:
            if active:
                grad_displacement[b, i, d] = g * amount[0]
            else:
                grad_displacement[b, i, d] = wp.float32(0.0)
        if need_amount != 0 and active:
            amount_sum = amount_sum + g * displacement[b, i, d]
    if need_amount != 0:
        wp.atomic_add(grad_amount, 0, amount_sum)


@wp.kernel
def displace_masked_backward_f64(
    displacement: wp.array3d(dtype=wp.float64),
    amount: wp.array(dtype=wp.float64),
    mask: wp.array2d(dtype=wp.bool),
    grad_output: wp.array3d(dtype=wp.float64),
    grad_points: wp.array3d(dtype=wp.float64),
    grad_displacement: wp.array3d(dtype=wp.float64),
    grad_amount: wp.array(dtype=wp.float64),
    n_dims: int,
    need_points: int,
    need_displacement: int,
    need_amount: int,
):
    """Compute the masked dense-displacement pullback in double precision."""

    b, i = wp.tid()
    active = mask[b, i]
    amount_sum = wp.float64(0.0)
    for d in range(n_dims):
        g = grad_output[b, i, d]
        if need_points != 0:
            grad_points[b, i, d] = g
        if need_displacement != 0:
            if active:
                grad_displacement[b, i, d] = g * amount[0]
            else:
                grad_displacement[b, i, d] = wp.float64(0.0)
        if need_amount != 0 and active:
            amount_sum = amount_sum + g * displacement[b, i, d]
    if need_amount != 0:
        wp.atomic_add(grad_amount, 0, amount_sum)


@wp.kernel
def shepard_forward_f32(
    points: wp.array3d(dtype=wp.float32),
    controls: wp.array3d(dtype=wp.float32),
    control_displacements: wp.array3d(dtype=wp.float32),
    radii: wp.array2d(dtype=wp.float32),
    n_controls: int,
    n_dims: int,
    save_auxiliaries: int,
    save_correction: int,
    field: wp.array3d(dtype=wp.float32),
    min_q: wp.array2d(dtype=wp.float32),
    denominator: wp.array2d(dtype=wp.float32),
    exact_count_out: wp.array2d(dtype=wp.int32),
    reference_index_out: wp.array2d(dtype=wp.int32),
    correction: wp.array3d(dtype=wp.float32),
):
    """Interpolate a compact Shepard displacement field in single precision."""

    b, i = wp.tid()
    exact_count = int(0)
    minimum = wp.float32(3.402823466e38)
    reference_index = int(-1)

    for j in range(n_controls):
        q = _normalized_distance_f32(points, controls, radii, b, i, j, n_dims)
        if q == wp.float32(0.0):
            exact_count = exact_count + 1
        elif q < wp.float32(1.0) and q < minimum:
            minimum = q
            reference_index = j

    if save_auxiliaries != 0:
        exact_count_out[b, i] = exact_count
        reference_index_out[b, i] = reference_index
    if exact_count > 0:
        inv_count = wp.float32(1.0) / wp.float32(exact_count)
        for d in range(n_dims):
            field[b, i, d] = wp.float32(0.0)
            if save_correction != 0:
                correction[b, i, d] = wp.float32(0.0)
        for j in range(n_controls):
            q = _normalized_distance_f32(points, controls, radii, b, i, j, n_dims)
            if q == wp.float32(0.0):
                for d in range(n_dims):
                    field[b, i, d] = field[b, i, d] + control_displacements[b, j, d]
        for d in range(n_dims):
            field[b, i, d] = field[b, i, d] * inv_count
        if save_auxiliaries != 0:
            min_q[b, i] = wp.float32(1.0)
            denominator[b, i] = wp.float32(exact_count)
            reference_index_out[b, i] = int(-1)
        return

    if minimum == wp.float32(3.402823466e38):
        if save_auxiliaries != 0:
            min_q[b, i] = wp.float32(1.0)
            denominator[b, i] = wp.float32(1.0)
        for d in range(n_dims):
            field[b, i, d] = wp.float32(0.0)
            if save_correction != 0:
                correction[b, i, d] = wp.float32(0.0)
        return

    # Multiplying every handle weight and the stationary background by the
    # same minimum q^2 keeps the quotient unchanged. Evaluating the handle
    # ratio as (minimum_q / q)^2 avoids overflow and q^2 underflow.
    if save_auxiliaries != 0:
        min_q[b, i] = minimum
    reference_t = wp.float32(1.0) - minimum
    reference_phi = (
        reference_t
        * reference_t
        * reference_t
        * reference_t
        * (wp.float32(4.0) * minimum + wp.float32(1.0))
    )
    background = minimum * minimum / reference_phi
    denom = background
    for d in range(n_dims):
        value = -background * control_displacements[b, reference_index, d]
        if save_correction != 0:
            correction[b, i, d] = value
        else:
            # When geometry gradients are unnecessary, field doubles as
            # per-query scratch so no correction tensor needs to be allocated.
            field[b, i, d] = value

    for j in range(n_controls):
        q = _normalized_distance_f32(points, controls, radii, b, i, j, n_dims)
        if q > wp.float32(0.0) and q < wp.float32(1.0):
            t = wp.float32(1.0) - q
            phi = t * t * t * t * (wp.float32(4.0) * q + wp.float32(1.0))
            ratio = minimum / q
            a = ratio * ratio * phi / reference_phi
            denom = denom + a
            if j != reference_index:
                for d in range(n_dims):
                    value = a * (
                        control_displacements[b, j, d]
                        - control_displacements[b, reference_index, d]
                    )
                    if save_correction != 0:
                        correction[b, i, d] = correction[b, i, d] + value
                    else:
                        field[b, i, d] = field[b, i, d] + value

    if save_auxiliaries != 0:
        denominator[b, i] = denom
    for d in range(n_dims):
        if save_correction != 0:
            correction[b, i, d] = correction[b, i, d] / denom
            field[b, i, d] = (
                control_displacements[b, reference_index, d] + correction[b, i, d]
            )
        else:
            field[b, i, d] = (
                control_displacements[b, reference_index, d] + field[b, i, d] / denom
            )


@wp.kernel
def shepard_backward_f32(
    points: wp.array3d(dtype=wp.float32),
    controls: wp.array3d(dtype=wp.float32),
    control_displacements: wp.array3d(dtype=wp.float32),
    radii: wp.array2d(dtype=wp.float32),
    min_q: wp.array2d(dtype=wp.float32),
    denominator: wp.array2d(dtype=wp.float32),
    exact_count: wp.array2d(dtype=wp.int32),
    reference_index: wp.array2d(dtype=wp.int32),
    correction: wp.array3d(dtype=wp.float32),
    grad_field: wp.array3d(dtype=wp.float32),
    n_dims: int,
    need_points: int,
    need_controls: int,
    need_control_displacements: int,
    need_radii: int,
    grad_points: wp.array3d(dtype=wp.float32),
    grad_controls: wp.array3d(dtype=wp.float32),
    grad_control_displacements: wp.array3d(dtype=wp.float32),
    grad_radii: wp.array2d(dtype=wp.float32),
):
    """Accumulate the control-centric Shepard pullback in single precision."""

    b, i, j = wp.tid()
    q = _normalized_distance_f32(points, controls, radii, b, i, j, n_dims)
    coincident = q == wp.float32(0.0)

    count = exact_count[b, i]
    if count > 0:
        if need_control_displacements != 0 and coincident:
            inv_count = wp.float32(1.0) / wp.float32(count)
            for d in range(n_dims):
                wp.atomic_add(
                    grad_control_displacements,
                    b,
                    j,
                    d,
                    grad_field[b, i, d] * inv_count,
                )
        return

    if coincident or q >= wp.float32(1.0):
        return
    radius = radii[b, j]
    t = wp.float32(1.0) - q
    phi = t * t * t * t * (wp.float32(4.0) * q + wp.float32(1.0))
    minimum = min_q[b, i]
    denom = denominator[b, i]
    reference_t = wp.float32(1.0) - minimum
    reference_phi = (
        reference_t
        * reference_t
        * reference_t
        * reference_t
        * (wp.float32(4.0) * minimum + wp.float32(1.0))
    )
    scaled_denom = reference_phi * denom
    ratio = minimum / q
    ratio_squared = ratio * ratio
    a = ratio_squared * phi

    ref = reference_index[b, i]
    if need_control_displacements != 0:
        for d in range(n_dims):
            wp.atomic_add(
                grad_control_displacements,
                b,
                j,
                d,
                (a / scaled_denom) * grad_field[b, i, d],
            )

    if need_points == 0 and need_controls == 0 and need_radii == 0:
        return

    phi_prime = -wp.float32(20.0) * q * t * t * t
    base_dot = wp.float32(0.0)
    correction_dot = wp.float32(0.0)
    reference_dot = wp.float32(0.0)
    for d in range(n_dims):
        g = grad_field[b, i, d]
        base_dot = base_dot + g * (
            control_displacements[b, j, d] - control_displacements[b, ref, d]
        )
        correction_dot = correction_dot + g * correction[b, i, d]
        reference_dot = reference_dot + g * control_displacements[b, ref, d]

    dot = base_dot - correction_dot
    d_a_d_q = ratio_squared * (phi_prime - wp.float32(2.0) * phi / q)
    q_d_a_d_q = ratio_squared * (q * phi_prime - wp.float32(2.0) * phi)
    minimum_d_a_d_q = ratio_squared * (
        minimum * phi_prime - wp.float32(2.0) * phi * ratio
    )
    gamma = wp.float32(0.0)
    q_gamma = wp.float32(0.0)
    if dot != wp.float32(0.0):
        gamma = (dot / scaled_denom) * d_a_d_q
        q_gamma = (dot / scaled_denom) * q_d_a_d_q
    elif j == ref and minimum * minimum == wp.float32(0.0):
        gamma = (
            reference_dot * minimum * minimum_d_a_d_q / (scaled_denom * scaled_denom)
        )
        q_gamma = (
            reference_dot
            * minimum
            * (q * minimum_d_a_d_q)
            / (scaled_denom * scaled_denom)
        )

    if need_points != 0 or need_controls != 0:
        for d in range(n_dims):
            normalized_delta = _normalized_component_f32(
                points[b, i, d], controls[b, j, d], radius
            )
            value = wp.float32(0.0)
            if normalized_delta != wp.float32(0.0):
                value = (gamma / radius) * (normalized_delta / q)
            if need_points != 0:
                wp.atomic_add(grad_points, b, i, d, value)
            if need_controls != 0:
                wp.atomic_sub(grad_controls, b, j, d, value)
    if need_radii != 0:
        wp.atomic_add(grad_radii, b, j, -q_gamma / radius)


@wp.kernel
def shepard_point_backward_f32(
    points: wp.array3d(dtype=wp.float32),
    controls: wp.array3d(dtype=wp.float32),
    control_displacements: wp.array3d(dtype=wp.float32),
    radii: wp.array2d(dtype=wp.float32),
    min_q: wp.array2d(dtype=wp.float32),
    denominator: wp.array2d(dtype=wp.float32),
    exact_count: wp.array2d(dtype=wp.int32),
    reference_index: wp.array2d(dtype=wp.int32),
    correction: wp.array3d(dtype=wp.float32),
    grad_field: wp.array3d(dtype=wp.float32),
    n_controls: int,
    n_dims: int,
    grad_points: wp.array3d(dtype=wp.float32),
):
    """Query-centric point pullback with no inter-control atomics."""

    b, i = wp.tid()
    for d in range(n_dims):
        grad_points[b, i, d] = wp.float32(0.0)
    if exact_count[b, i] > 0:
        return

    minimum = min_q[b, i]
    denom = denominator[b, i]
    reference_t = wp.float32(1.0) - minimum
    reference_phi = (
        reference_t
        * reference_t
        * reference_t
        * reference_t
        * (wp.float32(4.0) * minimum + wp.float32(1.0))
    )
    scaled_denom = reference_phi * denom
    ref = reference_index[b, i]

    for j in range(n_controls):
        q = _normalized_distance_f32(points, controls, radii, b, i, j, n_dims)
        coincident = q == wp.float32(0.0)
        if not coincident and q < wp.float32(1.0):
            radius = radii[b, j]
            t = wp.float32(1.0) - q
            phi = t * t * t * t * (wp.float32(4.0) * q + wp.float32(1.0))
            phi_prime = -wp.float32(20.0) * q * t * t * t
            ratio = minimum / q
            ratio_squared = ratio * ratio
            base_dot = wp.float32(0.0)
            correction_dot = wp.float32(0.0)
            reference_dot = wp.float32(0.0)
            for d in range(n_dims):
                g = grad_field[b, i, d]
                base_dot = base_dot + g * (
                    control_displacements[b, j, d] - control_displacements[b, ref, d]
                )
                correction_dot = correction_dot + g * correction[b, i, d]
                reference_dot = reference_dot + g * control_displacements[b, ref, d]
            dot = base_dot - correction_dot
            d_a_d_q = ratio_squared * (phi_prime - wp.float32(2.0) * phi / q)
            minimum_d_a_d_q = ratio_squared * (
                minimum * phi_prime - wp.float32(2.0) * phi * ratio
            )
            gamma = wp.float32(0.0)
            if dot != wp.float32(0.0):
                gamma = (dot / scaled_denom) * d_a_d_q
            elif j == ref and minimum * minimum == wp.float32(0.0):
                gamma = (
                    reference_dot
                    * minimum
                    * minimum_d_a_d_q
                    / (scaled_denom * scaled_denom)
                )
            for d in range(n_dims):
                normalized_delta = _normalized_component_f32(
                    points[b, i, d], controls[b, j, d], radius
                )
                if normalized_delta != wp.float32(0.0):
                    grad_points[b, i, d] = grad_points[b, i, d] + (
                        (gamma / radius) * (normalized_delta / q)
                    )


@wp.kernel
def shepard_forward_f64(
    points: wp.array3d(dtype=wp.float64),
    controls: wp.array3d(dtype=wp.float64),
    control_displacements: wp.array3d(dtype=wp.float64),
    radii: wp.array2d(dtype=wp.float64),
    n_controls: int,
    n_dims: int,
    save_auxiliaries: int,
    save_correction: int,
    field: wp.array3d(dtype=wp.float64),
    min_q: wp.array2d(dtype=wp.float64),
    denominator: wp.array2d(dtype=wp.float64),
    exact_count_out: wp.array2d(dtype=wp.int32),
    reference_index_out: wp.array2d(dtype=wp.int32),
    correction: wp.array3d(dtype=wp.float64),
):
    """Interpolate a compact Shepard displacement field in double precision."""

    b, i = wp.tid()
    exact_count = int(0)
    minimum = wp.float64(1.7976931348623157e308)
    reference_index = int(-1)

    for j in range(n_controls):
        q = _normalized_distance_f64(points, controls, radii, b, i, j, n_dims)
        if q == wp.float64(0.0):
            exact_count = exact_count + 1
        elif q < wp.float64(1.0) and q < minimum:
            minimum = q
            reference_index = j

    if save_auxiliaries != 0:
        exact_count_out[b, i] = exact_count
        reference_index_out[b, i] = reference_index
    if exact_count > 0:
        inv_count = wp.float64(1.0) / wp.float64(exact_count)
        for d in range(n_dims):
            field[b, i, d] = wp.float64(0.0)
            if save_correction != 0:
                correction[b, i, d] = wp.float64(0.0)
        for j in range(n_controls):
            q = _normalized_distance_f64(points, controls, radii, b, i, j, n_dims)
            if q == wp.float64(0.0):
                for d in range(n_dims):
                    field[b, i, d] = field[b, i, d] + control_displacements[b, j, d]
        for d in range(n_dims):
            field[b, i, d] = field[b, i, d] * inv_count
        if save_auxiliaries != 0:
            min_q[b, i] = wp.float64(1.0)
            denominator[b, i] = wp.float64(exact_count)
            reference_index_out[b, i] = int(-1)
        return

    if minimum == wp.float64(1.7976931348623157e308):
        if save_auxiliaries != 0:
            min_q[b, i] = wp.float64(1.0)
            denominator[b, i] = wp.float64(1.0)
        for d in range(n_dims):
            field[b, i, d] = wp.float64(0.0)
            if save_correction != 0:
                correction[b, i, d] = wp.float64(0.0)
        return

    if save_auxiliaries != 0:
        min_q[b, i] = minimum
    reference_t = wp.float64(1.0) - minimum
    reference_phi = (
        reference_t
        * reference_t
        * reference_t
        * reference_t
        * (wp.float64(4.0) * minimum + wp.float64(1.0))
    )
    background = minimum * minimum / reference_phi
    denom = background
    for d in range(n_dims):
        value = -background * control_displacements[b, reference_index, d]
        if save_correction != 0:
            correction[b, i, d] = value
        else:
            field[b, i, d] = value

    for j in range(n_controls):
        q = _normalized_distance_f64(points, controls, radii, b, i, j, n_dims)
        if q > wp.float64(0.0) and q < wp.float64(1.0):
            t = wp.float64(1.0) - q
            phi = t * t * t * t * (wp.float64(4.0) * q + wp.float64(1.0))
            ratio = minimum / q
            a = ratio * ratio * phi / reference_phi
            denom = denom + a
            if j != reference_index:
                for d in range(n_dims):
                    value = a * (
                        control_displacements[b, j, d]
                        - control_displacements[b, reference_index, d]
                    )
                    if save_correction != 0:
                        correction[b, i, d] = correction[b, i, d] + value
                    else:
                        field[b, i, d] = field[b, i, d] + value

    if save_auxiliaries != 0:
        denominator[b, i] = denom
    for d in range(n_dims):
        if save_correction != 0:
            correction[b, i, d] = correction[b, i, d] / denom
            field[b, i, d] = (
                control_displacements[b, reference_index, d] + correction[b, i, d]
            )
        else:
            field[b, i, d] = (
                control_displacements[b, reference_index, d] + field[b, i, d] / denom
            )


@wp.kernel
def shepard_backward_f64(
    points: wp.array3d(dtype=wp.float64),
    controls: wp.array3d(dtype=wp.float64),
    control_displacements: wp.array3d(dtype=wp.float64),
    radii: wp.array2d(dtype=wp.float64),
    min_q: wp.array2d(dtype=wp.float64),
    denominator: wp.array2d(dtype=wp.float64),
    exact_count: wp.array2d(dtype=wp.int32),
    reference_index: wp.array2d(dtype=wp.int32),
    correction: wp.array3d(dtype=wp.float64),
    grad_field: wp.array3d(dtype=wp.float64),
    n_dims: int,
    need_points: int,
    need_controls: int,
    need_control_displacements: int,
    need_radii: int,
    grad_points: wp.array3d(dtype=wp.float64),
    grad_controls: wp.array3d(dtype=wp.float64),
    grad_control_displacements: wp.array3d(dtype=wp.float64),
    grad_radii: wp.array2d(dtype=wp.float64),
):
    """Accumulate the control-centric Shepard pullback in double precision."""

    b, i, j = wp.tid()
    q = _normalized_distance_f64(points, controls, radii, b, i, j, n_dims)
    coincident = q == wp.float64(0.0)

    count = exact_count[b, i]
    if count > 0:
        if need_control_displacements != 0 and coincident:
            inv_count = wp.float64(1.0) / wp.float64(count)
            for d in range(n_dims):
                wp.atomic_add(
                    grad_control_displacements,
                    b,
                    j,
                    d,
                    grad_field[b, i, d] * inv_count,
                )
        return

    if coincident or q >= wp.float64(1.0):
        return
    radius = radii[b, j]
    t = wp.float64(1.0) - q
    phi = t * t * t * t * (wp.float64(4.0) * q + wp.float64(1.0))
    minimum = min_q[b, i]
    denom = denominator[b, i]
    reference_t = wp.float64(1.0) - minimum
    reference_phi = (
        reference_t
        * reference_t
        * reference_t
        * reference_t
        * (wp.float64(4.0) * minimum + wp.float64(1.0))
    )
    scaled_denom = reference_phi * denom
    ratio = minimum / q
    ratio_squared = ratio * ratio
    a = ratio_squared * phi

    ref = reference_index[b, i]
    if need_control_displacements != 0:
        for d in range(n_dims):
            wp.atomic_add(
                grad_control_displacements,
                b,
                j,
                d,
                (a / scaled_denom) * grad_field[b, i, d],
            )

    if need_points == 0 and need_controls == 0 and need_radii == 0:
        return

    phi_prime = -wp.float64(20.0) * q * t * t * t
    base_dot = wp.float64(0.0)
    correction_dot = wp.float64(0.0)
    reference_dot = wp.float64(0.0)
    for d in range(n_dims):
        g = grad_field[b, i, d]
        base_dot = base_dot + g * (
            control_displacements[b, j, d] - control_displacements[b, ref, d]
        )
        correction_dot = correction_dot + g * correction[b, i, d]
        reference_dot = reference_dot + g * control_displacements[b, ref, d]

    dot = base_dot - correction_dot
    d_a_d_q = ratio_squared * (phi_prime - wp.float64(2.0) * phi / q)
    q_d_a_d_q = ratio_squared * (q * phi_prime - wp.float64(2.0) * phi)
    minimum_d_a_d_q = ratio_squared * (
        minimum * phi_prime - wp.float64(2.0) * phi * ratio
    )
    gamma = wp.float64(0.0)
    q_gamma = wp.float64(0.0)
    if dot != wp.float64(0.0):
        gamma = (dot / scaled_denom) * d_a_d_q
        q_gamma = (dot / scaled_denom) * q_d_a_d_q
    elif j == ref and minimum * minimum == wp.float64(0.0):
        gamma = (
            reference_dot * minimum * minimum_d_a_d_q / (scaled_denom * scaled_denom)
        )
        q_gamma = (
            reference_dot
            * minimum
            * (q * minimum_d_a_d_q)
            / (scaled_denom * scaled_denom)
        )

    if need_points != 0 or need_controls != 0:
        for d in range(n_dims):
            normalized_delta = _normalized_component_f64(
                points[b, i, d], controls[b, j, d], radius
            )
            value = wp.float64(0.0)
            if normalized_delta != wp.float64(0.0):
                value = (gamma / radius) * (normalized_delta / q)
            if need_points != 0:
                wp.atomic_add(grad_points, b, i, d, value)
            if need_controls != 0:
                wp.atomic_sub(grad_controls, b, j, d, value)
    if need_radii != 0:
        wp.atomic_add(grad_radii, b, j, -q_gamma / radius)


@wp.kernel
def shepard_point_backward_f64(
    points: wp.array3d(dtype=wp.float64),
    controls: wp.array3d(dtype=wp.float64),
    control_displacements: wp.array3d(dtype=wp.float64),
    radii: wp.array2d(dtype=wp.float64),
    min_q: wp.array2d(dtype=wp.float64),
    denominator: wp.array2d(dtype=wp.float64),
    exact_count: wp.array2d(dtype=wp.int32),
    reference_index: wp.array2d(dtype=wp.int32),
    correction: wp.array3d(dtype=wp.float64),
    grad_field: wp.array3d(dtype=wp.float64),
    n_controls: int,
    n_dims: int,
    grad_points: wp.array3d(dtype=wp.float64),
):
    """Query-centric point pullback with no inter-control atomics."""

    b, i = wp.tid()
    for d in range(n_dims):
        grad_points[b, i, d] = wp.float64(0.0)
    if exact_count[b, i] > 0:
        return

    minimum = min_q[b, i]
    denom = denominator[b, i]
    reference_t = wp.float64(1.0) - minimum
    reference_phi = (
        reference_t
        * reference_t
        * reference_t
        * reference_t
        * (wp.float64(4.0) * minimum + wp.float64(1.0))
    )
    scaled_denom = reference_phi * denom
    ref = reference_index[b, i]

    for j in range(n_controls):
        q = _normalized_distance_f64(points, controls, radii, b, i, j, n_dims)
        coincident = q == wp.float64(0.0)
        if not coincident and q < wp.float64(1.0):
            radius = radii[b, j]
            t = wp.float64(1.0) - q
            phi = t * t * t * t * (wp.float64(4.0) * q + wp.float64(1.0))
            phi_prime = -wp.float64(20.0) * q * t * t * t
            ratio = minimum / q
            ratio_squared = ratio * ratio
            base_dot = wp.float64(0.0)
            correction_dot = wp.float64(0.0)
            reference_dot = wp.float64(0.0)
            for d in range(n_dims):
                g = grad_field[b, i, d]
                base_dot = base_dot + g * (
                    control_displacements[b, j, d] - control_displacements[b, ref, d]
                )
                correction_dot = correction_dot + g * correction[b, i, d]
                reference_dot = reference_dot + g * control_displacements[b, ref, d]
            dot = base_dot - correction_dot
            d_a_d_q = ratio_squared * (phi_prime - wp.float64(2.0) * phi / q)
            minimum_d_a_d_q = ratio_squared * (
                minimum * phi_prime - wp.float64(2.0) * phi * ratio
            )
            gamma = wp.float64(0.0)
            if dot != wp.float64(0.0):
                gamma = (dot / scaled_denom) * d_a_d_q
            elif j == ref and minimum * minimum == wp.float64(0.0):
                gamma = (
                    reference_dot
                    * minimum
                    * minimum_d_a_d_q
                    / (scaled_denom * scaled_denom)
                )
            for d in range(n_dims):
                normalized_delta = _normalized_component_f64(
                    points[b, i, d], controls[b, j, d], radius
                )
                if normalized_delta != wp.float64(0.0):
                    grad_points[b, i, d] = grad_points[b, i, d] + (
                        (gamma / radius) * (normalized_delta / q)
                    )


__all__ = [
    "displace_backward_f32",
    "displace_backward_f64",
    "displace_forward_f32",
    "displace_forward_f64",
    "displace_masked_backward_f32",
    "displace_masked_backward_f64",
    "displace_masked_forward_f32",
    "displace_masked_forward_f64",
    "shepard_backward_f32",
    "shepard_backward_f64",
    "shepard_forward_f32",
    "shepard_forward_f64",
    "shepard_point_backward_f32",
    "shepard_point_backward_f64",
]
