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

"""Torch custom-op integration for Warp-backed mesh morphing."""

from __future__ import annotations

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from .kernels import (
    displace_backward_f32,
    displace_backward_f64,
    displace_forward_f32,
    displace_forward_f64,
    displace_masked_backward_f32,
    displace_masked_backward_f64,
    displace_masked_forward_f32,
    displace_masked_forward_f64,
    shepard_backward_f32,
    shepard_backward_f64,
    shepard_forward_f32,
    shepard_forward_f64,
    shepard_point_backward_f32,
    shepard_point_backward_f64,
)

wp.init()
wp.config.log_level = wp.LOG_WARNING

# ``warp_launch_context`` wraps Torch's current stream. Synchronizing Warp's
# previous stream on entry is unnecessary and creates an illegal dependency
# during CUDA Graph capture, so every scoped launch below disables it.


def _wp_dtype(dtype: torch.dtype):
    if dtype == torch.float32:
        return wp.float32
    if dtype == torch.float64:
        return wp.float64
    raise TypeError(f"Warp morphing supports float32 and float64, got {dtype}")


def _check_common_dtype(*tensors: torch.Tensor) -> None:
    dtype = tensors[0].dtype
    device = tensors[0].device
    if dtype not in (torch.float32, torch.float64):
        raise TypeError(f"morphing supports float32 and float64, got {dtype}")
    if any(t.dtype != dtype for t in tensors):
        raise TypeError("all floating morphing tensors must have the same dtype")
    if any(t.device != device for t in tensors):
        raise ValueError("all morphing tensors must be on the same device")


def _empty_contiguous_like(tensor: torch.Tensor) -> torch.Tensor:
    """Allocate a contiguous tensor with ``tensor``'s shape, dtype, and device."""

    return torch.empty(tensor.shape, dtype=tensor.dtype, device=tensor.device)


def _empty_3d(reference: torch.Tensor) -> torch.Tensor:
    """Return a rank-3 zero-size launch placeholder."""

    return torch.empty((0, 0, 0), dtype=reference.dtype, device=reference.device)


def _empty_2d(reference: torch.Tensor) -> torch.Tensor:
    """Return a rank-2 zero-size launch placeholder."""

    return torch.empty((0, 0), dtype=reference.dtype, device=reference.device)


def _empty_1d(reference: torch.Tensor) -> torch.Tensor:
    """Return a rank-1 zero-size launch placeholder."""

    return torch.empty((0,), dtype=reference.dtype, device=reference.device)


def _wp_view(tensor: torch.Tensor, dtype):
    """Create the faster zero-copy Warp array descriptor for a Torch tensor."""

    return wp.from_torch(
        tensor.detach(), dtype=dtype, return_ctype=True, requires_grad=False
    )


@torch.library.custom_op(
    "physicsnemo::displace_points_warp_impl",
    mutates_args=(),
    schema=(
        "(Tensor points, Tensor displacement, Tensor amount, Tensor? weights) -> Tensor"
    ),
)
def displace_points_warp_impl(
    points: torch.Tensor,
    displacement: torch.Tensor,
    amount: torch.Tensor,
    weights: torch.Tensor | None,
) -> torch.Tensor:
    """Apply a normalized batched dense displacement with Warp."""
    floating_inputs = [points, displacement, amount]
    if weights is not None and weights.dtype != torch.bool:
        floating_inputs.append(weights)
    _check_common_dtype(*floating_inputs)
    if points.ndim != 3 or displacement.shape != points.shape:
        raise ValueError("points and displacement must have identical rank-3 shapes")
    if weights is not None and weights.shape != points.shape[:2]:
        raise ValueError("weights must have shape (batch, num_points)")
    if weights is not None and weights.dtype not in (torch.bool, points.dtype):
        raise TypeError("weights must be bool or match the point dtype")
    if weights is not None and weights.device != points.device:
        raise ValueError("weights and points must be on the same device")
    if amount.shape != (1,):
        raise ValueError("amount must be a one-element tensor")

    points_c = points.contiguous()
    displacement_c = displacement.contiguous()
    amount_c = amount.contiguous()
    weights_c = weights.contiguous() if weights is not None else None
    output = torch.empty_like(points_c)
    if output.numel() == 0:
        return output

    wp_dtype = _wp_dtype(points.dtype)
    is_mask = weights_c is not None and weights_c.dtype == torch.bool
    if is_mask:
        kernel = (
            displace_masked_forward_f32
            if points.dtype == torch.float32
            else displace_masked_forward_f64
        )
    else:
        kernel = (
            displace_forward_f32
            if points.dtype == torch.float32
            else displace_forward_f64
        )
    wp_device, wp_stream = FunctionSpec.warp_launch_context(points_c)
    with wp.ScopedStream(wp_stream, sync_enter=False):
        common = [
            _wp_view(points_c, wp_dtype),
            _wp_view(displacement_c, wp_dtype),
            _wp_view(amount_c, wp_dtype),
        ]
        if is_mask:
            inputs = [
                *common,
                _wp_view(weights_c, wp.bool),
                _wp_view(output, wp_dtype),
            ]
        else:
            weights_launch = weights_c if weights_c is not None else _empty_2d(points_c)
            inputs = [
                *common,
                _wp_view(weights_launch, wp_dtype),
                int(weights_c is not None),
                _wp_view(output, wp_dtype),
            ]
        wp.launch(
            kernel,
            dim=tuple(points_c.shape),
            inputs=inputs,
            device=wp_device,
            stream=wp_stream,
        )
    return output


@displace_points_warp_impl.register_fake
def _displace_points_warp_fake(
    points: torch.Tensor,
    displacement: torch.Tensor,
    amount: torch.Tensor,
    weights: torch.Tensor | None,
) -> torch.Tensor:
    _ = displacement, amount, weights
    return _empty_contiguous_like(points)


def _setup_displace_context(
    ctx: torch.autograd.function.FunctionCtx,
    inputs: tuple,
    output: torch.Tensor,
) -> None:
    _ = output
    _, displacement, amount, weights = inputs
    needs = ctx.needs_input_grad
    # Displacement values are only needed to differentiate amount or floating
    # weights. Avoid retaining a full morph field in the common case where only
    # the displacement-producing model needs a gradient.
    ctx.save_nonpoint_inputs = bool(any(needs[1:]))
    ctx.save_displacement = bool(needs[2] or needs[3])
    ctx.has_weights = weights is not None
    saved = []
    if ctx.save_nonpoint_inputs:
        saved.append(amount.contiguous())
        if weights is not None:
            saved.append(weights.contiguous())
        if ctx.save_displacement:
            saved.append(displacement.contiguous())
    ctx.save_for_backward(*saved)


# Keeping the native pullback behind its own opaque custom op lets AOTAutograd
# compile callers without tracing into Warp. It intentionally has no autograd
# registration: Warp morphing guarantees first-order derivatives only.
@torch.library.custom_op(
    "physicsnemo::displace_points_warp_backward_impl",
    mutates_args=(),
    schema=(
        "(Tensor grad_output, Tensor? displacement, Tensor amount, Tensor? weights, "
        "bool need_points=True, bool need_displacement=True, bool need_amount=True, "
        "bool need_weights=True) -> (Tensor?, Tensor?, Tensor?, Tensor?)"
    ),
)
def displace_points_warp_backward_impl(
    grad_output: torch.Tensor,
    displacement: torch.Tensor | None,
    amount: torch.Tensor,
    weights: torch.Tensor | None,
    need_points: bool = True,
    need_displacement: bool = True,
    need_amount: bool = True,
    need_weights: bool = True,
) -> tuple[
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
]:
    """Evaluate the first-order dense-displacement pullback with Warp."""
    floating_inputs = [grad_output, amount]
    if displacement is not None:
        floating_inputs.append(displacement)
    if weights is not None and weights.dtype != torch.bool:
        floating_inputs.append(weights)
    _check_common_dtype(*floating_inputs)
    if grad_output.ndim != 3:
        raise ValueError("grad_output must be a rank-3 tensor")
    if displacement is not None and grad_output.shape != displacement.shape:
        raise ValueError(
            "grad_output and displacement must have matching rank-3 shapes"
        )
    if (need_amount or need_weights) and displacement is None:
        raise ValueError("displacement is required for amount or weight gradients")
    if amount.shape != (1,):
        raise ValueError("amount has an invalid normalized shape")
    if weights is not None:
        if weights.shape != grad_output.shape[:2]:
            raise ValueError("weights have an invalid normalized shape")
        if weights.device != grad_output.device:
            raise ValueError("weights and grad_output must be on the same device")
        if weights.dtype not in (torch.bool, grad_output.dtype):
            raise TypeError("weights must be bool or match grad_output dtype")
    if need_weights and (weights is None or weights.dtype == torch.bool):
        raise ValueError("only floating weights can require a gradient")

    grad_output_c = grad_output.contiguous()
    displacement_c = displacement.contiguous() if displacement is not None else None
    weights_c = weights.contiguous() if weights is not None else None
    grad_points = _empty_contiguous_like(grad_output_c) if need_points else None
    grad_displacement = (
        _empty_contiguous_like(grad_output_c) if need_displacement else None
    )
    grad_amount = torch.zeros_like(amount) if need_amount else None
    grad_weights = (
        torch.empty(
            grad_output_c.shape[:2],
            dtype=grad_output.dtype,
            device=grad_output.device,
        )
        if need_weights
        else None
    )

    if grad_output_c.numel() > 0 and (
        need_points or need_displacement or need_amount or need_weights
    ):
        wp_dtype = _wp_dtype(grad_output.dtype)
        is_mask = weights_c is not None and weights_c.dtype == torch.bool
        if is_mask:
            kernel = (
                displace_masked_backward_f32
                if grad_output.dtype == torch.float32
                else displace_masked_backward_f64
            )
        else:
            kernel = (
                displace_backward_f32
                if grad_output.dtype == torch.float32
                else displace_backward_f64
            )
        wp_device, wp_stream = FunctionSpec.warp_launch_context(grad_output_c)
        with wp.ScopedStream(wp_stream, sync_enter=False):
            displacement_launch = (
                displacement_c
                if displacement_c is not None
                else _empty_3d(grad_output_c)
            )
            points_launch = (
                grad_points if grad_points is not None else _empty_3d(grad_output_c)
            )
            displacement_grad_launch = (
                grad_displacement
                if grad_displacement is not None
                else _empty_3d(grad_output_c)
            )
            amount_grad_launch = (
                grad_amount if grad_amount is not None else _empty_1d(grad_output_c)
            )
            common = [
                _wp_view(displacement_launch, wp_dtype),
                _wp_view(amount.contiguous(), wp_dtype),
            ]
            if is_mask:
                inputs = [
                    *common,
                    _wp_view(weights_c, wp.bool),
                    _wp_view(grad_output_c, wp_dtype),
                    _wp_view(points_launch, wp_dtype),
                    _wp_view(displacement_grad_launch, wp_dtype),
                    _wp_view(amount_grad_launch, wp_dtype),
                    int(grad_output_c.shape[2]),
                    int(need_points),
                    int(need_displacement),
                    int(need_amount),
                ]
            else:
                weights_launch = (
                    weights_c if weights_c is not None else _empty_2d(grad_output_c)
                )
                weights_grad_launch = (
                    grad_weights
                    if grad_weights is not None
                    else _empty_2d(grad_output_c)
                )
                inputs = [
                    *common,
                    _wp_view(weights_launch, wp_dtype),
                    _wp_view(grad_output_c, wp_dtype),
                    _wp_view(points_launch, wp_dtype),
                    _wp_view(displacement_grad_launch, wp_dtype),
                    _wp_view(amount_grad_launch, wp_dtype),
                    _wp_view(weights_grad_launch, wp_dtype),
                    int(grad_output_c.shape[2]),
                    int(weights_c is not None),
                    int(need_points),
                    int(need_displacement),
                    int(need_amount),
                    int(need_weights),
                ]
            wp.launch(
                kernel,
                dim=tuple(grad_output_c.shape[:2]),
                inputs=inputs,
                device=wp_device,
                stream=wp_stream,
            )
    return grad_points, grad_displacement, grad_amount, grad_weights


@displace_points_warp_backward_impl.register_fake
def _displace_points_warp_backward_fake(
    grad_output: torch.Tensor,
    displacement: torch.Tensor | None,
    amount: torch.Tensor,
    weights: torch.Tensor | None,
    need_points: bool = True,
    need_displacement: bool = True,
    need_amount: bool = True,
    need_weights: bool = True,
) -> tuple[
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
]:
    _ = displacement
    return (
        _empty_contiguous_like(grad_output) if need_points else None,
        _empty_contiguous_like(grad_output) if need_displacement else None,
        torch.empty_like(amount) if need_amount else None,
        (
            torch.empty(
                grad_output.shape[:2],
                dtype=grad_output.dtype,
                device=grad_output.device,
            )
            if need_weights and weights is not None
            else None
        ),
    )


def _backward_displace(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[torch.Tensor | None, ...]:
    needs = ctx.needs_input_grad
    if grad_output is None or not any(needs):
        return None, None, None, None

    # The point contribution is an exact identity; return the cotangent directly
    # and keep it outside the opaque first-order Warp pullback.
    grad_points = grad_output if needs[0] else None
    if any(needs[1:]):
        saved = list(ctx.saved_tensors)
        amount = saved.pop(0)
        weights = saved.pop(0) if ctx.has_weights else None
        displacement = saved.pop(0) if ctx.save_displacement else None
        _, grad_displacement, grad_amount, grad_weights = (
            displace_points_warp_backward_impl(
                grad_output,
                displacement,
                amount,
                weights,
                False,
                bool(needs[1]),
                bool(needs[2]),
                bool(needs[3]),
            )
        )
    else:
        grad_displacement = grad_amount = grad_weights = None

    return (
        grad_points if needs[0] else None,
        grad_displacement if needs[1] else None,
        grad_amount if needs[2] else None,
        grad_weights if needs[3] else None,
    )


displace_points_warp_impl.register_autograd(
    _backward_displace, setup_context=_setup_displace_context
)


@torch.library.custom_op(
    "physicsnemo::compact_shepard_field_warp_impl",
    mutates_args=(),
    schema=(
        "(Tensor points, Tensor controls, Tensor control_displacements, "
        "Tensor radii, bool save_correction=True) -> "
        "(Tensor, Tensor, Tensor, Tensor, Tensor, Tensor)"
    ),
)
def compact_shepard_field_warp_impl(
    points: torch.Tensor,
    controls: torch.Tensor,
    control_displacements: torch.Tensor,
    radii: torch.Tensor,
    save_correction: bool = True,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    """Evaluate the compact Shepard displacement field with Warp."""
    _check_common_dtype(points, controls, control_displacements, radii)
    if points.ndim != 3 or controls.ndim != 3:
        raise ValueError("points and controls must be normalized rank-3 tensors")
    if control_displacements.shape != controls.shape:
        raise ValueError("control_displacements must match controls")
    if controls.shape[0] != points.shape[0] or controls.shape[2] != points.shape[2]:
        raise ValueError(
            "points and controls must have aligned batch/spatial dimensions"
        )
    if radii.shape != controls.shape[:2]:
        raise ValueError("radii must have shape (batch, num_controls)")

    points_c = points.contiguous()
    controls_c = controls.contiguous()
    control_displacements_c = control_displacements.contiguous()
    radii_c = radii.contiguous()
    batch, num_points, num_dims = points_c.shape
    num_controls = controls_c.shape[1]
    field = torch.empty_like(points_c)
    min_q = torch.empty((batch, num_points), dtype=points.dtype, device=points.device)
    denominator = torch.empty_like(min_q)
    exact_count = torch.empty(
        (batch, num_points), dtype=torch.int32, device=points.device
    )
    reference_index = torch.empty_like(exact_count)
    correction = torch.empty_like(points_c) if save_correction else _empty_3d(points_c)

    if batch * num_points > 0:
        wp_dtype = _wp_dtype(points.dtype)
        kernel = (
            shepard_forward_f32
            if points.dtype == torch.float32
            else shepard_forward_f64
        )
        wp_device, wp_stream = FunctionSpec.warp_launch_context(points_c)
        with wp.ScopedStream(wp_stream, sync_enter=False):
            wp.launch(
                kernel,
                dim=(batch, num_points),
                inputs=[
                    _wp_view(points_c, wp_dtype),
                    _wp_view(controls_c, wp_dtype),
                    _wp_view(control_displacements_c, wp_dtype),
                    _wp_view(radii_c, wp_dtype),
                    int(num_controls),
                    int(num_dims),
                    int(1),
                    int(save_correction),
                    _wp_view(field, wp_dtype),
                    _wp_view(min_q, wp_dtype),
                    _wp_view(denominator, wp_dtype),
                    _wp_view(exact_count, wp.int32),
                    _wp_view(reference_index, wp.int32),
                    _wp_view(correction, wp_dtype),
                ],
                device=wp_device,
                stream=wp_stream,
            )
    return field, min_q, denominator, exact_count, reference_index, correction


@compact_shepard_field_warp_impl.register_fake
def _compact_shepard_field_warp_fake(
    points: torch.Tensor,
    controls: torch.Tensor,
    control_displacements: torch.Tensor,
    radii: torch.Tensor,
    save_correction: bool = True,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    _ = controls, control_displacements, radii
    prefix = points.shape[:2]
    return (
        _empty_contiguous_like(points),
        torch.empty(prefix, dtype=points.dtype, device=points.device),
        torch.empty(prefix, dtype=points.dtype, device=points.device),
        torch.empty(prefix, dtype=torch.int32, device=points.device),
        torch.empty(prefix, dtype=torch.int32, device=points.device),
        (_empty_contiguous_like(points) if save_correction else _empty_3d(points)),
    )


@torch.library.custom_op(
    "physicsnemo::compact_shepard_field_warp_forward_only_impl", mutates_args=()
)
def compact_shepard_field_warp_forward_only_impl(
    points: torch.Tensor,
    controls: torch.Tensor,
    control_displacements: torch.Tensor,
    radii: torch.Tensor,
) -> torch.Tensor:
    """Evaluate only the field, omitting every backward-only auxiliary."""

    _check_common_dtype(points, controls, control_displacements, radii)
    if points.ndim != 3 or controls.ndim != 3:
        raise ValueError("points and controls must be normalized rank-3 tensors")
    if control_displacements.shape != controls.shape:
        raise ValueError("control_displacements must match controls")
    if controls.shape[0] != points.shape[0] or controls.shape[2] != points.shape[2]:
        raise ValueError(
            "points and controls must have aligned batch/spatial dimensions"
        )
    if radii.shape != controls.shape[:2]:
        raise ValueError("radii must have shape (batch, num_controls)")

    points_c = points.contiguous()
    controls_c = controls.contiguous()
    control_displacements_c = control_displacements.contiguous()
    radii_c = radii.contiguous()
    batch, num_points, num_dims = points_c.shape
    num_controls = controls_c.shape[1]
    field = torch.empty_like(points_c)
    if batch * num_points > 0:
        wp_dtype = _wp_dtype(points.dtype)
        kernel = (
            shepard_forward_f32
            if points.dtype == torch.float32
            else shepard_forward_f64
        )
        empty_2d = _empty_2d(points_c)
        empty_int = torch.empty((0, 0), dtype=torch.int32, device=points_c.device)
        empty_3d = _empty_3d(points_c)
        wp_device, wp_stream = FunctionSpec.warp_launch_context(points_c)
        with wp.ScopedStream(wp_stream, sync_enter=False):
            wp.launch(
                kernel,
                dim=(batch, num_points),
                inputs=[
                    _wp_view(points_c, wp_dtype),
                    _wp_view(controls_c, wp_dtype),
                    _wp_view(control_displacements_c, wp_dtype),
                    _wp_view(radii_c, wp_dtype),
                    int(num_controls),
                    int(num_dims),
                    int(0),
                    int(0),
                    _wp_view(field, wp_dtype),
                    _wp_view(empty_2d, wp_dtype),
                    _wp_view(empty_2d, wp_dtype),
                    _wp_view(empty_int, wp.int32),
                    _wp_view(empty_int, wp.int32),
                    _wp_view(empty_3d, wp_dtype),
                ],
                device=wp_device,
                stream=wp_stream,
            )
    return field


@compact_shepard_field_warp_forward_only_impl.register_fake
def _compact_shepard_field_warp_forward_only_fake(
    points: torch.Tensor,
    controls: torch.Tensor,
    control_displacements: torch.Tensor,
    radii: torch.Tensor,
) -> torch.Tensor:
    _ = controls, control_displacements, radii
    return _empty_contiguous_like(points)


def _setup_shepard_context(
    ctx: torch.autograd.function.FunctionCtx,
    inputs: tuple,
    output: tuple[
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
        torch.Tensor,
    ],
) -> None:
    points, controls, control_displacements, radii, _ = inputs
    _, min_q, denominator, exact_count, reference_index, correction = output
    needs = ctx.needs_input_grad
    ctx.save_geometry_values = bool(needs[0] or needs[1] or needs[3])
    saved = [
        points.contiguous(),
        controls.contiguous(),
        radii.contiguous(),
        min_q,
        denominator,
        exact_count,
        reference_index,
    ]
    if ctx.save_geometry_values:
        # Keep the separately accumulated correction: reconstructing it as
        # field-reference would lose near-handle precision.
        saved.extend([control_displacements.contiguous(), correction])
    ctx.save_for_backward(*saved)
    ctx.mark_non_differentiable(
        min_q, denominator, exact_count, reference_index, correction
    )


# This opaque pullback is the deliberate first-order autograd boundary; its
# fake implementation supports AOT tracing without promising higher derivatives.
@torch.library.custom_op(
    "physicsnemo::compact_shepard_field_warp_backward_impl",
    mutates_args=(),
    schema=(
        "(Tensor grad_field, Tensor points, Tensor controls, "
        "Tensor? control_displacements, Tensor radii, Tensor min_q, "
        "Tensor denominator, Tensor exact_count, Tensor reference_index, "
        "Tensor? correction, bool need_points=True, bool need_controls=True, "
        "bool need_control_displacements=True, bool need_radii=True) -> "
        "(Tensor?, Tensor?, Tensor?, Tensor?)"
    ),
)
def compact_shepard_field_warp_backward_impl(
    grad_field: torch.Tensor,
    points: torch.Tensor,
    controls: torch.Tensor,
    control_displacements: torch.Tensor | None,
    radii: torch.Tensor,
    min_q: torch.Tensor,
    denominator: torch.Tensor,
    exact_count: torch.Tensor,
    reference_index: torch.Tensor,
    correction: torch.Tensor | None,
    need_points: bool = True,
    need_controls: bool = True,
    need_control_displacements: bool = True,
    need_radii: bool = True,
) -> tuple[
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
]:
    """Evaluate the first-order compact-Shepard pullback with Warp."""
    floating_inputs = [grad_field, points, controls, radii, min_q, denominator]
    if control_displacements is not None:
        floating_inputs.append(control_displacements)
    if correction is not None:
        floating_inputs.append(correction)
    _check_common_dtype(*floating_inputs)
    if exact_count.dtype != torch.int32 or exact_count.device != points.device:
        raise TypeError("exact_count must be an int32 tensor on the points device")
    if reference_index.dtype != torch.int32 or reference_index.device != points.device:
        raise TypeError("reference_index must be an int32 tensor on the points device")
    if grad_field.shape != points.shape:
        raise ValueError("grad_field and points must have matching shapes")
    if controls.ndim != 3:
        raise ValueError("controls must be rank 3")
    if (
        control_displacements is not None
        and controls.shape != control_displacements.shape
    ):
        raise ValueError("controls and control_displacements must match")
    geometry_needed = need_points or need_controls or need_radii
    if geometry_needed and (control_displacements is None or correction is None):
        raise ValueError(
            "control_displacements and correction are required for geometry gradients"
        )
    if radii.shape != controls.shape[:2]:
        raise ValueError("radii must have shape (batch, num_controls)")
    if min_q.shape != points.shape[:2] or denominator.shape != points.shape[:2]:
        raise ValueError("saved normalization tensors must match the query prefix")
    if (
        exact_count.shape != points.shape[:2]
        or reference_index.shape != points.shape[:2]
    ):
        raise ValueError("saved index/count tensors must match the query prefix")
    if correction is not None and correction.shape != points.shape:
        raise ValueError("saved correction must match points")

    grad_field_c = grad_field.contiguous()
    points_c = points.contiguous()
    controls_c = controls.contiguous()
    control_displacements_c = (
        control_displacements.contiguous()
        if control_displacements is not None
        else None
    )
    radii_c = radii.contiguous()
    min_q_c = min_q.contiguous()
    denominator_c = denominator.contiguous()
    exact_count_c = exact_count.contiguous()
    reference_index_c = reference_index.contiguous()
    correction_c = correction.contiguous() if correction is not None else None
    batch, num_points, num_dims = points.shape
    num_controls = controls.shape[1]
    grad_points = (
        (
            torch.zeros(points_c.shape, dtype=points.dtype, device=points.device)
            if num_controls == 0
            else torch.empty(points_c.shape, dtype=points.dtype, device=points.device)
        )
        if need_points
        else None
    )
    grad_controls = torch.zeros_like(controls_c) if need_controls else None
    grad_control_displacements = (
        torch.zeros_like(controls_c) if need_control_displacements else None
    )
    grad_radii = torch.zeros_like(radii_c) if need_radii else None

    if batch * num_points * num_controls > 0 and (
        need_points or need_controls or need_control_displacements or need_radii
    ):
        wp_dtype = _wp_dtype(points.dtype)
        pair_kernel = (
            shepard_backward_f32
            if points.dtype == torch.float32
            else shepard_backward_f64
        )
        point_kernel = (
            shepard_point_backward_f32
            if points.dtype == torch.float32
            else shepard_point_backward_f64
        )
        wp_device, wp_stream = FunctionSpec.warp_launch_context(grad_field_c)
        with wp.ScopedStream(wp_stream, sync_enter=False):
            control_displacements_launch = (
                control_displacements_c
                if control_displacements_c is not None
                else _empty_3d(points_c)
            )
            correction_launch = (
                correction_c if correction_c is not None else _empty_3d(points_c)
            )
            common = [
                _wp_view(points_c, wp_dtype),
                _wp_view(controls_c, wp_dtype),
                _wp_view(control_displacements_launch, wp_dtype),
                _wp_view(radii_c, wp_dtype),
                _wp_view(min_q_c, wp_dtype),
                _wp_view(denominator_c, wp_dtype),
                _wp_view(exact_count_c, wp.int32),
                _wp_view(reference_index_c, wp.int32),
                _wp_view(correction_launch, wp_dtype),
                _wp_view(grad_field_c, wp_dtype),
            ]
            if need_points:
                wp.launch(
                    point_kernel,
                    dim=(batch, num_points),
                    inputs=[
                        *common,
                        int(num_controls),
                        int(num_dims),
                        _wp_view(grad_points, wp_dtype),
                    ],
                    device=wp_device,
                    stream=wp_stream,
                )
            if need_controls or need_control_displacements or need_radii:
                controls_grad_launch = (
                    grad_controls if grad_controls is not None else _empty_3d(points_c)
                )
                displacement_grad_launch = (
                    grad_control_displacements
                    if grad_control_displacements is not None
                    else _empty_3d(points_c)
                )
                radii_grad_launch = (
                    grad_radii if grad_radii is not None else _empty_2d(points_c)
                )
                wp.launch(
                    pair_kernel,
                    dim=(batch, num_points, num_controls),
                    inputs=[
                        *common,
                        int(num_dims),
                        int(0),
                        int(need_controls),
                        int(need_control_displacements),
                        int(need_radii),
                        _wp_view(_empty_3d(points_c), wp_dtype),
                        _wp_view(controls_grad_launch, wp_dtype),
                        _wp_view(displacement_grad_launch, wp_dtype),
                        _wp_view(radii_grad_launch, wp_dtype),
                    ],
                    device=wp_device,
                    stream=wp_stream,
                )
    return grad_points, grad_controls, grad_control_displacements, grad_radii


@compact_shepard_field_warp_backward_impl.register_fake
def _compact_shepard_field_warp_backward_fake(
    grad_field: torch.Tensor,
    points: torch.Tensor,
    controls: torch.Tensor,
    control_displacements: torch.Tensor | None,
    radii: torch.Tensor,
    min_q: torch.Tensor,
    denominator: torch.Tensor,
    exact_count: torch.Tensor,
    reference_index: torch.Tensor,
    correction: torch.Tensor | None,
    need_points: bool = True,
    need_controls: bool = True,
    need_control_displacements: bool = True,
    need_radii: bool = True,
) -> tuple[
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
]:
    _ = (
        grad_field,
        control_displacements,
        min_q,
        denominator,
        exact_count,
        reference_index,
        correction,
    )
    return (
        _empty_contiguous_like(points) if need_points else None,
        _empty_contiguous_like(controls) if need_controls else None,
        _empty_contiguous_like(controls) if need_control_displacements else None,
        _empty_contiguous_like(radii) if need_radii else None,
    )


def _backward_shepard(
    ctx: torch.autograd.function.FunctionCtx,
    grad_field: torch.Tensor | None,
    grad_min_q: torch.Tensor | None,
    grad_denominator: torch.Tensor | None,
    grad_exact_count: torch.Tensor | None,
    grad_reference_index: torch.Tensor | None,
    grad_correction: torch.Tensor | None,
) -> tuple[torch.Tensor | None, ...]:
    _ = (
        grad_min_q,
        grad_denominator,
        grad_exact_count,
        grad_reference_index,
        grad_correction,
    )
    needs = ctx.needs_input_grad
    if grad_field is None or not any(needs):
        return None, None, None, None, None

    saved = list(ctx.saved_tensors)
    points = saved.pop(0)
    controls = saved.pop(0)
    radii = saved.pop(0)
    min_q = saved.pop(0)
    denominator = saved.pop(0)
    exact_count = saved.pop(0)
    reference_index = saved.pop(0)
    if ctx.save_geometry_values:
        control_displacements = saved.pop(0)
        correction = saved.pop(0)
    else:
        control_displacements = correction = None
    grad_points, grad_controls, grad_control_displacements, grad_radii = (
        compact_shepard_field_warp_backward_impl(
            grad_field,
            points,
            controls,
            control_displacements,
            radii,
            min_q,
            denominator,
            exact_count,
            reference_index,
            correction,
            bool(needs[0]),
            bool(needs[1]),
            bool(needs[2]),
            bool(needs[3]),
        )
    )

    return (
        grad_points if needs[0] else None,
        grad_controls if needs[1] else None,
        grad_control_displacements if needs[2] else None,
        grad_radii if needs[3] else None,
        None,
    )


compact_shepard_field_warp_impl.register_autograd(
    _backward_shepard, setup_context=_setup_shepard_context
)


def displace_points_warp(
    points: torch.Tensor,
    displacement: torch.Tensor,
    amount: torch.Tensor,
    weights: torch.Tensor | None,
) -> torch.Tensor:
    """Normalized rank-3 Warp dense-displacement entry point."""
    return displace_points_warp_impl(
        points.contiguous(),
        displacement.contiguous(),
        amount.contiguous(),
        weights.contiguous() if weights is not None else None,
    )


def morph_points_warp(
    points: torch.Tensor,
    control_points: torch.Tensor,
    control_displacements: torch.Tensor,
    radius: torch.Tensor,
    amount: torch.Tensor,
    weights: torch.Tensor | None,
) -> torch.Tensor:
    """Normalized rank-3 Warp compact-Shepard morphing entry point."""
    if control_points.shape[1] == 0:
        # Preserve every differentiable zero dependency without paying for two
        # Warp launches or allocating field auxiliaries.
        zero = (
            control_points.sum()
            + control_displacements.sum()
            + radius.sum()
            + amount.sum()
        )
        if weights is not None and weights.dtype != torch.bool:
            zero = zero + weights.sum()
        return points + zero * 0

    points_c = points.contiguous()
    controls_c = control_points.contiguous()
    control_displacements_c = control_displacements.contiguous()
    radius_c = radius.contiguous()
    amount_c = amount.contiguous()
    weights_c = weights.contiguous() if weights is not None else None
    needs_field_grad = torch.is_grad_enabled() and any(
        tensor.requires_grad
        for tensor in (points_c, controls_c, control_displacements_c, radius_c)
    )
    if needs_field_grad:
        needs_geometry_grad = (
            points_c.requires_grad or controls_c.requires_grad or radius_c.requires_grad
        )
        field, _, _, _, _, _ = compact_shepard_field_warp_impl(
            points_c,
            controls_c,
            control_displacements_c,
            radius_c,
            needs_geometry_grad,
        )
    else:
        field = compact_shepard_field_warp_forward_only_impl(
            points_c, controls_c, control_displacements_c, radius_c
        )
    return displace_points_warp_impl(points_c, field, amount_c, weights_c)


__all__ = [
    "compact_shepard_field_warp_backward_impl",
    "compact_shepard_field_warp_forward_only_impl",
    "compact_shepard_field_warp_impl",
    "displace_points_warp",
    "displace_points_warp_backward_impl",
    "displace_points_warp_impl",
    "morph_points_warp",
]
