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

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec
from physicsnemo.nn.functional.derivatives._mesh_cotan_operator_utils import (
    safe_eps,
    validate_cotan_divergence_inputs,
)

wp.init()
wp.config.log_level = wp.LOG_WARNING


@wp.kernel
def _cotan_divergence_accumulate_kernel(
    points: wp.array2d(dtype=wp.float32),
    edges: wp.array2d(dtype=wp.int32),
    cotan_weights: wp.array(dtype=wp.float32),
    vector_field: wp.array2d(dtype=wp.float32),
    n_dims: int,
    accumulation: wp.array(dtype=wp.float32),
):
    edge_id = wp.tid()
    v0 = edges[edge_id, 0]
    v1 = edges[edge_id, 1]

    flat_flux = float(0.0)
    for dim in range(n_dims):
        edge_vector = points[v1, dim] - points[v0, dim]
        edge_average = 0.5 * (vector_field[v0, dim] + vector_field[v1, dim])
        flat_flux = flat_flux + edge_average * edge_vector

    weighted_flux = cotan_weights[edge_id] * flat_flux
    wp.atomic_add(accumulation, v0, weighted_flux)
    wp.atomic_add(accumulation, v1, -weighted_flux)


@wp.kernel
def _cotan_divergence_normalize_kernel(
    accumulation: wp.array(dtype=wp.float32),
    dual_volumes: wp.array(dtype=wp.float32),
    eps: float,
    output: wp.array(dtype=wp.float32),
):
    point_id = wp.tid()
    volume = dual_volumes[point_id]
    if volume < eps:
        volume = eps
    output[point_id] = accumulation[point_id] / volume


def _divergence_backward_torch(
    *,
    points: torch.Tensor,
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    vector_field: torch.Tensor,
    output: torch.Tensor,
    grad_output: torch.Tensor,
    needs_points: bool,
    needs_weights: bool,
    needs_volumes: bool,
    needs_vector: bool,
) -> tuple[
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
]:
    """Compute cotangent divergence gradients using explicit edge formulas."""
    safe_volumes = dual_volumes.clamp(min=safe_eps(dual_volumes.dtype))
    q = grad_output / safe_volumes

    v0 = edges[:, 0].to(torch.int64)
    v1 = edges[:, 1].to(torch.int64)
    edge_q = q[v0] - q[v1]
    edge_scale = edge_q * cotan_weights

    edge_vectors = points[v1] - points[v0]
    edge_average = 0.5 * (vector_field[v0] + vector_field[v1])

    grad_points = None
    if needs_points:
        grad_points = torch.zeros_like(points)
        point_contrib = edge_scale.view(-1, 1) * edge_average
        grad_points.index_add_(0, v0, -point_contrib)
        grad_points.index_add_(0, v1, point_contrib)

    grad_weights = None
    if needs_weights:
        grad_weights = edge_q * (edge_average * edge_vectors).sum(dim=-1)

    grad_volumes = None
    if needs_volumes:
        grad_volumes = -(grad_output * output / safe_volumes)

    grad_vector = None
    if needs_vector:
        grad_vector = torch.zeros_like(vector_field)
        vector_contrib = 0.5 * edge_scale.view(-1, 1) * edge_vectors
        grad_vector.index_add_(0, v0, vector_contrib)
        grad_vector.index_add_(0, v1, vector_contrib)

    return grad_points, grad_weights, grad_volumes, grad_vector


@torch.library.custom_op(
    "physicsnemo::mesh_cotan_divergence_warp_impl", mutates_args=()
)
def mesh_cotan_divergence_impl(
    points: torch.Tensor,
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    vector_field: torch.Tensor,
    eps: float,
) -> torch.Tensor:
    """Compute cotangent/DEC mesh divergence with Warp kernels."""
    validate_cotan_divergence_inputs(
        points=points,
        edges=edges,
        cotan_weights=cotan_weights,
        dual_volumes=dual_volumes,
        vector_field=vector_field,
        function_name="mesh_cotan_divergence",
    )

    points_fp32 = points.to(dtype=torch.float32).contiguous()
    vector_fp32 = vector_field.to(dtype=torch.float32).contiguous()
    edges_i32 = edges.to(dtype=torch.int32, device=points.device).contiguous()
    weights_fp32 = cotan_weights.to(
        dtype=torch.float32, device=points.device
    ).contiguous()
    volumes_fp32 = dual_volumes.to(
        dtype=torch.float32, device=points.device
    ).contiguous()

    accumulation = torch.zeros(
        (points.shape[0],), dtype=torch.float32, device=points.device
    )
    output = torch.empty_like(accumulation)
    wp_device, wp_stream = FunctionSpec.warp_launch_context(points_fp32)

    with FunctionSpec.warp_stream_scope(wp_stream):
        if edges_i32.shape[0] > 0:
            wp.launch(
                kernel=_cotan_divergence_accumulate_kernel,
                dim=edges_i32.shape[0],
                inputs=[
                    wp.from_torch(points_fp32, dtype=wp.float32),
                    wp.from_torch(edges_i32, dtype=wp.int32),
                    wp.from_torch(weights_fp32, dtype=wp.float32),
                    wp.from_torch(vector_fp32, dtype=wp.float32),
                    int(points_fp32.shape[1]),
                    wp.from_torch(accumulation, dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
        wp.launch(
            kernel=_cotan_divergence_normalize_kernel,
            dim=points.shape[0],
            inputs=[
                wp.from_torch(accumulation, dtype=wp.float32),
                wp.from_torch(volumes_fp32, dtype=wp.float32),
                float(eps),
                wp.from_torch(output, dtype=wp.float32),
            ],
            device=wp_device,
            stream=wp_stream,
        )

    if output.dtype != vector_field.dtype:
        output = output.to(dtype=vector_field.dtype)
    return output


@mesh_cotan_divergence_impl.register_fake
def _mesh_cotan_divergence_impl_fake(
    points: torch.Tensor,
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    vector_field: torch.Tensor,
    eps: float,
) -> torch.Tensor:
    """Fake tensor propagation for cotangent divergence custom op."""
    _ = edges, cotan_weights, dual_volumes, vector_field, eps
    return torch.empty(
        (points.shape[0],), device=points.device, dtype=vector_field.dtype
    )


def setup_mesh_cotan_divergence_context(
    ctx: torch.autograd.function.FunctionCtx, inputs: tuple, output: torch.Tensor
) -> None:
    """Store backward context for the cotangent divergence custom op."""
    points, edges, cotan_weights, dual_volumes, vector_field, eps = inputs
    ctx.save_for_backward(
        points.to(dtype=torch.float32).contiguous(),
        edges.to(dtype=torch.int64, device=points.device).contiguous(),
        cotan_weights.to(dtype=torch.float32, device=points.device).contiguous(),
        dual_volumes.to(dtype=torch.float32, device=points.device).contiguous(),
        vector_field.to(dtype=torch.float32).contiguous(),
        output.to(dtype=torch.float32).contiguous(),
    )
    ctx.eps = float(eps)
    ctx.points_dtype = points.dtype
    ctx.weights_dtype = cotan_weights.dtype
    ctx.volumes_dtype = dual_volumes.dtype
    ctx.vector_dtype = vector_field.dtype


def backward_mesh_cotan_divergence(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[
    torch.Tensor | None,
    None,
    torch.Tensor | None,
    torch.Tensor | None,
    torch.Tensor | None,
    None,
]:
    """Backward pass for the cotangent divergence custom op."""
    needs_points = ctx.needs_input_grad[0]
    needs_weights = ctx.needs_input_grad[2]
    needs_volumes = ctx.needs_input_grad[3]
    needs_vector = ctx.needs_input_grad[4]
    if grad_output is None or not (
        needs_points or needs_weights or needs_volumes or needs_vector
    ):
        return None, None, None, None, None, None

    points, edges, weights, volumes, vector_field, output = ctx.saved_tensors
    grad_output_fp32 = grad_output.to(dtype=torch.float32).contiguous()
    grad_points, grad_weights, grad_volumes, grad_vector = _divergence_backward_torch(
        points=points,
        edges=edges,
        cotan_weights=weights,
        dual_volumes=volumes,
        vector_field=vector_field,
        output=output,
        grad_output=grad_output_fp32,
        needs_points=needs_points,
        needs_weights=needs_weights,
        needs_volumes=needs_volumes,
        needs_vector=needs_vector,
    )

    if grad_points is not None and grad_points.dtype != ctx.points_dtype:
        grad_points = grad_points.to(dtype=ctx.points_dtype)
    if grad_weights is not None and grad_weights.dtype != ctx.weights_dtype:
        grad_weights = grad_weights.to(dtype=ctx.weights_dtype)
    if grad_volumes is not None and grad_volumes.dtype != ctx.volumes_dtype:
        grad_volumes = grad_volumes.to(dtype=ctx.volumes_dtype)
    if grad_vector is not None and grad_vector.dtype != ctx.vector_dtype:
        grad_vector = grad_vector.to(dtype=ctx.vector_dtype)

    return grad_points, None, grad_weights, grad_volumes, grad_vector, None


mesh_cotan_divergence_impl.register_autograd(
    backward_mesh_cotan_divergence,
    setup_context=setup_mesh_cotan_divergence_context,
)


def mesh_cotan_divergence_warp(
    points: torch.Tensor,
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    vector_field: torch.Tensor,
) -> torch.Tensor:
    """Compute cotangent/DEC mesh divergence with Warp kernels."""
    eps = safe_eps(torch.float32)
    return mesh_cotan_divergence_impl(
        points,
        edges,
        cotan_weights,
        dual_volumes,
        vector_field,
        float(eps),
    )
