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
    validate_cotan_laplacian_inputs,
)

wp.init()
wp.config.log_level = wp.LOG_WARNING


@wp.kernel
def _cotan_laplacian_accumulate_kernel(
    edges: wp.array2d(dtype=wp.int32),
    cotan_weights: wp.array(dtype=wp.float32),
    values: wp.array2d(dtype=wp.float32),
    n_components: int,
    accumulation: wp.array2d(dtype=wp.float32),
):
    tid = wp.tid()
    edge_id = tid // n_components
    comp = tid - edge_id * n_components

    v0 = edges[edge_id, 0]
    v1 = edges[edge_id, 1]
    weight = cotan_weights[edge_id]
    delta = values[v1, comp] - values[v0, comp]
    contrib = weight * delta

    wp.atomic_add(accumulation, v0, comp, contrib)
    wp.atomic_add(accumulation, v1, comp, -contrib)


@wp.kernel
def _cotan_laplacian_normalize_kernel(
    accumulation: wp.array2d(dtype=wp.float32),
    dual_volumes: wp.array(dtype=wp.float32),
    eps: float,
    n_components: int,
    output: wp.array2d(dtype=wp.float32),
):
    tid = wp.tid()
    point_id = tid // n_components
    comp = tid - point_id * n_components
    volume = dual_volumes[point_id]
    if volume < eps:
        volume = eps
    output[point_id, comp] = accumulation[point_id, comp] / volume


def _laplacian_backward_torch(
    *,
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    values_flat: torch.Tensor,
    output_flat: torch.Tensor,
    grad_output_flat: torch.Tensor,
    needs_weights: bool,
    needs_volumes: bool,
    needs_values: bool,
) -> tuple[torch.Tensor | None, torch.Tensor | None, torch.Tensor | None]:
    """Compute cotangent Laplacian gradients using explicit edge formulas."""
    safe_volumes = dual_volumes.clamp(min=safe_eps(dual_volumes.dtype))
    q = grad_output_flat / safe_volumes.view(-1, 1)

    v0 = edges[:, 0].to(torch.int64)
    v1 = edges[:, 1].to(torch.int64)
    qdiff = q[v0] - q[v1]
    value_delta = values_flat[v1] - values_flat[v0]

    grad_weights = None
    if needs_weights:
        grad_weights = (qdiff * value_delta).sum(dim=-1)

    grad_volumes = None
    if needs_volumes:
        grad_volumes = -(grad_output_flat * output_flat / safe_volumes.view(-1, 1)).sum(
            dim=-1
        )

    grad_values = None
    if needs_values:
        weighted_qdiff = cotan_weights.view(-1, 1) * qdiff
        grad_values = torch.zeros_like(values_flat)
        grad_values.index_add_(0, v0, -weighted_qdiff)
        grad_values.index_add_(0, v1, weighted_qdiff)

    return grad_weights, grad_volumes, grad_values


@torch.library.custom_op("physicsnemo::mesh_cotan_laplacian_warp_impl", mutates_args=())
def mesh_cotan_laplacian_impl(
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    values: torch.Tensor,
    eps: float,
) -> torch.Tensor:
    """Apply the normalized cotangent Laplacian with Warp kernels."""
    validate_cotan_laplacian_inputs(
        edges=edges,
        cotan_weights=cotan_weights,
        dual_volumes=dual_volumes,
        values=values,
        function_name="mesh_cotan_laplacian",
    )

    n_points = values.shape[0]
    value_shape = values.shape[1:]
    values_flat_fp32 = values.to(dtype=torch.float32).reshape(n_points, -1).contiguous()
    n_components = values_flat_fp32.shape[1]

    edges_i32 = edges.to(dtype=torch.int32, device=values.device).contiguous()
    weights_fp32 = cotan_weights.to(
        dtype=torch.float32, device=values.device
    ).contiguous()
    volumes_fp32 = dual_volumes.to(
        dtype=torch.float32, device=values.device
    ).contiguous()

    accumulation = torch.zeros_like(values_flat_fp32)
    output_flat = torch.empty_like(values_flat_fp32)
    wp_device, wp_stream = FunctionSpec.warp_launch_context(values_flat_fp32)

    with FunctionSpec.warp_stream_scope(wp_stream):
        if edges_i32.shape[0] > 0:
            wp.launch(
                kernel=_cotan_laplacian_accumulate_kernel,
                dim=edges_i32.shape[0] * n_components,
                inputs=[
                    wp.from_torch(edges_i32, dtype=wp.int32),
                    wp.from_torch(weights_fp32, dtype=wp.float32),
                    wp.from_torch(values_flat_fp32, dtype=wp.float32),
                    int(n_components),
                    wp.from_torch(accumulation, dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
        wp.launch(
            kernel=_cotan_laplacian_normalize_kernel,
            dim=n_points * n_components,
            inputs=[
                wp.from_torch(accumulation, dtype=wp.float32),
                wp.from_torch(volumes_fp32, dtype=wp.float32),
                float(eps),
                int(n_components),
                wp.from_torch(output_flat, dtype=wp.float32),
            ],
            device=wp_device,
            stream=wp_stream,
        )

    output = output_flat.reshape(n_points, *value_shape)
    if output.dtype != values.dtype:
        output = output.to(dtype=values.dtype)
    return output


@mesh_cotan_laplacian_impl.register_fake
def _mesh_cotan_laplacian_impl_fake(
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    values: torch.Tensor,
    eps: float,
) -> torch.Tensor:
    """Fake tensor propagation for cotangent Laplacian custom op."""
    _ = edges, cotan_weights, dual_volumes, eps
    return torch.empty_like(values)


def setup_mesh_cotan_laplacian_context(
    ctx: torch.autograd.function.FunctionCtx, inputs: tuple, output: torch.Tensor
) -> None:
    """Store backward context for the cotangent Laplacian custom op."""
    edges, cotan_weights, dual_volumes, values, eps = inputs
    n_points = values.shape[0]
    ctx.save_for_backward(
        edges.to(dtype=torch.int64, device=values.device).contiguous(),
        cotan_weights.to(dtype=torch.float32, device=values.device).contiguous(),
        dual_volumes.to(dtype=torch.float32, device=values.device).contiguous(),
        values.to(dtype=torch.float32).reshape(n_points, -1).contiguous(),
        output.to(dtype=torch.float32).reshape(n_points, -1).contiguous(),
    )
    ctx.eps = float(eps)
    ctx.values_shape = values.shape
    ctx.weights_dtype = cotan_weights.dtype
    ctx.volumes_dtype = dual_volumes.dtype
    ctx.values_dtype = values.dtype


def backward_mesh_cotan_laplacian(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[None, torch.Tensor | None, torch.Tensor | None, torch.Tensor | None, None]:
    """Backward pass for the cotangent Laplacian custom op."""
    needs_weights = ctx.needs_input_grad[1]
    needs_volumes = ctx.needs_input_grad[2]
    needs_values = ctx.needs_input_grad[3]
    if grad_output is None or not (needs_weights or needs_volumes or needs_values):
        return None, None, None, None, None

    edges, weights, volumes, values_flat, output_flat = ctx.saved_tensors
    grad_output_flat = (
        grad_output.to(dtype=torch.float32).reshape(values_flat.shape).contiguous()
    )
    grad_weights, grad_volumes, grad_values_flat = _laplacian_backward_torch(
        edges=edges,
        cotan_weights=weights,
        dual_volumes=volumes,
        values_flat=values_flat,
        output_flat=output_flat,
        grad_output_flat=grad_output_flat,
        needs_weights=needs_weights,
        needs_volumes=needs_volumes,
        needs_values=needs_values,
    )

    if grad_weights is not None and grad_weights.dtype != ctx.weights_dtype:
        grad_weights = grad_weights.to(dtype=ctx.weights_dtype)
    if grad_volumes is not None and grad_volumes.dtype != ctx.volumes_dtype:
        grad_volumes = grad_volumes.to(dtype=ctx.volumes_dtype)
    grad_values = None
    if grad_values_flat is not None:
        grad_values = grad_values_flat.reshape(ctx.values_shape)
        if grad_values.dtype != ctx.values_dtype:
            grad_values = grad_values.to(dtype=ctx.values_dtype)

    return None, grad_weights, grad_volumes, grad_values, None


mesh_cotan_laplacian_impl.register_autograd(
    backward_mesh_cotan_laplacian,
    setup_context=setup_mesh_cotan_laplacian_context,
)


def mesh_cotan_laplacian_warp(
    edges: torch.Tensor,
    cotan_weights: torch.Tensor,
    dual_volumes: torch.Tensor,
    values: torch.Tensor,
) -> torch.Tensor:
    """Apply the normalized cotangent Laplacian with Warp kernels."""
    eps = safe_eps(torch.float32)
    return mesh_cotan_laplacian_impl(
        edges,
        cotan_weights,
        dual_volumes,
        values,
        float(eps),
    )
