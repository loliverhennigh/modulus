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

import math

import torch
import warp as wp

from physicsnemo.core.function_spec import FunctionSpec

from .utils import validate_inputs

### Warp runtime initialization for custom kernels.
wp.init()
wp.config.quiet = True


@wp.func
def _point2(points: wp.array2d(dtype=wp.float32), idx: int) -> wp.vec2f:
    return wp.vec2f(points[idx, 0], points[idx, 1])


@wp.func
def _point3(points: wp.array2d(dtype=wp.float32), idx: int) -> wp.vec3f:
    return wp.vec3f(points[idx, 0], points[idx, 1], points[idx, 2])


@wp.func
def _triangle_face_coeff(
    points: wp.array2d(dtype=wp.float32),
    cells: wp.array2d(dtype=wp.int32),
    cell_idx: int,
    face_idx: int,
) -> wp.vec2f:
    i0 = cells[cell_idx, 0]
    i1 = cells[cell_idx, 1]
    i2 = cells[cell_idx, 2]

    p0 = _point2(points, i0)
    p1 = _point2(points, i1)
    p2 = _point2(points, i2)

    centroid = (p0 + p1 + p2) / 3.0
    area = 0.5 * wp.abs(
        (p1[0] - p0[0]) * (p2[1] - p0[1]) - (p1[1] - p0[1]) * (p2[0] - p0[0])
    )
    inv_area = 1.0 / wp.max(area, 1.0e-12)

    va = p1
    vb = p2
    if face_idx == 1:
        va = p0
        vb = p2
    elif face_idx == 2:
        va = p0
        vb = p1

    edge = vb - va
    normal = wp.vec2f(edge[1], -edge[0])
    face_center = 0.5 * (va + vb)
    to_face = face_center - centroid
    if wp.dot(normal, to_face) < 0.0:
        normal = -normal

    return normal * inv_area


@wp.func
def _tetra_face_coeff(
    points: wp.array2d(dtype=wp.float32),
    cells: wp.array2d(dtype=wp.int32),
    cell_idx: int,
    face_idx: int,
) -> wp.vec3f:
    i0 = cells[cell_idx, 0]
    i1 = cells[cell_idx, 1]
    i2 = cells[cell_idx, 2]
    i3 = cells[cell_idx, 3]

    p0 = _point3(points, i0)
    p1 = _point3(points, i1)
    p2 = _point3(points, i2)
    p3 = _point3(points, i3)

    centroid = 0.25 * (p0 + p1 + p2 + p3)
    volume = wp.abs(wp.dot(p1 - p0, wp.cross(p2 - p0, p3 - p0))) / 6.0
    inv_volume = 1.0 / wp.max(volume, 1.0e-12)

    va = p1
    vb = p2
    vc = p3
    if face_idx == 1:
        va = p0
        vb = p2
        vc = p3
    elif face_idx == 2:
        va = p0
        vb = p1
        vc = p3
    elif face_idx == 3:
        va = p0
        vb = p1
        vc = p2

    normal = 0.5 * wp.cross(vb - va, vc - va)
    face_center = (va + vb + vc) / 3.0
    to_face = face_center - centroid
    if wp.dot(normal, to_face) < 0.0:
        normal = -normal

    return normal * inv_volume


@wp.kernel
def _mesh_green_gauss_2d_forward_kernel(
    points: wp.array2d(dtype=wp.float32),
    cells: wp.array2d(dtype=wp.int32),
    neighbors: wp.array2d(dtype=wp.int32),
    values: wp.array2d(dtype=wp.float32),
    gradients: wp.array3d(dtype=wp.float32),
):
    i, comp = wp.tid()

    vi = values[i, comp]
    gx = float(0.0)
    gy = float(0.0)
    for f in range(3):
        coeff = _triangle_face_coeff(points, cells, i, f)
        j = neighbors[i, f]

        phi_f = vi
        if j >= 0:
            phi_f = 0.5 * (vi + values[j, comp])

        gx = gx + coeff[0] * phi_f
        gy = gy + coeff[1] * phi_f

    gradients[i, 0, comp] = gx
    gradients[i, 1, comp] = gy


@wp.kernel
def _mesh_green_gauss_3d_forward_kernel(
    points: wp.array2d(dtype=wp.float32),
    cells: wp.array2d(dtype=wp.int32),
    neighbors: wp.array2d(dtype=wp.int32),
    values: wp.array2d(dtype=wp.float32),
    gradients: wp.array3d(dtype=wp.float32),
):
    i, comp = wp.tid()

    vi = values[i, comp]
    gx = float(0.0)
    gy = float(0.0)
    gz = float(0.0)
    for f in range(4):
        coeff = _tetra_face_coeff(points, cells, i, f)
        j = neighbors[i, f]

        phi_f = vi
        if j >= 0:
            phi_f = 0.5 * (vi + values[j, comp])

        gx = gx + coeff[0] * phi_f
        gy = gy + coeff[1] * phi_f
        gz = gz + coeff[2] * phi_f

    gradients[i, 0, comp] = gx
    gradients[i, 1, comp] = gy
    gradients[i, 2, comp] = gz


@wp.kernel
def _mesh_green_gauss_2d_backward_kernel(
    points: wp.array2d(dtype=wp.float32),
    cells: wp.array2d(dtype=wp.int32),
    neighbors: wp.array2d(dtype=wp.int32),
    grad_output: wp.array3d(dtype=wp.float32),
    grad_values: wp.array2d(dtype=wp.float32),
):
    i, f, comp = wp.tid()
    j = neighbors[i, f]
    coeff = _triangle_face_coeff(points, cells, i, f)

    dot_go = grad_output[i, 0, comp] * coeff[0] + grad_output[i, 1, comp] * coeff[1]
    owner_contrib = dot_go
    if j >= 0:
        owner_contrib = 0.5 * dot_go

    wp.atomic_add(grad_values, i, comp, owner_contrib)
    if j >= 0:
        wp.atomic_add(grad_values, j, comp, 0.5 * dot_go)


@wp.kernel
def _mesh_green_gauss_3d_backward_kernel(
    points: wp.array2d(dtype=wp.float32),
    cells: wp.array2d(dtype=wp.int32),
    neighbors: wp.array2d(dtype=wp.int32),
    grad_output: wp.array3d(dtype=wp.float32),
    grad_values: wp.array2d(dtype=wp.float32),
):
    i, f, comp = wp.tid()
    j = neighbors[i, f]
    coeff = _tetra_face_coeff(points, cells, i, f)

    dot_go = (
        grad_output[i, 0, comp] * coeff[0]
        + grad_output[i, 1, comp] * coeff[1]
        + grad_output[i, 2, comp] * coeff[2]
    )
    owner_contrib = dot_go
    if j >= 0:
        owner_contrib = 0.5 * dot_go

    wp.atomic_add(grad_values, i, comp, owner_contrib)
    if j >= 0:
        wp.atomic_add(grad_values, j, comp, 0.5 * dot_go)


def _launch_forward(
    *,
    points_fp32: torch.Tensor,
    cells_i32: torch.Tensor,
    neighbors_i32: torch.Tensor,
    values_flat_fp32: torch.Tensor,
    grads_flat: torch.Tensor,
    dims: int,
    wp_device,
    wp_stream,
) -> None:
    ### Launch a single forward kernel across cells and value components.
    kernel = (
        _mesh_green_gauss_2d_forward_kernel
        if dims == 2
        else _mesh_green_gauss_3d_forward_kernel
    )

    n_cells = values_flat_fp32.shape[0]
    n_components = values_flat_fp32.shape[1]

    with wp.ScopedStream(wp_stream):
        wp.launch(
            kernel=kernel,
            dim=(n_cells, n_components),
            inputs=[
                wp.from_torch(points_fp32, dtype=wp.float32),
                wp.from_torch(cells_i32, dtype=wp.int32),
                wp.from_torch(neighbors_i32, dtype=wp.int32),
                wp.from_torch(values_flat_fp32, dtype=wp.float32),
                wp.from_torch(grads_flat, dtype=wp.float32),
            ],
            device=wp_device,
            stream=wp_stream,
        )


def _launch_backward(
    *,
    points_fp32: torch.Tensor,
    cells_i32: torch.Tensor,
    neighbors_i32: torch.Tensor,
    grad_output_components_fp32: torch.Tensor,
    grad_values_flat: torch.Tensor,
    dims: int,
    wp_device,
    wp_stream,
) -> None:
    ### Launch a single backward kernel across cells, faces, and value components.
    kernel = (
        _mesh_green_gauss_2d_backward_kernel
        if dims == 2
        else _mesh_green_gauss_3d_backward_kernel
    )

    n_cells = grad_output_components_fp32.shape[0]
    n_faces = neighbors_i32.shape[1]
    n_components = grad_output_components_fp32.shape[2]

    with wp.ScopedStream(wp_stream):
        wp.launch(
            kernel=kernel,
            dim=(n_cells, n_faces, n_components),
            inputs=[
                wp.from_torch(points_fp32, dtype=wp.float32),
                wp.from_torch(cells_i32, dtype=wp.int32),
                wp.from_torch(neighbors_i32, dtype=wp.int32),
                wp.from_torch(grad_output_components_fp32, dtype=wp.float32),
                wp.from_torch(grad_values_flat, dtype=wp.float32),
            ],
            device=wp_device,
            stream=wp_stream,
        )


def _launch_backward_with_tape(
    *,
    points_fp32: torch.Tensor,
    cells_i32: torch.Tensor,
    neighbors_i32: torch.Tensor,
    values_flat_fp32: torch.Tensor,
    grad_output_components_fp32: torch.Tensor,
    dims: int,
    needs_points: bool,
    needs_values: bool,
    wp_device,
    wp_stream,
) -> tuple[torch.Tensor | None, torch.Tensor | None]:
    """Run Warp Tape autodiff for gradients w.r.t. points and/or values."""
    kernel = (
        _mesh_green_gauss_2d_forward_kernel
        if dims == 2
        else _mesh_green_gauss_3d_forward_kernel
    )

    n_cells = values_flat_fp32.shape[0]
    n_components = values_flat_fp32.shape[1]
    grads_flat = torch.empty(
        (n_cells, dims, n_components),
        device=values_flat_fp32.device,
        dtype=torch.float32,
    )

    with wp.ScopedStream(wp_stream):
        with wp.Tape() as tape:
            wp_points = wp.from_torch(
                points_fp32, dtype=wp.float32, requires_grad=needs_points
            )
            wp_cells = wp.from_torch(cells_i32, dtype=wp.int32)
            wp_neighbors = wp.from_torch(neighbors_i32, dtype=wp.int32)
            wp_values = wp.from_torch(
                values_flat_fp32, dtype=wp.float32, requires_grad=needs_values
            )
            wp_grads = wp.from_torch(grads_flat, dtype=wp.float32, requires_grad=True)

            wp.launch(
                kernel=kernel,
                dim=(n_cells, n_components),
                inputs=[
                    wp_points,
                    wp_cells,
                    wp_neighbors,
                    wp_values,
                    wp_grads,
                ],
                device=wp_device,
                stream=wp_stream,
            )

        grad_map = {
            wp_grads: wp.from_torch(grad_output_components_fp32, dtype=wp.float32)
        }
        tape.backward(grads=grad_map)

        grad_points = None
        grad_values = None
        if needs_points:
            grad_points = wp.to_torch(tape.gradients[wp_points])
        if needs_values:
            grad_values = wp.to_torch(tape.gradients[wp_values])
    return grad_points, grad_values


@torch.library.custom_op(
    "physicsnemo::mesh_green_gauss_gradient_warp_impl", mutates_args=()
)
def mesh_green_gauss_gradient_impl(
    points: torch.Tensor,
    cells: torch.Tensor,
    neighbors: torch.Tensor,
    values: torch.Tensor,
) -> torch.Tensor:
    """Compute Green-Gauss cell-centered gradients with Warp kernels."""
    validate_inputs(points=points, cells=cells, neighbors=neighbors, values=values)

    points_fp32 = points.to(dtype=torch.float32).contiguous()
    cells_i32 = cells.to(dtype=torch.int32).contiguous()
    neighbors_i32 = neighbors.to(dtype=torch.int32).contiguous()
    values_fp32 = values.to(dtype=torch.float32).contiguous()

    n_cells = values.shape[0]
    dims = points.shape[1]
    value_shape = values.shape[1:]
    values_flat_fp32 = values_fp32.reshape(n_cells, -1).contiguous()

    grads_flat = torch.empty(
        (n_cells, dims, values_flat_fp32.shape[1]),
        device=values.device,
        dtype=torch.float32,
    )

    wp_device, wp_stream = FunctionSpec.warp_launch_context(values_fp32)
    _launch_forward(
        points_fp32=points_fp32,
        cells_i32=cells_i32,
        neighbors_i32=neighbors_i32,
        values_flat_fp32=values_flat_fp32,
        grads_flat=grads_flat,
        dims=dims,
        wp_device=wp_device,
        wp_stream=wp_stream,
    )

    output = grads_flat.reshape(n_cells, dims, *value_shape)
    if output.dtype != values.dtype:
        output = output.to(dtype=values.dtype)
    return output


@mesh_green_gauss_gradient_impl.register_fake
def _mesh_green_gauss_gradient_impl_fake(
    points: torch.Tensor,
    cells: torch.Tensor,
    neighbors: torch.Tensor,
    values: torch.Tensor,
) -> torch.Tensor:
    """Fake tensor propagation for Green-Gauss custom op."""
    _ = (cells, neighbors)
    dims = points.shape[1]
    return torch.empty(
        (values.shape[0], dims, *values.shape[1:]),
        device=values.device,
        dtype=values.dtype,
    )


def setup_mesh_green_gauss_gradient_context(
    ctx: torch.autograd.function.FunctionCtx, inputs: tuple, output: torch.Tensor
) -> None:
    """Store backward context for Green-Gauss custom-op autograd."""
    points, cells, neighbors, values = inputs
    _ = output
    values_fp32 = values.to(dtype=torch.float32).contiguous()
    n_cells = values_fp32.shape[0]
    ctx.save_for_backward(
        points.to(dtype=torch.float32).contiguous(),
        cells.to(dtype=torch.int32).contiguous(),
        neighbors.to(dtype=torch.int32).contiguous(),
        values_fp32.reshape(n_cells, -1).contiguous(),
    )
    ctx.points_dtype = points.dtype
    ctx.value_shape = values.shape
    ctx.values_dtype = values.dtype
    ctx.dims = points.shape[1]


def backward_mesh_green_gauss_gradient(
    ctx: torch.autograd.function.FunctionCtx,
    grad_output: torch.Tensor,
) -> tuple[torch.Tensor | None, None, None, torch.Tensor | None]:
    """Backward pass for Green-Gauss custom op."""
    needs_points = ctx.needs_input_grad[0]
    needs_values = ctx.needs_input_grad[3]
    if grad_output is None or (not needs_points and not needs_values):
        return None, None, None, None

    points_fp32, cells_i32, neighbors_i32, values_flat_fp32 = ctx.saved_tensors
    grad_output_fp32 = grad_output.to(dtype=torch.float32).contiguous()

    values_shape = ctx.value_shape
    n_cells = values_shape[0]
    value_shape = values_shape[1:]
    n_components = math.prod(value_shape) if value_shape else 1

    grad_output_components = grad_output_fp32.reshape(n_cells, ctx.dims, n_components)
    grad_output_components = grad_output_components.contiguous()

    grad_points = None
    grad_values_flat = torch.zeros(
        (n_cells, n_components),
        device=grad_output.device,
        dtype=torch.float32,
    )
    wp_device, wp_stream = FunctionSpec.warp_launch_context(grad_output_fp32)
    if needs_points:
        grad_points_fp32, grad_values_fp32 = _launch_backward_with_tape(
            points_fp32=points_fp32,
            cells_i32=cells_i32,
            neighbors_i32=neighbors_i32,
            values_flat_fp32=values_flat_fp32,
            grad_output_components_fp32=grad_output_components,
            dims=ctx.dims,
            needs_points=needs_points,
            needs_values=needs_values,
            wp_device=wp_device,
            wp_stream=wp_stream,
        )
        if grad_points_fp32 is not None:
            grad_points = grad_points_fp32
            if grad_points.dtype != ctx.points_dtype:
                grad_points = grad_points.to(dtype=ctx.points_dtype)
        if needs_values and grad_values_fp32 is not None:
            grad_values_flat = grad_values_fp32
    elif needs_values:
        _launch_backward(
            points_fp32=points_fp32,
            cells_i32=cells_i32,
            neighbors_i32=neighbors_i32,
            grad_output_components_fp32=grad_output_components,
            grad_values_flat=grad_values_flat,
            dims=ctx.dims,
            wp_device=wp_device,
            wp_stream=wp_stream,
        )

    grad_values = None
    if needs_values:
        grad_values = grad_values_flat.reshape(values_shape)
        if grad_values.dtype != ctx.values_dtype:
            grad_values = grad_values.to(dtype=ctx.values_dtype)
    return grad_points, None, None, grad_values


mesh_green_gauss_gradient_impl.register_autograd(
    backward_mesh_green_gauss_gradient,
    setup_context=setup_mesh_green_gauss_gradient_context,
)


def mesh_green_gauss_gradient_warp(
    points: torch.Tensor,
    cells: torch.Tensor,
    neighbors: torch.Tensor,
    values: torch.Tensor,
) -> torch.Tensor:
    """Compute Green-Gauss cell gradients with Warp kernels.

    Notes
    -----
    Warp kernels compute in ``float32`` internally. Inputs in wider floating
    dtypes are accepted and cast to ``float32`` for compute.
    """
    return mesh_green_gauss_gradient_impl(points, cells, neighbors, values)
