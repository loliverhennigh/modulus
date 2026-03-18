# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch

from physicsnemo.core.function_spec import FunctionSpec

_WARP_AVAILABLE = True
_WARP_IMPORT_ERROR: Exception | None = None

### Optional Warp dependency detection for backend availability.
try:  # pragma: no cover - optional dependency
    import warp as wp
except Exception as exc:  # pragma: no cover - optional dependency
    _WARP_AVAILABLE = False
    _WARP_IMPORT_ERROR = exc

if _WARP_AVAILABLE:
    ### Warp runtime initialization for custom kernels.
    wp.init()
    wp.config.quiet = True

    @wp.kernel
    def _mesh_lsq_gradient_1d_kernel(
        points: wp.array2d(dtype=wp.float32),
        values: wp.array(dtype=wp.float32),
        offsets: wp.array(dtype=wp.int32),
        indices: wp.array(dtype=wp.int32),
        weight_power: float,
        min_neighbors: int,
        reg_eps: float,
        gradients: wp.array2d(dtype=wp.float32),
    ):
        i = wp.tid()

        # Read the CSR neighbor segment for this entity.
        start = offsets[i]
        end = offsets[i + 1]
        count = end - start
        if count < min_neighbors:
            gradients[i, 0] = 0.0
            return

        # Gather center state and initialize normal-equation accumulators.
        px = points[i, 0]
        pval = values[i]

        m00 = float(reg_eps)
        b0 = float(0.0)

        # Accumulate A^T W A and A^T W b over neighbors.
        for p in range(start, end):
            n = indices[p]
            dx = points[n, 0] - px
            dphi = values[n] - pval

            dist2 = dx * dx + 1.0e-20
            w = wp.pow(dist2, -0.5 * weight_power)

            m00 = m00 + w * dx * dx
            b0 = b0 + w * dx * dphi

        # Solve the 1x1 normal equation with a numerical floor.
        gx = float(0.0)
        if m00 > 1.0e-20:
            gx = b0 / m00

        gradients[i, 0] = gx

    @wp.kernel
    def _mesh_lsq_gradient_2d_kernel(
        points: wp.array2d(dtype=wp.float32),
        values: wp.array(dtype=wp.float32),
        offsets: wp.array(dtype=wp.int32),
        indices: wp.array(dtype=wp.int32),
        weight_power: float,
        min_neighbors: int,
        reg_eps: float,
        gradients: wp.array2d(dtype=wp.float32),
    ):
        i = wp.tid()

        # Read the CSR neighbor segment for this entity.
        start = offsets[i]
        end = offsets[i + 1]
        count = end - start
        if count < min_neighbors:
            gradients[i, 0] = 0.0
            gradients[i, 1] = 0.0
            return

        # Gather center state and initialize normal-equation accumulators.
        px = points[i, 0]
        py = points[i, 1]
        pval = values[i]

        m00 = float(reg_eps)
        m01 = float(0.0)
        m11 = float(reg_eps)
        b0 = float(0.0)
        b1 = float(0.0)

        # Accumulate A^T W A and A^T W b over neighbors.
        for p in range(start, end):
            n = indices[p]
            dx = points[n, 0] - px
            dy = points[n, 1] - py
            dphi = values[n] - pval

            dist2 = dx * dx + dy * dy + 1.0e-20
            w = wp.pow(dist2, -0.5 * weight_power)

            m00 = m00 + w * dx * dx
            m01 = m01 + w * dx * dy
            m11 = m11 + w * dy * dy

            b0 = b0 + w * dx * dphi
            b1 = b1 + w * dy * dphi

        # Solve the 2x2 system analytically with determinant-based conditioning.
        det = m00 * m11 - m01 * m01

        gx = float(0.0)
        gy = float(0.0)
        stability_scale = m00 * m11 + 1.0e-20
        if wp.abs(det) > 1.0e-6 * stability_scale:
            inv00 = m11 / det
            inv01 = -m01 / det
            inv11 = m00 / det
            gx = inv00 * b0 + inv01 * b1
            gy = inv01 * b0 + inv11 * b1

        gradients[i, 0] = gx
        gradients[i, 1] = gy

    @wp.kernel
    def _mesh_lsq_gradient_3d_kernel(
        points: wp.array2d(dtype=wp.float32),
        values: wp.array(dtype=wp.float32),
        offsets: wp.array(dtype=wp.int32),
        indices: wp.array(dtype=wp.int32),
        weight_power: float,
        min_neighbors: int,
        reg_eps: float,
        gradients: wp.array2d(dtype=wp.float32),
    ):
        i = wp.tid()

        # Read the CSR neighbor segment for this entity.
        start = offsets[i]
        end = offsets[i + 1]
        count = end - start
        if count < min_neighbors:
            gradients[i, 0] = 0.0
            gradients[i, 1] = 0.0
            gradients[i, 2] = 0.0
            return

        # Gather center state and initialize normal-equation accumulators.
        px = points[i, 0]
        py = points[i, 1]
        pz = points[i, 2]
        pval = values[i]

        m00 = float(reg_eps)
        m01 = float(0.0)
        m02 = float(0.0)
        m11 = float(reg_eps)
        m12 = float(0.0)
        m22 = float(reg_eps)

        b0 = float(0.0)
        b1 = float(0.0)
        b2 = float(0.0)

        # Accumulate A^T W A and A^T W b over neighbors.
        for p in range(start, end):
            n = indices[p]
            dx = points[n, 0] - px
            dy = points[n, 1] - py
            dz = points[n, 2] - pz
            dphi = values[n] - pval

            dist2 = dx * dx + dy * dy + dz * dz + 1.0e-20
            w = wp.pow(dist2, -0.5 * weight_power)

            m00 = m00 + w * dx * dx
            m01 = m01 + w * dx * dy
            m02 = m02 + w * dx * dz
            m11 = m11 + w * dy * dy
            m12 = m12 + w * dy * dz
            m22 = m22 + w * dz * dz

            b0 = b0 + w * dx * dphi
            b1 = b1 + w * dy * dphi
            b2 = b2 + w * dz * dphi

        # Build cofactors and solve the 3x3 system analytically.
        c00 = m11 * m22 - m12 * m12
        c01 = -(m01 * m22 - m12 * m02)
        c02 = m01 * m12 - m11 * m02
        c11 = m00 * m22 - m02 * m02
        c12 = -(m00 * m12 - m01 * m02)
        c22 = m00 * m11 - m01 * m01

        det = m00 * c00 + m01 * c01 + m02 * c02

        gx = float(0.0)
        gy = float(0.0)
        gz = float(0.0)
        trace = m00 + m11 + m22
        stability_scale = trace * trace * trace + 1.0e-20
        if wp.abs(det) > 1.0e-8 * stability_scale:
            inv_det = 1.0 / det
            inv00 = c00 * inv_det
            inv01 = c01 * inv_det
            inv02 = c02 * inv_det
            inv11 = c11 * inv_det
            inv12 = c12 * inv_det
            inv22 = c22 * inv_det

            gx = inv00 * b0 + inv01 * b1 + inv02 * b2
            gy = inv01 * b0 + inv11 * b1 + inv12 * b2
            gz = inv02 * b0 + inv12 * b1 + inv22 * b2

        gradients[i, 0] = gx
        gradients[i, 1] = gy
        gradients[i, 2] = gz


def _validate_inputs(
    points: torch.Tensor,
    values: torch.Tensor,
    neighbor_offsets: torch.Tensor,
    neighbor_indices: torch.Tensor,
    *,
    min_neighbors: int,
) -> None:
    ### Validate core tensor shapes and dimensions.
    if points.ndim != 2:
        raise ValueError(f"points must have shape (n_entities, dims), got {points.shape=}")
    if points.shape[1] < 1 or points.shape[1] > 3:
        raise ValueError(
            f"warp mesh_lsq_gradient supports 1D/2D/3D points, got dims={points.shape[1]}"
        )
    if values.ndim < 1:
        raise ValueError(f"values must have shape (n_entities, ...), got {values.shape=}")
    if values.shape[0] != points.shape[0]:
        raise ValueError(
            f"values leading dimension must match points: {values.shape[0]} != {points.shape[0]}"
        )
    if neighbor_offsets.ndim != 1:
        raise ValueError("neighbor_offsets must be rank-1")
    if neighbor_offsets.shape[0] != points.shape[0] + 1:
        raise ValueError(
            "neighbor_offsets must have shape (n_entities + 1,), "
            f"got {neighbor_offsets.shape} for n_entities={points.shape[0]}"
        )
    if neighbor_indices.ndim != 1:
        raise ValueError("neighbor_indices must be rank-1")

    ### Validate all inputs are co-located on the same device.
    if not (
        points.device == values.device
        and points.device == neighbor_offsets.device
        and points.device == neighbor_indices.device
    ):
        raise ValueError(
            "points, values, neighbor_offsets, and neighbor_indices must be on the same device"
        )

    ### Validate floating-point and index dtypes.
    if not torch.is_floating_point(points):
        raise TypeError("points must be floating-point")
    if not torch.is_floating_point(values):
        raise TypeError("values must be floating-point")
    if neighbor_offsets.dtype not in (torch.int32, torch.int64):
        raise TypeError("neighbor_offsets must be int32 or int64")
    if neighbor_indices.dtype not in (torch.int32, torch.int64):
        raise TypeError("neighbor_indices must be int32 or int64")
    if min_neighbors < 0:
        raise ValueError("min_neighbors must be non-negative")

    ### Validate CSR range invariants.
    if int(neighbor_offsets[0].item()) != 0:
        raise ValueError("neighbor_offsets must start at 0")
    if int(neighbor_offsets[-1].item()) != neighbor_indices.shape[0]:
        raise ValueError("neighbor_offsets[-1] must equal len(neighbor_indices)")
    if torch.any(neighbor_offsets[1:] < neighbor_offsets[:-1]):
        raise ValueError("neighbor_offsets must be non-decreasing")

    if neighbor_indices.numel() > 0:
        idx_min = int(neighbor_indices.min().item())
        idx_max = int(neighbor_indices.max().item())
        if idx_min < 0 or idx_max >= points.shape[0]:
            raise ValueError(
                f"neighbor_indices must satisfy 0 <= index < n_entities ({points.shape[0]})"
            )


if _WARP_AVAILABLE:
    @wp.kernel
    def _mesh_lsq_gradient_1d_backward_kernel(
        points: wp.array2d(dtype=wp.float32),
        offsets: wp.array(dtype=wp.int32),
        indices: wp.array(dtype=wp.int32),
        grad_output: wp.array2d(dtype=wp.float32),
        weight_power: float,
        min_neighbors: int,
        reg_eps: float,
        grad_values: wp.array(dtype=wp.float32),
    ):
        i = wp.tid()

        start = offsets[i]
        end = offsets[i + 1]
        count = end - start
        if count < min_neighbors:
            return

        px = points[i, 0]
        m00 = float(reg_eps)
        for p in range(start, end):
            n = indices[p]
            dx = points[n, 0] - px
            dist2 = dx * dx + 1.0e-20
            w = wp.pow(dist2, -0.5 * weight_power)
            m00 = m00 + w * dx * dx

        p0 = float(0.0)
        if m00 > 1.0e-20:
            p0 = grad_output[i, 0] / m00

        self_contrib = float(0.0)
        for p in range(start, end):
            n = indices[p]
            dx = points[n, 0] - px
            dist2 = dx * dx + 1.0e-20
            w = wp.pow(dist2, -0.5 * weight_power)
            c = w * p0 * dx
            wp.atomic_add(grad_values, n, c)
            self_contrib = self_contrib - c

        wp.atomic_add(grad_values, i, self_contrib)

    @wp.kernel
    def _mesh_lsq_gradient_2d_backward_kernel(
        points: wp.array2d(dtype=wp.float32),
        offsets: wp.array(dtype=wp.int32),
        indices: wp.array(dtype=wp.int32),
        grad_output: wp.array2d(dtype=wp.float32),
        weight_power: float,
        min_neighbors: int,
        reg_eps: float,
        grad_values: wp.array(dtype=wp.float32),
    ):
        i = wp.tid()

        start = offsets[i]
        end = offsets[i + 1]
        count = end - start
        if count < min_neighbors:
            return

        px = points[i, 0]
        py = points[i, 1]
        m00 = float(reg_eps)
        m01 = float(0.0)
        m11 = float(reg_eps)
        for p in range(start, end):
            n = indices[p]
            dx = points[n, 0] - px
            dy = points[n, 1] - py
            dist2 = dx * dx + dy * dy + 1.0e-20
            w = wp.pow(dist2, -0.5 * weight_power)
            m00 = m00 + w * dx * dx
            m01 = m01 + w * dx * dy
            m11 = m11 + w * dy * dy

        p0 = float(0.0)
        p1 = float(0.0)
        det = m00 * m11 - m01 * m01
        stability_scale = m00 * m11 + 1.0e-20
        if wp.abs(det) > 1.0e-6 * stability_scale:
            inv00 = m11 / det
            inv01 = -m01 / det
            inv11 = m00 / det
            go0 = grad_output[i, 0]
            go1 = grad_output[i, 1]
            p0 = inv00 * go0 + inv01 * go1
            p1 = inv01 * go0 + inv11 * go1

        self_contrib = float(0.0)
        for p in range(start, end):
            n = indices[p]
            dx = points[n, 0] - px
            dy = points[n, 1] - py
            dist2 = dx * dx + dy * dy + 1.0e-20
            w = wp.pow(dist2, -0.5 * weight_power)
            c = w * (p0 * dx + p1 * dy)
            wp.atomic_add(grad_values, n, c)
            self_contrib = self_contrib - c

        wp.atomic_add(grad_values, i, self_contrib)

    @wp.kernel
    def _mesh_lsq_gradient_3d_backward_kernel(
        points: wp.array2d(dtype=wp.float32),
        offsets: wp.array(dtype=wp.int32),
        indices: wp.array(dtype=wp.int32),
        grad_output: wp.array2d(dtype=wp.float32),
        weight_power: float,
        min_neighbors: int,
        reg_eps: float,
        grad_values: wp.array(dtype=wp.float32),
    ):
        i = wp.tid()

        start = offsets[i]
        end = offsets[i + 1]
        count = end - start
        if count < min_neighbors:
            return

        px = points[i, 0]
        py = points[i, 1]
        pz = points[i, 2]
        m00 = float(reg_eps)
        m01 = float(0.0)
        m02 = float(0.0)
        m11 = float(reg_eps)
        m12 = float(0.0)
        m22 = float(reg_eps)
        for p in range(start, end):
            n = indices[p]
            dx = points[n, 0] - px
            dy = points[n, 1] - py
            dz = points[n, 2] - pz
            dist2 = dx * dx + dy * dy + dz * dz + 1.0e-20
            w = wp.pow(dist2, -0.5 * weight_power)
            m00 = m00 + w * dx * dx
            m01 = m01 + w * dx * dy
            m02 = m02 + w * dx * dz
            m11 = m11 + w * dy * dy
            m12 = m12 + w * dy * dz
            m22 = m22 + w * dz * dz

        c00 = m11 * m22 - m12 * m12
        c01 = -(m01 * m22 - m12 * m02)
        c02 = m01 * m12 - m11 * m02
        c11 = m00 * m22 - m02 * m02
        c12 = -(m00 * m12 - m01 * m02)
        c22 = m00 * m11 - m01 * m01
        det = m00 * c00 + m01 * c01 + m02 * c02

        p0 = float(0.0)
        p1 = float(0.0)
        p2 = float(0.0)
        trace = m00 + m11 + m22
        stability_scale = trace * trace * trace + 1.0e-20
        if wp.abs(det) > 1.0e-8 * stability_scale:
            inv_det = 1.0 / det
            inv00 = c00 * inv_det
            inv01 = c01 * inv_det
            inv02 = c02 * inv_det
            inv11 = c11 * inv_det
            inv12 = c12 * inv_det
            inv22 = c22 * inv_det
            go0 = grad_output[i, 0]
            go1 = grad_output[i, 1]
            go2 = grad_output[i, 2]
            p0 = inv00 * go0 + inv01 * go1 + inv02 * go2
            p1 = inv01 * go0 + inv11 * go1 + inv12 * go2
            p2 = inv02 * go0 + inv12 * go1 + inv22 * go2

        self_contrib = float(0.0)
        for p in range(start, end):
            n = indices[p]
            dx = points[n, 0] - px
            dy = points[n, 1] - py
            dz = points[n, 2] - pz
            dist2 = dx * dx + dy * dy + dz * dz + 1.0e-20
            w = wp.pow(dist2, -0.5 * weight_power)
            c = w * (p0 * dx + p1 * dy + p2 * dz)
            wp.atomic_add(grad_values, n, c)
            self_contrib = self_contrib - c

        wp.atomic_add(grad_values, i, self_contrib)


def _launch_forward(
    *,
    points_fp32: torch.Tensor,
    values_flat_fp32: torch.Tensor,
    offsets_i32: torch.Tensor,
    indices_i32: torch.Tensor,
    weight_power: float,
    min_neighbors: int,
    reg_eps: float,
    grads_components: torch.Tensor,
    wp_device,
    wp_stream,
) -> None:
    ### Launch one LSQ forward kernel per value component.
    n_dims = points_fp32.shape[1]
    n_entities = points_fp32.shape[0]
    n_components = values_flat_fp32.shape[1]
    kernel = (
        _mesh_lsq_gradient_1d_kernel
        if n_dims == 1
        else _mesh_lsq_gradient_2d_kernel
        if n_dims == 2
        else _mesh_lsq_gradient_3d_kernel
    )

    wp_points = wp.from_torch(points_fp32, dtype=wp.float32)
    wp_offsets = wp.from_torch(offsets_i32, dtype=wp.int32)
    wp_indices = wp.from_torch(indices_i32, dtype=wp.int32)

    with wp.ScopedStream(wp_stream):
        for comp in range(n_components):
            wp.launch(
                kernel=kernel,
                dim=n_entities,
                inputs=[
                    wp_points,
                    wp.from_torch(values_flat_fp32[:, comp].contiguous(), dtype=wp.float32),
                    wp_offsets,
                    wp_indices,
                    float(weight_power),
                    int(min_neighbors),
                    float(reg_eps),
                    wp.from_torch(grads_components[comp], dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )


def _launch_backward(
    *,
    points_fp32: torch.Tensor,
    offsets_i32: torch.Tensor,
    indices_i32: torch.Tensor,
    grad_output_components_fp32: torch.Tensor,
    weight_power: float,
    min_neighbors: int,
    reg_eps: float,
    grad_values_flat: torch.Tensor,
    wp_device,
    wp_stream,
) -> None:
    ### Launch one LSQ backward kernel per value component.
    n_dims = points_fp32.shape[1]
    n_entities = points_fp32.shape[0]
    n_components = grad_output_components_fp32.shape[0]
    kernel = (
        _mesh_lsq_gradient_1d_backward_kernel
        if n_dims == 1
        else _mesh_lsq_gradient_2d_backward_kernel
        if n_dims == 2
        else _mesh_lsq_gradient_3d_backward_kernel
    )

    wp_points = wp.from_torch(points_fp32, dtype=wp.float32)
    wp_offsets = wp.from_torch(offsets_i32, dtype=wp.int32)
    wp_indices = wp.from_torch(indices_i32, dtype=wp.int32)

    with wp.ScopedStream(wp_stream):
        for comp in range(n_components):
            comp_grad_values = torch.zeros(
                (n_entities,),
                device=grad_values_flat.device,
                dtype=torch.float32,
            )
            wp.launch(
                kernel=kernel,
                dim=n_entities,
                inputs=[
                    wp_points,
                    wp_offsets,
                    wp_indices,
                    wp.from_torch(grad_output_components_fp32[comp], dtype=wp.float32),
                    float(weight_power),
                    int(min_neighbors),
                    float(reg_eps),
                    wp.from_torch(comp_grad_values, dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
            grad_values_flat[:, comp] = comp_grad_values


class _MeshLSQGradientWarpAutograd(torch.autograd.Function):
    ### Bridge warp LSQ kernels into torch autograd (value gradients).
    @staticmethod
    def forward(  # type: ignore[override]
        ctx,
        points: torch.Tensor,
        values: torch.Tensor,
        neighbor_offsets: torch.Tensor,
        neighbor_indices: torch.Tensor,
        weight_power: float,
        min_neighbors: int,
        reg_eps: float,
    ) -> torch.Tensor:
        points_fp32 = points.to(dtype=torch.float32).contiguous()
        values_fp32 = values.to(dtype=torch.float32).contiguous()
        offsets_i32 = neighbor_offsets.to(dtype=torch.int32, device=points.device).contiguous()
        indices_i32 = neighbor_indices.to(dtype=torch.int32, device=points.device).contiguous()

        n_entities = points_fp32.shape[0]
        n_dims = points_fp32.shape[1]
        values_flat = values_fp32.reshape(n_entities, -1)
        n_components = values_flat.shape[1]

        ### Store component-wise output as (C, N, dims) for contiguous warp writes.
        grads_components = torch.empty(
            (n_components, n_entities, n_dims),
            dtype=torch.float32,
            device=points.device,
        )

        wp_device, wp_stream = FunctionSpec.warp_launch_context(points_fp32)
        _launch_forward(
            points_fp32=points_fp32,
            values_flat_fp32=values_flat,
            offsets_i32=offsets_i32,
            indices_i32=indices_i32,
            weight_power=weight_power,
            min_neighbors=min_neighbors,
            reg_eps=reg_eps,
            grads_components=grads_components,
            wp_device=wp_device,
            wp_stream=wp_stream,
        )

        value_shape = values.shape[1:]
        output = grads_components.permute(1, 2, 0).reshape(n_entities, n_dims, *value_shape)
        if output.dtype != values.dtype:
            output = output.to(dtype=values.dtype)

        ### Save tensors/metadata needed for backward.
        ctx.save_for_backward(points_fp32, offsets_i32, indices_i32)
        ctx.value_shape = values.shape
        ctx.values_dtype = values.dtype
        ctx.weight_power = float(weight_power)
        ctx.min_neighbors = int(min_neighbors)
        ctx.reg_eps = float(reg_eps)
        return output

    @staticmethod
    def backward(ctx, grad_output: torch.Tensor):  # type: ignore[override]
        if grad_output is None:
            return None, None, None, None, None, None, None

        points_fp32, offsets_i32, indices_i32 = ctx.saved_tensors
        grad_output_fp32 = grad_output.to(dtype=torch.float32).contiguous()
        values_shape = ctx.value_shape
        n_entities = values_shape[0]
        value_shape = values_shape[1:]
        n_components = int(torch.tensor(value_shape).prod().item()) if value_shape else 1

        grad_output_components = grad_output_fp32.reshape(n_entities, grad_output_fp32.shape[1], n_components)
        grad_output_components = grad_output_components.permute(2, 0, 1).contiguous()
        grad_values_flat = torch.empty(
            (n_entities, n_components),
            device=grad_output.device,
            dtype=torch.float32,
        )

        wp_device, wp_stream = FunctionSpec.warp_launch_context(grad_output_fp32)
        _launch_backward(
            points_fp32=points_fp32,
            offsets_i32=offsets_i32,
            indices_i32=indices_i32,
            grad_output_components_fp32=grad_output_components,
            weight_power=ctx.weight_power,
            min_neighbors=ctx.min_neighbors,
            reg_eps=ctx.reg_eps,
            grad_values_flat=grad_values_flat,
            wp_device=wp_device,
            wp_stream=wp_stream,
        )

        grad_values = grad_values_flat.reshape(values_shape)
        if grad_values.dtype != ctx.values_dtype:
            grad_values = grad_values.to(dtype=ctx.values_dtype)
        return None, grad_values, None, None, None, None, None


def mesh_lsq_gradient_warp(
    points: torch.Tensor,
    values: torch.Tensor,
    neighbor_offsets: torch.Tensor,
    neighbor_indices: torch.Tensor,
    weight_power: float = 2.0,
    min_neighbors: int = 0,
    reg_eps: float = 1.0e-6,
) -> torch.Tensor:
    ### Ensure Warp backend is available before dispatch.
    if not _WARP_AVAILABLE:
        raise ImportError(
            "mesh_lsq_gradient warp backend requires warp>=0.6.0"
        ) from _WARP_IMPORT_ERROR

    ### Validate inputs before launching kernels.
    _validate_inputs(
        points=points,
        values=values,
        neighbor_offsets=neighbor_offsets,
        neighbor_indices=neighbor_indices,
        min_neighbors=min_neighbors,
    )
    if points.requires_grad:
        raise ValueError("warp mesh_lsq_gradient currently supports gradients w.r.t values only")

    ### Dispatch through an autograd-capable wrapper around Warp launches.
    return _MeshLSQGradientWarpAutograd.apply(
        points,
        values,
        neighbor_offsets,
        neighbor_indices,
        float(weight_power),
        int(min_neighbors),
        float(reg_eps),
    )
