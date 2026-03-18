# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import torch

from physicsnemo.core.function_spec import FunctionSpec

from .utils import build_geometry, validate_inputs

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

    ### ============================================================
    ### Forward kernels (Green-Gauss reconstruction per component)
    ### ============================================================

    @wp.kernel
    def _mesh_green_gauss_2d_forward_kernel(
        values: wp.array(dtype=wp.float32),
        coeff: wp.array3d(dtype=wp.float32),
        neighbors: wp.array2d(dtype=wp.int32),
        gradients: wp.array2d(dtype=wp.float32),
    ):
        i = wp.tid()
        n_faces = coeff.shape[1]

        vi = values[i]
        gx = float(0.0)
        gy = float(0.0)
        for f in range(n_faces):
            j = neighbors[i, f]
            phi_f = vi
            if j >= 0:
                phi_f = 0.5 * (vi + values[j])

            gx = gx + coeff[i, f, 0] * phi_f
            gy = gy + coeff[i, f, 1] * phi_f

        gradients[i, 0] = gx
        gradients[i, 1] = gy

    @wp.kernel
    def _mesh_green_gauss_3d_forward_kernel(
        values: wp.array(dtype=wp.float32),
        coeff: wp.array3d(dtype=wp.float32),
        neighbors: wp.array2d(dtype=wp.int32),
        gradients: wp.array2d(dtype=wp.float32),
    ):
        i = wp.tid()
        n_faces = coeff.shape[1]

        vi = values[i]
        gx = float(0.0)
        gy = float(0.0)
        gz = float(0.0)
        for f in range(n_faces):
            j = neighbors[i, f]
            phi_f = vi
            if j >= 0:
                phi_f = 0.5 * (vi + values[j])

            gx = gx + coeff[i, f, 0] * phi_f
            gy = gy + coeff[i, f, 1] * phi_f
            gz = gz + coeff[i, f, 2] * phi_f

        gradients[i, 0] = gx
        gradients[i, 1] = gy
        gradients[i, 2] = gz

    ### ============================================================
    ### Backward kernels (adjoint wrt cell-centered values)
    ### ============================================================

    @wp.kernel
    def _mesh_green_gauss_2d_backward_kernel(
        grad_output: wp.array2d(dtype=wp.float32),
        coeff: wp.array3d(dtype=wp.float32),
        neighbors: wp.array2d(dtype=wp.int32),
        grad_values: wp.array(dtype=wp.float32),
    ):
        i, f = wp.tid()
        j = neighbors[i, f]

        dot_go = grad_output[i, 0] * coeff[i, f, 0] + grad_output[i, 1] * coeff[i, f, 1]
        owner_contrib = dot_go
        if j >= 0:
            owner_contrib = 0.5 * dot_go

        wp.atomic_add(grad_values, i, owner_contrib)
        if j >= 0:
            wp.atomic_add(grad_values, j, 0.5 * dot_go)

    @wp.kernel
    def _mesh_green_gauss_3d_backward_kernel(
        grad_output: wp.array2d(dtype=wp.float32),
        coeff: wp.array3d(dtype=wp.float32),
        neighbors: wp.array2d(dtype=wp.int32),
        grad_values: wp.array(dtype=wp.float32),
    ):
        i, f = wp.tid()
        j = neighbors[i, f]

        dot_go = (
            grad_output[i, 0] * coeff[i, f, 0]
            + grad_output[i, 1] * coeff[i, f, 1]
            + grad_output[i, 2] * coeff[i, f, 2]
        )
        owner_contrib = dot_go
        if j >= 0:
            owner_contrib = 0.5 * dot_go

        wp.atomic_add(grad_values, i, owner_contrib)
        if j >= 0:
            wp.atomic_add(grad_values, j, 0.5 * dot_go)


def _launch_forward(
    *,
    values_flat_fp32: torch.Tensor,
    coeff_fp32: torch.Tensor,
    neighbors_i32: torch.Tensor,
    grads_components: torch.Tensor,
    dims: int,
    wp_device,
    wp_stream,
) -> None:
    ### Launch one forward kernel per value component.
    kernel = (
        _mesh_green_gauss_2d_forward_kernel
        if dims == 2
        else _mesh_green_gauss_3d_forward_kernel
    )
    n_cells = values_flat_fp32.shape[0]
    n_components = values_flat_fp32.shape[1]

    wp_coeff = wp.from_torch(coeff_fp32, dtype=wp.float32)
    wp_neighbors = wp.from_torch(neighbors_i32, dtype=wp.int32)

    with wp.ScopedStream(wp_stream):
        for comp in range(n_components):
            wp.launch(
                kernel=kernel,
                dim=n_cells,
                inputs=[
                    wp.from_torch(values_flat_fp32[:, comp].contiguous(), dtype=wp.float32),
                    wp_coeff,
                    wp_neighbors,
                    wp.from_torch(grads_components[comp], dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )


def _launch_backward(
    *,
    grad_output_components_fp32: torch.Tensor,
    coeff_fp32: torch.Tensor,
    neighbors_i32: torch.Tensor,
    grad_values_flat: torch.Tensor,
    dims: int,
    wp_device,
    wp_stream,
) -> None:
    ### Launch one backward kernel per value component.
    kernel = (
        _mesh_green_gauss_2d_backward_kernel
        if dims == 2
        else _mesh_green_gauss_3d_backward_kernel
    )
    n_cells = grad_output_components_fp32.shape[1]
    n_faces = coeff_fp32.shape[1]
    n_components = grad_output_components_fp32.shape[0]

    wp_coeff = wp.from_torch(coeff_fp32, dtype=wp.float32)
    wp_neighbors = wp.from_torch(neighbors_i32, dtype=wp.int32)

    with wp.ScopedStream(wp_stream):
        for comp in range(n_components):
            comp_grad_values = torch.zeros(
                (n_cells,),
                device=grad_values_flat.device,
                dtype=torch.float32,
            )
            wp.launch(
                kernel=kernel,
                dim=(n_cells, n_faces),
                inputs=[
                    wp.from_torch(grad_output_components_fp32[comp], dtype=wp.float32),
                    wp_coeff,
                    wp_neighbors,
                    wp.from_torch(comp_grad_values, dtype=wp.float32),
                ],
                device=wp_device,
                stream=wp_stream,
            )
            grad_values_flat[:, comp] = comp_grad_values


class _MeshGreenGaussWarpAutograd(torch.autograd.Function):
    ### Wrap Warp Green-Gauss forward/backward kernels for torch autograd.
    @staticmethod
    def forward(  # type: ignore[override]
        ctx,
        points: torch.Tensor,
        cells: torch.Tensor,
        values: torch.Tensor,
    ) -> torch.Tensor:
        ### Geometry is treated as fixed for Warp backward (value gradients only).
        if points.requires_grad:
            raise ValueError("warp mesh_green_gauss_gradient does not support gradients w.r.t. points")

        coeff, neighbors = build_geometry(points=points, cells=cells)
        coeff_fp32 = coeff.to(dtype=torch.float32).contiguous()
        neighbors_i32 = neighbors.to(dtype=torch.int32).contiguous()
        values_fp32 = values.to(dtype=torch.float32).contiguous()

        n_cells = values.shape[0]
        dims = points.shape[1]
        value_shape = values.shape[1:]
        values_flat_fp32 = values_fp32.reshape(n_cells, -1)
        n_components = values_flat_fp32.shape[1]

        ### Store per-component output as (C, N, dims) for contiguous writes.
        grads_components = torch.empty(
            (n_components, n_cells, dims),
            device=values.device,
            dtype=torch.float32,
        )

        wp_device, wp_stream = FunctionSpec.warp_launch_context(values_fp32)
        _launch_forward(
            values_flat_fp32=values_flat_fp32,
            coeff_fp32=coeff_fp32,
            neighbors_i32=neighbors_i32,
            grads_components=grads_components,
            dims=dims,
            wp_device=wp_device,
            wp_stream=wp_stream,
        )

        output = grads_components.permute(1, 2, 0).reshape(n_cells, dims, *value_shape)
        if output.dtype != values.dtype:
            output = output.to(dtype=values.dtype)

        ### Save tensors/metadata needed by backward.
        ctx.save_for_backward(coeff_fp32, neighbors_i32)
        ctx.value_shape = values.shape
        ctx.values_dtype = values.dtype
        ctx.dims = dims
        return output

    @staticmethod
    def backward(ctx, grad_output: torch.Tensor):  # type: ignore[override]
        coeff_fp32, neighbors_i32 = ctx.saved_tensors
        if grad_output is None:
            return None, None, None

        grad_output_fp32 = grad_output.to(dtype=torch.float32).contiguous()
        values_shape = ctx.value_shape
        n_cells = values_shape[0]
        value_shape = values_shape[1:]
        n_components = int(torch.tensor(value_shape).prod().item()) if value_shape else 1

        grad_output_components = grad_output_fp32.reshape(n_cells, ctx.dims, n_components)
        grad_output_components = grad_output_components.permute(2, 0, 1).contiguous()

        grad_values_flat = torch.empty(
            (n_cells, n_components),
            device=grad_output.device,
            dtype=torch.float32,
        )
        wp_device, wp_stream = FunctionSpec.warp_launch_context(grad_output_fp32)
        _launch_backward(
            grad_output_components_fp32=grad_output_components,
            coeff_fp32=coeff_fp32,
            neighbors_i32=neighbors_i32,
            grad_values_flat=grad_values_flat,
            dims=ctx.dims,
            wp_device=wp_device,
            wp_stream=wp_stream,
        )

        grad_values = grad_values_flat.reshape(values_shape)
        if grad_values.dtype != ctx.values_dtype:
            grad_values = grad_values.to(dtype=ctx.values_dtype)
        return None, None, grad_values


def mesh_green_gauss_gradient_warp(
    points: torch.Tensor,
    cells: torch.Tensor,
    values: torch.Tensor,
) -> torch.Tensor:
    ### Ensure Warp backend is available before dispatch.
    if not _WARP_AVAILABLE:
        raise ImportError(
            "mesh_green_gauss_gradient warp backend requires warp>=0.6.0"
        ) from _WARP_IMPORT_ERROR

    ### Validate inputs before launching kernels.
    validate_inputs(points=points, cells=cells, values=values)

    ### Dispatch through an autograd-capable wrapper.
    return _MeshGreenGaussWarpAutograd.apply(points, cells, values)
