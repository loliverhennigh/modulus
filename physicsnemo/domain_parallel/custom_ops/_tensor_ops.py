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

r"""Custom tensor operations for ShardTensor dispatch.

This module provides dispatch and function handlers for tensor operations
that need special handling when applied to ``ShardTensor`` objects. Handlers
are registered with both ``__torch_dispatch__`` (ATen level) and
``__torch_function__`` (Python level) on :class:`ShardTensor`.
"""

from __future__ import annotations

from typing import Any, Callable

import torch
from torch.distributed.tensor._dtensor_spec import TensorMeta
from torch.distributed.tensor.placement_types import (
    Shard,
)

from physicsnemo.domain_parallel._shard_tensor_spec import (
    ShardTensorSpec,
    _stride_from_contiguous_shape_C_style,
)
from physicsnemo.domain_parallel.shard_tensor import ShardTensor

aten = torch.ops.aten


def _extract_cross_inputs(
    args: tuple[Any, ...], kwargs: dict[str, Any]
) -> tuple[
    ShardTensor,
    ShardTensor,
]:
    r"""Extract and validate ShardTensor inputs for cross-product handlers."""
    input_tensor = args[0] if len(args) > 0 else kwargs.get("input")
    other_tensor = args[1] if len(args) > 1 else kwargs.get("other")

    _validate_cross_inputs(input_tensor, other_tensor)
    return input_tensor, other_tensor


def _validate_cross_inputs(input_tensor: Any, other_tensor: Any) -> None:
    r"""Validate ShardTensor inputs for cross-product handlers."""
    if not isinstance(input_tensor, ShardTensor) or not isinstance(
        other_tensor, ShardTensor
    ):
        raise RuntimeError(
            "cross with ShardTensor inputs requires both arguments to be ShardTensor."
        )

    if input_tensor._spec.mesh != other_tensor._spec.mesh:
        raise RuntimeError(
            "cross requires both ShardTensor inputs to share the same device mesh."
        )
    if input_tensor._spec.placements != other_tensor._spec.placements:
        raise RuntimeError(
            "cross requires both ShardTensor inputs to have identical placements."
        )


def _cross_result_from_local(
    input_tensor: ShardTensor,
    local_input: torch.Tensor,
    local_result: torch.Tensor,
) -> ShardTensor:
    r"""Wrap a local cross-product result with matching ShardTensor metadata."""
    sharding_shapes = (
        input_tensor._spec.sharding_shapes()
        if local_result.shape == local_input.shape
        else "infer"
    )
    return ShardTensor.from_local(
        local_result,
        input_tensor._spec.mesh,
        input_tensor._spec.placements,
        sharding_shapes=sharding_shapes,
    )


def _normalize_cross_dim(
    input_tensor: ShardTensor,
    dim: int | None,
    *,
    allow_none: bool,
    op_name: str,
) -> int:
    r"""Return the global cross-product dimension and reject invalid vector dims."""
    ndim = input_tensor.ndim
    if dim is None:
        if not allow_none:
            raise TypeError(f"{op_name}(): argument 'dim' must be int, not NoneType")
        try:
            normalized_dim = next(
                i for i, size in enumerate(input_tensor.shape) if size == 3
            )
        except StopIteration:
            raise RuntimeError(
                "torch.cross with ShardTensor dim=None requires an input dimension "
                "of size 3."
            )
    else:
        if not isinstance(dim, int):
            raise TypeError(
                f"{op_name}(): argument 'dim' must be int, not {type(dim).__name__}"
            )
        if dim < -ndim or dim >= ndim:
            raise IndexError(
                "Dimension out of range "
                f"(expected to be in range of [{-ndim}, {ndim - 1}], but got {dim})"
            )
        normalized_dim = dim if dim >= 0 else dim + ndim

    if any(
        isinstance(placement, Shard) and placement.dim == normalized_dim
        for placement in input_tensor._spec.placements
    ):
        raise RuntimeError(
            "cross with ShardTensor inputs is not supported along a sharded dimension."
        )
    return normalized_dim


def linalg_cross_wrapper(
    func: Callable,
    types: tuple[Any, ...],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> ShardTensor:
    r"""Functional wrapper for ``torch.linalg.cross`` on ShardTensor inputs."""
    if kwargs is None:
        kwargs = {}

    if kwargs.get("out", None) is not None:
        raise RuntimeError(
            "torch.linalg.cross(out=...) is not supported for ShardTensor."
        )

    input_tensor, other_tensor = _extract_cross_inputs(args, kwargs)
    dim = kwargs.get("dim", -1)
    if len(args) > 2:
        dim = args[2]
    dim = _normalize_cross_dim(
        input_tensor,
        dim,
        allow_none=False,
        op_name="linalg_cross",
    )

    local_input = input_tensor.to_local()
    local_result = torch.linalg.cross(
        local_input,
        other_tensor.to_local(),
        dim=dim,
    )
    return _cross_result_from_local(input_tensor, local_input, local_result)


def _linalg_cross_dispatch(
    input_tensor: ShardTensor,
    other_tensor: ShardTensor,
    *,
    dim: int = -1,
) -> ShardTensor:
    r"""ATen dispatch handler for ``aten.linalg_cross.default``."""
    _validate_cross_inputs(input_tensor, other_tensor)
    dim = _normalize_cross_dim(
        input_tensor,
        dim,
        allow_none=False,
        op_name="linalg_cross",
    )
    local_input = input_tensor.to_local()
    local_result = torch.linalg.cross(
        local_input,
        other_tensor.to_local(),
        dim=dim,
    )
    return _cross_result_from_local(input_tensor, local_input, local_result)


def cross_wrapper(
    func: Callable,
    types: tuple[Any, ...],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> ShardTensor:
    r"""Functional wrapper for ``torch.cross`` on ShardTensor inputs."""
    if kwargs is None:
        kwargs = {}

    if kwargs.get("out", None) is not None:
        raise RuntimeError("torch.cross(out=...) is not supported for ShardTensor.")

    input_tensor, other_tensor = _extract_cross_inputs(args, kwargs)
    dim = kwargs.get("dim", None)
    if len(args) > 2:
        dim = args[2]
    dim = _normalize_cross_dim(
        input_tensor,
        dim,
        allow_none=True,
        op_name="cross",
    )

    local_input = input_tensor.to_local()
    local_result = torch.cross(
        local_input,
        other_tensor.to_local(),
        dim=dim,
    )
    return _cross_result_from_local(input_tensor, local_input, local_result)


def _cross_dispatch(
    input_tensor: ShardTensor,
    other_tensor: ShardTensor,
    dim: int | None = None,
) -> ShardTensor:
    r"""ATen dispatch handler for ``aten.cross.default``."""
    _validate_cross_inputs(input_tensor, other_tensor)
    dim = _normalize_cross_dim(
        input_tensor,
        dim,
        allow_none=True,
        op_name="cross",
    )
    local_input = input_tensor.to_local()
    local_result = torch.cross(
        local_input,
        other_tensor.to_local(),
        dim=dim,
    )
    return _cross_result_from_local(input_tensor, local_input, local_result)


def _unbind_output_metadata(
    input_spec: ShardTensorSpec, dim: int
) -> tuple[int, list, dict[int, list[torch.Size]]]:
    r"""Compute the normalized dim, output placements, and sharding shapes for unbind.

    Validates that the unbind dimension is not sharded and does not use
    ``Partial`` placement, then returns the metadata needed to construct
    the output ``ShardTensor`` objects.

    Parameters
    ----------
    input_spec : ShardTensorSpec
        Specification of the input sharded tensor.
    dim : int
        Dimension along which to unbind (may be negative).

    Returns
    -------
    tuple[int, list, dict[int, list[torch.Size]]]
        - Normalized (non-negative) ``dim``.
        - Output placements (shard dims above ``dim`` shifted down by 1).
        - Output sharding shapes with the unbind dimension removed.

    Raises
    ------
    RuntimeError
        If attempting to unbind along a sharded dimension (not yet implemented).
        If attempting to unbind with ``Partial`` placement (not yet supported).
    """
    ndim = len(input_spec.shape)
    if dim < 0:
        dim = dim % ndim

    # if the unbind dimension is along a dimension that is sharded, we have to handle that.
    # If it's along an unsharded dimension, there is nearly nothing to do.
    input_placements = input_spec.placements
    shards = [s for s in input_placements if isinstance(s, Shard)]

    if dim in [i.dim for i in shards]:
        raise RuntimeError("No implementation for unbinding along sharding axis yet.")

    new_placements: list = []
    for p in input_placements:
        if p.is_replicate():
            new_placements.append(p)
        elif p.is_shard():
            if p.dim > dim:
                new_placements.append(Shard(p.dim - 1))
            else:
                new_placements.append(p)
        elif p.is_partial():
            raise RuntimeError("Partial placement not supported yet for unbind")

    out_sharding_shapes: dict[int, list[torch.Size]] = {
        mesh_dim: [
            torch.Size(list(cs[:dim]) + list(cs[dim + 1 :])) for cs in shard_shapes
        ]
        for mesh_dim, shard_shapes in input_spec.sharding_shapes().items()
    }

    return dim, new_placements, out_sharding_shapes


def _unbind_dispatch(tensor: ShardTensor, dim: int = 0) -> tuple[ShardTensor, ...]:
    r"""Dispatch handler for ``aten.unbind.int`` on :class:`ShardTensor`.

    Called at the ``__torch_dispatch__`` level (below autograd).  Operates
    directly on the local tensor and constructs output ``ShardTensor``
    objects with the correct metadata; the autograd engine above handles
    gradient tracking.

    Parameters
    ----------
    tensor : ShardTensor
        Input sharded tensor.
    dim : int, default=0
        Dimension along which to unbind.

    Returns
    -------
    tuple[ShardTensor, ...]
        Tuple of ShardTensors, one per slice along ``dim``.

    Note
    ----
    This handler is needed for operations like attention in Stormcast and other
    models that unbind tensors along non-sharded dimensions.
    """
    input_spec = tensor._spec
    dim, new_placements, out_sharding_shapes = _unbind_output_metadata(input_spec, dim)

    # We are reducing tensor rank and returning one tensor per slice
    original_shape = list(input_spec.shape)
    original_shape.pop(dim)

    output_spec = ShardTensorSpec(
        mesh=input_spec.mesh,
        placements=tuple(new_placements),
        tensor_meta=TensorMeta(
            torch.Size(tuple(original_shape)),
            stride=_stride_from_contiguous_shape_C_style(original_shape),
            dtype=input_spec.tensor_meta.dtype,
        ),
        _sharding_shapes={k: tuple(v) for k, v in out_sharding_shapes.items()},
    )

    local_results = aten.unbind.int(tensor._local_tensor, dim)

    return tuple(
        ShardTensor(
            local_result,
            output_spec,
            requires_grad=False,  # Adjusted after the dispatcher
        )
        for local_result in local_results
    )


def unbind_wrapper(
    func: Callable,
    types: tuple[Any, ...],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> tuple[ShardTensor, ...]:
    r"""Functional-level wrapper for ``torch.unbind`` on ShardTensor.

    This is a ``__torch_function__``-level intercept (above autograd).  It
    uses ``to_local()`` / ``from_local()`` so that the autograd graph is
    preserved through the unbind operation.

    Parameters
    ----------
    func : Callable
        The original function being wrapped (``torch.unbind`` or
        ``torch.Tensor.unbind``).
    types : tuple[Any, ...]
        Types of the input arguments (unused).
    args : tuple[Any, ...]
        Positional arguments. Expected ``(input,)`` or ``(input, dim)``.
    kwargs : dict[str, Any]
        Keyword arguments (may contain ``dim``).

    Returns
    -------
    tuple[ShardTensor, ...]
        Tuple of ShardTensors, one per slice along the unbind dimension.
    """
    input_tensor: ShardTensor = args[0]
    dim: int = args[1] if len(args) > 1 else kwargs.get("dim", 0)

    input_spec = input_tensor._spec
    dim, new_placements, out_sharding_shapes = _unbind_output_metadata(input_spec, dim)

    # to_local() / from_local() preserve the autograd graph
    local_input = input_tensor.to_local()
    local_results = torch.unbind(local_input, dim)

    return tuple(
        ShardTensor.from_local(
            local_result,
            input_spec.mesh,
            new_placements,
            out_sharding_shapes,
        )
        for local_result in local_results
    )


# Python-level function handlers (__torch_function__).
ShardTensor.register_function_handler(torch.linalg.cross, linalg_cross_wrapper)
ShardTensor.register_function_handler(aten.linalg_cross.default, linalg_cross_wrapper)
if hasattr(torch, "cross"):
    ShardTensor.register_function_handler(torch.cross, cross_wrapper)
ShardTensor.register_function_handler(torch.Tensor.cross, cross_wrapper)
ShardTensor.register_function_handler(aten.cross.default, cross_wrapper)
ShardTensor.register_function_handler(torch.unbind, unbind_wrapper)
ShardTensor.register_function_handler(torch.Tensor.unbind, unbind_wrapper)

# ATen-level dispatch handler (__torch_dispatch__).
ShardTensor.register_dispatch_handler(aten.linalg_cross.default, _linalg_cross_dispatch)
ShardTensor.register_dispatch_handler(aten.cross.default, _cross_dispatch)
ShardTensor.register_dispatch_handler(aten.unbind.int, _unbind_dispatch)
