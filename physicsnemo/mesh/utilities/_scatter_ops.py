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

"""Scatter operation utilities for aggregating data across mesh elements.

This module provides unified scatter-based aggregation operations that are
commonly used throughout physicsnemo.mesh for transferring data between different
mesh entities (points, cells, facets).
"""

import torch
from jaxtyping import Float, Int
from torch.distributed.tensor.placement_types import Shard

from physicsnemo.distributed.utils import compute_split_shapes
from physicsnemo.mesh.utilities._tolerances import safe_eps


def _is_shard_tensor(tensor: object) -> bool:
    """Return whether ``tensor`` is a ShardTensor instance."""
    return (
        tensor.__class__.__name__ == "ShardTensor"
        and hasattr(tensor, "full_tensor")
        and hasattr(tensor, "_spec")
    )


def _materialize_shard_tensor(tensor: torch.Tensor | None) -> torch.Tensor | None:
    """Gather ShardTensor inputs before running dense scatter operations."""
    if tensor is not None and _is_shard_tensor(tensor):
        return tensor.full_tensor()
    return tensor


def _redistribute_like_template(
    tensor: torch.Tensor, template: torch.Tensor
) -> torch.Tensor:
    """Return ``tensor`` as a ShardTensor with ``template``'s placement."""
    if not _is_shard_tensor(template):
        raise RuntimeError("ShardTensor runtime is unavailable")

    shard_tensor_cls = type(template)
    mesh = template.device_mesh
    placements = template._spec.placements

    if not any(isinstance(placement, Shard) for placement in placements):
        return shard_tensor_cls.from_local(
            tensor,
            mesh,
            placements,
        )

    local_tensor = tensor
    sharding_shapes: dict[int, list[tuple[int, ...]]] = {}
    coordinate = mesh.get_coordinate()
    for mesh_dim, placement in enumerate(placements):
        if not isinstance(placement, Shard):
            continue

        tensor_dim = placement.dim
        split_sizes = compute_split_shapes(
            local_tensor.shape[tensor_dim], mesh.size(mesh_dim)
        )
        split_starts = []
        running_sum = 0
        for split_size in split_sizes[:-1]:
            running_sum += split_size
            split_starts.append(running_sum)

        chunks = torch.tensor_split(local_tensor, split_starts, dim=tensor_dim)
        sharding_shapes[mesh_dim] = [tuple(chunk.shape) for chunk in chunks]
        local_tensor = chunks[coordinate[mesh_dim]].contiguous()

    return shard_tensor_cls.from_local(
        local_tensor,
        mesh,
        placements,
        sharding_shapes=sharding_shapes,
    )


def _first_shard_tensor(*tensors: torch.Tensor | None) -> torch.Tensor | None:
    """Return the first ShardTensor in ``tensors``, if any."""
    for tensor in tensors:
        if _is_shard_tensor(tensor):
            return tensor
    return None


def _scatter_aggregate_dense(
    src_data: Float[torch.Tensor, "n_src ..."],
    src_to_dst_mapping: Int[torch.Tensor, " n_src"],
    n_dst: int,
    weights: Float[torch.Tensor, " n_src"] | None = None,
    aggregation: str = "mean",
) -> Float[torch.Tensor, "n_dst ..."]:
    """Dense implementation for :func:`scatter_aggregate`."""
    dtype = src_data.dtype

    ### Get data shape beyond the first dimension
    data_shape = src_data.shape[1:]

    if aggregation not in ("mean", "sum"):
        raise ValueError(f"Invalid {aggregation=}. Must be 'mean' or 'sum'.")

    ### Fast path: unweighted sum is a single scatter_add_ with no extra work
    if weights is None and aggregation == "sum":
        aggregated_data = src_data.new_zeros((n_dst, *data_shape), dtype=dtype)
        expanded_indices = src_to_dst_mapping.view(
            -1, *([1] * len(data_shape))
        ).expand_as(src_data)
        aggregated_data.scatter_add_(dim=0, index=expanded_indices, src=src_data)
        return aggregated_data

    ### Initialize weights if not provided
    if weights is None:
        weights = torch.ones_like(src_to_dst_mapping, dtype=dtype)

    ### Ensure weights have same dtype as data (avoid dtype mismatch in multiplication)
    if weights.dtype != dtype:
        weights = weights.to(dtype)

    ### Weight the source data
    # Broadcast weights to match data shape: (n_src, *data_shape)
    weight_shape = [len(weights)] + [1] * len(data_shape)
    weighted_data = src_data * weights.view(weight_shape)

    ### Scatter-add weighted data to destinations
    aggregated_data = src_data.new_zeros((n_dst, *data_shape), dtype=dtype)

    # Expand src_to_dst_mapping to match data dimensions
    expanded_indices = src_to_dst_mapping.view(-1, *([1] * len(data_shape))).expand_as(
        weighted_data
    )

    aggregated_data.scatter_add_(
        dim=0,
        index=expanded_indices,
        src=weighted_data,
    )

    ### Normalize weighted sum to weighted mean
    if aggregation == "mean":
        ### Compute sum of weights at each destination
        weight_sums = src_data.new_zeros((n_dst,), dtype=dtype)
        weight_sums.scatter_add_(
            dim=0,
            index=src_to_dst_mapping,
            src=weights,
        )

        ### Normalize by total weight (avoid division by zero)
        weight_sums = weight_sums.clamp(min=safe_eps(weight_sums.dtype))
        aggregated_data = aggregated_data / weight_sums.view(
            -1, *([1] * len(data_shape))
        )

    return aggregated_data


def scatter_aggregate(
    src_data: Float[torch.Tensor, "n_src ..."],
    src_to_dst_mapping: Int[torch.Tensor, " n_src"],
    n_dst: int,
    weights: Float[torch.Tensor, " n_src"] | None = None,
    aggregation: str = "mean",
) -> Float[torch.Tensor, "n_dst ..."]:
    """Aggregate source data to destination using scatter operations.

    This is the core scatter-based aggregation pattern used throughout physicsnemo.mesh
    for operations like:

    - Aggregating cell data to points
    - Aggregating parent cell data to facets
    - Merging duplicate point data

    The pattern is:
    1. Initialize destination tensor with zeros
    2. Scatter-add weighted source data to destinations
    3. Scatter-add weights to compute normalization
    4. Divide aggregated data by total weights

    ShardTensor inputs are gathered to run the dense reference scatter operation,
    then redistributed back to the source tensor placement. This keeps mesh
    transfer paths working while ShardTensor does not have a native
    ``scatter_add_`` sharding rule.

    Parameters
    ----------
    src_data : torch.Tensor
        Source data to aggregate, shape (n_src, *data_shape).
    src_to_dst_mapping : torch.Tensor
        Mapping from each source to its destination index,
        shape (n_src,). Each value should be in [0, n_dst).
    n_dst : int
        Number of destination elements.
    weights : torch.Tensor or None
        Optional weights for each source element, shape (n_src,).
        If None, uses uniform weights of 1.0.
    aggregation : str
        Aggregation mode:

        - "mean": Weighted mean (uses weights if provided, uniform otherwise)
        - "sum": Weighted sum (no normalization)

    Returns
    -------
    torch.Tensor
        Aggregated data at destinations, shape (n_dst, *data_shape).
        For "mean" mode, values are weighted averages.
        For "sum" mode, values are weighted sums.

    Examples
    --------
    >>> # Aggregate cell data to points
    >>> src_data = torch.tensor([[1.0], [2.0], [3.0]])  # 3 cells
    >>> src_to_dst = torch.tensor([0, 0, 1])  # map to 2 points
    >>> result = scatter_aggregate(src_data, src_to_dst, n_dst=2)
    >>> # result = [[1.5], [3.0]]  # point 0 gets mean of cells 0,1
    """
    shard_template = _first_shard_tensor(src_data, weights, src_to_dst_mapping)
    if shard_template is None:
        return _scatter_aggregate_dense(
            src_data=src_data,
            src_to_dst_mapping=src_to_dst_mapping,
            n_dst=n_dst,
            weights=weights,
            aggregation=aggregation,
        )

    dense_result = _scatter_aggregate_dense(
        src_data=_materialize_shard_tensor(src_data),
        src_to_dst_mapping=_materialize_shard_tensor(src_to_dst_mapping),
        n_dst=n_dst,
        weights=_materialize_shard_tensor(weights),
        aggregation=aggregation,
    )
    return _redistribute_like_template(dense_result, shard_template)
