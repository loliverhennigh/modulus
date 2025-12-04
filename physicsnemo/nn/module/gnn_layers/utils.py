# SPDX-FileCopyrightText: Copyright (c) 2023 - 2025 NVIDIA CORPORATION & AFFILIATES.
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

import importlib
from typing import Any, Callable, Dict, Tuple, Union

import torch
from torch import Tensor
from torch.utils.checkpoint import checkpoint

from physicsnemo.nn.gnn_layers.graph import CuGraphCSC
from physicsnemo.nn.gnn_layers.graph_types import (
    CUGRAPH_OPS_AVAILABLE,
    PYG_AVAILABLE,
    GraphType,
    raise_missing_pyg_error,
)

if CUGRAPH_OPS_AVAILABLE:
    pylibcugraphops_operators = importlib.import_module(
        "pylibcugraphops.pytorch.operators"
    )
    agg_concat_e2n = pylibcugraphops_operators.agg_concat_e2n
    update_efeat_bipartite_e2e = pylibcugraphops_operators.update_efeat_bipartite_e2e
    update_efeat_static_e2e = pylibcugraphops_operators.update_efeat_static_e2e
else:
    agg_concat_e2n = None
    update_efeat_bipartite_e2e = None
    update_efeat_static_e2e = None


def checkpoint_identity(layer: Callable, *args: Any, **kwargs: Any) -> Any:
    """Applies the identity function for checkpointing.

    This function serves as an identity function for use with model layers
    when checkpointing is not enabled. It simply forwards the input arguments
    to the specified layer and returns its output.

    Parameters
    ----------
    layer : Callable
        The model layer or function to apply to the input arguments.
    *args
        Positional arguments to be passed to the layer.
    **kwargs
        Keyword arguments to be passed to the layer.

    Returns
    -------
    Any
        The output of the specified layer after processing the input arguments.
    """
    return layer(*args)


def set_checkpoint_fn(do_checkpointing: bool) -> Callable:
    """Sets checkpoint function.

    This function returns the appropriate checkpoint function based on the
    provided `do_checkpointing` flag. If `do_checkpointing` is True, the
    function returns the checkpoint function from PyTorch's
    `torch.utils.checkpoint`. Otherwise, it returns an identity function
    that simply passes the inputs through the given layer.

    Parameters
    ----------
    do_checkpointing : bool
        Whether to use checkpointing for gradient computation. Checkpointing
        can reduce memory usage during backpropagation at the cost of
        increased computation time.

    Returns
    -------
    Callable
        The selected checkpoint function to use for gradient computation.
    """
    if do_checkpointing:
        return checkpoint
    else:
        return checkpoint_identity


if PYG_AVAILABLE:
    pyg_data = importlib.import_module("torch_geometric.data")
    torch_scatter = importlib.import_module("torch_scatter")
    PyGData = pyg_data.Data
    PyGHeteroData = pyg_data.HeteroData

    def concat_message_function(edges: Tensor) -> Dict[str, Tensor]:
        """Concatenates source node, destination node, and edge features.

        Parameters
        ----------
        edges : Tensor
            Edges.

        Returns
        -------
        Dict[Tensor]
            Concatenated source node, destination node, and edge features.
        """
        # concats src node , dst node, and edge features
        cat_feat = torch.cat((edges.data["x"], edges.src["x"], edges.dst["x"]), dim=1)
        return {"cat_feat": cat_feat}

    def concat_efeat_pyg(
        efeat: Tensor,
        nfeat: Union[Tensor, Tuple[Tensor, Tensor]],
        graph: PyGData | PyGHeteroData,
    ) -> Tensor:
        """Concatenates edge features with source and destination node features.
        Use for PyG graphs.

        Parameters
        ----------
        efeat : Tensor
            Edge features.
        nfeat : Tensor | Tuple[Tensor]
            Node features.
        graph : PyGData
            Graph.

        Returns
        -------
        Tensor
            Concatenated edge features with source and destination node features.
        """
        src_feat, dst_feat = nfeat if isinstance(nfeat, Tuple) else (nfeat, nfeat)
        if isinstance(graph, PyGHeteroData):
            src_idx, dst_idx = graph[graph.edge_types[0]].edge_index.long()
        else:
            src_idx, dst_idx = graph.edge_index.long()
        cat_feat = torch.cat((efeat, src_feat[src_idx], dst_feat[dst_idx]), dim=1)
        return cat_feat

    def concat_efeat(
        efeat: Tensor,
        nfeat: Union[Tensor, Tuple[Tensor]],
        graph: GraphType,
    ) -> Tensor:
        """Concatenates edge features with source and destination node features.
        Use for homogeneous graphs.

        Parameters
        ----------
        efeat : Tensor
            Edge features.
        nfeat : Tensor | Tuple[Tensor]
            Node features.
        graph : GraphType
            Graph.

        Returns
        -------
        Tensor
            Concatenated edge features with source and destination node features.
        """
        if isinstance(nfeat, Tensor):
            if isinstance(graph, CuGraphCSC):
                if graph.is_distributed:
                    src_feat = graph.get_src_node_features_in_local_graph(nfeat)
                    # torch.int64 to avoid indexing overflows due tu current behavior of cugraph-ops
                    bipartite_graph = graph.to_bipartite_csc(dtype=torch.int64)
                    dst_feat = nfeat
                    efeat = update_efeat_bipartite_e2e(
                        efeat, src_feat, dst_feat, bipartite_graph, "concat"
                    )

                else:
                    static_graph = graph.to_static_csc()
                    efeat = update_efeat_static_e2e(
                        efeat,
                        nfeat,
                        static_graph,
                        mode="concat",
                        use_source_emb=True,
                        use_target_emb=True,
                    )
            elif isinstance(graph, (PyGData, PyGHeteroData)):
                efeat = concat_efeat_pyg(efeat, nfeat, graph)
            else:
                raise ValueError(f"Unsupported graph type: {type(graph)}")
        elif isinstance(nfeat, Tuple):
            src_feat, dst_feat = nfeat
            # update edge features through concatenating edge and node features
            if isinstance(graph, CuGraphCSC):
                if graph.is_distributed:
                    src_feat = graph.get_src_node_features_in_local_graph(src_feat)
                # torch.int64 to avoid indexing overflows due tu current behavior of cugraph-ops
                bipartite_graph = graph.to_bipartite_csc(dtype=torch.int64)
                efeat = update_efeat_bipartite_e2e(
                    efeat, src_feat, dst_feat, bipartite_graph, "concat"
                )
            elif isinstance(graph, (PyGData, PyGHeteroData)):
                efeat = concat_efeat_pyg(efeat, (src_feat, dst_feat), graph)
            else:
                raise ValueError(f"Unsupported graph type: {type(graph)}")
        else:
            raise ValueError(f"Unsupported node feature type: {type(nfeat)}")

        return efeat

    def concat_efeat_hetero(
        mesh_efeat: Tensor,
        world_efeat: Tensor,
        nfeat: Union[Tensor, Tuple[Tensor, Tensor]],
        graph: GraphType,
    ) -> Tensor:
        """Concatenates edge features with source and destination node features.
        Use for heterogeneous graphs.
        """

        if isinstance(graph, CuGraphCSC):
            raise NotImplementedError(
                "concat_efeat_hetero is not supported for CuGraphCSC graphs yet."
            )
        elif isinstance(graph, PyGData):
            efeat = concat_efeat_pyg(
                torch.cat((mesh_efeat, world_efeat), dim=0), nfeat, graph
            )
        else:
            raise ValueError(f"Unsupported graph type: {type(graph)}")

        return efeat

    @torch.jit.script
    def sum_edge_node_feat(
        efeat: Tensor,
        src_feat: Tensor,
        dst_feat: Tensor,
        src_idx: Tensor,
        dst_idx: Tensor,
    ) -> Tensor:
        """Sums edge features with source and destination node features.

        Parameters
        ----------
        efeat : Tensor
            Edge features.
        src_feat : Tensor
            Source node features.
        dst_feat : Tensor
            Destination node features.
        src_idx : Tensor
            Source node indices.
        dst_idx : Tensor
            Destination node indices.

        Returns
        -------
        Tensor
            Sum of edge features with source and destination node features.
        """

        return efeat + src_feat[src_idx] + dst_feat[dst_idx]

    def sum_efeat(
        efeat: Tensor,
        nfeat: Union[Tensor, Tuple[Tensor]],
        graph: GraphType,
    ):
        """Sums edge features with source and destination node features.

        Parameters
        ----------
        efeat : Tensor
            Edge features.
        nfeat : Tensor | Tuple[Tensor]
            Node features (static setting) or tuple of node features of
            source and destination nodes (bipartite setting).
        graph : GraphType
            The underlying graph.

        Returns
        -------
        Tensor
            Sum of edge features with source and destination node features.
        """
        if isinstance(nfeat, Tensor):
            if isinstance(graph, CuGraphCSC):
                if graph.is_distributed:
                    src_feat = graph.get_src_node_features_in_local_graph(nfeat)
                    dst_feat = nfeat
                    bipartite_graph = graph.to_bipartite_csc()
                    sum_efeat = update_efeat_bipartite_e2e(
                        efeat, src_feat, dst_feat, bipartite_graph, mode="sum"
                    )

                else:
                    static_graph = graph.to_static_csc()
                    sum_efeat = update_efeat_bipartite_e2e(
                        efeat, nfeat, static_graph, mode="sum"
                    )
            elif isinstance(graph, PyGData):
                src_feat, dst_feat = nfeat, nfeat
                src, dst = graph.edge_index.long()
                sum_efeat = sum_edge_node_feat(efeat, src_feat, dst_feat, src, dst)
            else:
                raise ValueError(f"Unsupported graph type: {type(graph)}")
        else:
            src_feat, dst_feat = nfeat
            if isinstance(graph, CuGraphCSC):
                if graph.is_distributed:
                    src_feat = graph.get_src_node_features_in_local_graph(src_feat)

                bipartite_graph = graph.to_bipartite_csc()
                sum_efeat = update_efeat_bipartite_e2e(
                    efeat, src_feat, dst_feat, bipartite_graph, mode="sum"
                )
            elif isinstance(graph, (PyGData, PyGHeteroData)):
                if isinstance(graph, PyGHeteroData):
                    src, dst = graph[graph.edge_types[0]].edge_index.long()
                else:
                    src, dst = graph.edge_index.long()
                sum_efeat = sum_edge_node_feat(efeat, src_feat, dst_feat, src, dst)
            else:
                raise ValueError(f"Unsupported graph type: {type(graph)}")

        return sum_efeat

    def agg_concat_pyg(
        efeat: Tensor,
        nfeat: Tensor,
        graph: PyGData | PyGHeteroData,
        aggregation: str,
    ) -> Tensor:
        if isinstance(graph, PyGHeteroData):
            _, dst = graph[graph.edge_types[0]].edge_index.long()
        else:
            _, dst = graph.edge_index.long()
        h_dest = torch_scatter.scatter(
            efeat, dst, dim=0, dim_size=nfeat.shape[0], reduce=aggregation
        )
        cat_feat = torch.cat((h_dest, nfeat), -1)
        return cat_feat

    def aggregate_and_concat(
        efeat: Tensor,
        nfeat: Tensor,
        graph: GraphType,
        aggregation: str,
    ):
        """
        Aggregates edge features and concatenates result with destination node features.

        Parameters
        ----------
        efeat : Tensor
            Edge features.
        nfeat : Tensor
            Node features (destination nodes).
        graph : GraphType
            Graph.
        aggregation : str
            Aggregation method (sum or mean).

        Returns
        -------
        Tensor
            Aggregated edge features concatenated with destination node features.

        Raises
        ------
        RuntimeError
            If aggregation method is not sum or mean.
        """

        if isinstance(graph, CuGraphCSC):
            # in this case, we don't have to distinguish a distributed setting
            # or the defalt setting as both efeat and nfeat are already
            # gurantueed to be on the same rank on both cases due to our
            # partitioning scheme
            static_graph = graph.to_static_csc()
            cat_feat = agg_concat_e2n(nfeat, efeat, static_graph, aggregation)
        elif isinstance(graph, (PyGData, PyGHeteroData)):
            cat_feat = agg_concat_pyg(efeat, nfeat, graph, aggregation)
        else:
            raise ValueError(f"Unsupported graph type: {type(graph)}")

        return cat_feat

    def aggregate_and_concat_hetero(
        mesh_efeat: Tensor,
        world_efeat: Tensor,
        nfeat: Tensor,
        graph: GraphType,
        aggregation: str,
    ):
        """
        Aggregates edge features and concatenates result with destination node features.
        Use for heterogeneous graphs.

        Parameters
        ----------
        mesh_efeat : Tensor
            Mesh edge features.
        world_efeat : Tensor
            World edge features.
        nfeat : Tensor
            Node features (destination nodes).
        graph : GraphType
            Graph.
        aggregation : str
            Aggregation method (sum or mean).

        Returns
        -------
        Tensor
            Aggregated edge features concatenated with destination node features.

        Raises
        ------
        RuntimeError
            If aggregation method is not sum or mean.
        """

        if isinstance(graph, CuGraphCSC):
            raise NotImplementedError(
                "aggregate_and_concat_hetero is not supported for CuGraphCSC graphs yet."
            )
        elif isinstance(graph, PyGData):
            cat_feat = agg_concat_pyg(
                torch.cat((mesh_efeat, world_efeat), dim=0), nfeat, graph, aggregation
            )
        else:
            raise ValueError(f"Unsupported graph type: {type(graph)}")

        return cat_feat


else:

    def concat_message_function(*args, **kwargs):
        """Placeholder for when PyG is not available."""
        raise_missing_pyg_error()

    def concat_efeat_pyg(*args, **kwargs):
        """Placeholder for when PyG is not available."""
        raise_missing_pyg_error()

    def concat_efeat(*args, **kwargs):
        """Placeholder for when PyG is not available."""
        raise_missing_pyg_error()

    def concat_efeat_hetero(*args, **kwargs):
        """Placeholder for when PyG is not available."""
        raise_missing_pyg_error()

    def sum_edge_node_feat(*args, **kwargs):
        """Placeholder for when PyG is not available."""
        raise_missing_pyg_error()

    def sum_efeat(*args, **kwargs):
        """Placeholder for when PyG is not available."""
        raise_missing_pyg_error()

    def agg_concat_pyg(*args, **kwargs):
        """Placeholder for when PyG is not available."""
        raise_missing_pyg_error()

    def aggregate_and_concat(*args, **kwargs):
        """Placeholder for when PyG is not available."""
        raise_missing_pyg_error()

    def aggregate_and_concat_hetero(*args, **kwargs):
        """Placeholder for when PyG is not available."""
        raise_missing_pyg_error()
