# SPDX-FileCopyrightText: Copyright (c) 2023 - 2024 NVIDIA CORPORATION & AFFILIATES.
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


import numpy as np
import torch
import vtk

try:
    import nvidia.dali as dali
    import nvidia.dali.plugin.pytorch as dali_pth
except ImportError:
    raise ImportError(
        "DALI dataset requires NVIDIA DALI package to be installed. "
        + "The package can be installed at:\n"
        + "https://docs.nvidia.com/deeplearning/dali/user-guide/docs/installation.html"
    )

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple, Union

from torch import Tensor

from physicsnemo.datapipes.datapipe import Datapipe
from physicsnemo.datapipes.meta import DatapipeMetaData

from .readers import parse_vtk_polydata, parse_vtk_unstructuredgrid, read_cgns, read_vtp, read_vtu, read_vtm


@dataclass
class MetaData(DatapipeMetaData):
    name: str = "TransientMeshDatapipe"
    # Optimization
    auto_device: bool = True
    cuda_graphs: bool = True
    # Parallel
    ddp_sharding: bool = True


class TransientMeshDatapipe(Datapipe):
    """DALI data pipeline for transient mesh data

    The data is expected to be stored in the following format:
    data_dir/
    ├── sim_0001/
    │   ├── mesh_0001.vtm
    │   ├── mesh_0002.vtm
    │   └── ...
    ├── sim_0002/
    │   ├── mesh_0001.vtm
    │   ├── mesh_0002.vtm
    │   └── ...
    └── ...
    The data is returned as a tuple of vertices, attributes, and edges.
    An example of the data output a tuple of tensors:
      vertices : torch.Size([batch_size, sequence_length, num_vertices, dim])
      ux       : torch.Size([batch_size, sequence_length, num_vertices, 1])
      edges    : torch.Size([batch_size, sequence_length, num_edges, 2])

    Parameters
    ----------
    data_dir : str
        Root directory containing sub-folders for each simulation run.
    variables : List[str]
        Ordered list of variable names to read from each mesh file.
    num_variables : int
        Number of variables (channels) expected in ``variables``.
    file_format : str, optional
        Mesh file format, by default "vtm". Supported formats: "vtm", "vtp", "vtu", "cgns".
    stats_dir : Union[str, None], optional
        Directory holding ``global_means.npy`` and ``global_stds.npy`` files used for normalisation.
    sequence_length : int, optional
        Number of consecutive timesteps returned in each sample sequence, by default ``2``.
    batch_size : int, optional
        Samples per batch, by default ``1``.
    shuffle : bool, optional
        Shuffle sequences each epoch, by default ``True``.
    num_workers : int, optional
        Number of Python workers used by DALI external source, by default ``1``.
    device : Union[str, torch.device], optional
        Device on which the DALI pipeline runs, by default the first CUDA device.
    process_rank : int, optional
        Local rank id when using distributed training, by default ``0``.
    world_size : int, optional
        Total number of distributed processes, by default ``1``.
    cache_data : bool, optional
        Whether to cache parsed mesh data in memory, by default ``False``.
    """

    def __init__(
        self,
        data_dir: str,
        variables: List[str],
        num_variables: int,
        file_format: str = "vtp",
        stats_dir: Union[str, None] = None,
        sequence_length: int = 2,
        batch_size: int = 1,
        shuffle: bool = True,
        num_workers: int = 1,
        device: Union[str, torch.device] = "cuda",
        process_rank: int = 0,
        world_size: int = 1,
        cache_data: bool = False,
    ):
        super().__init__(meta=MetaData())
        self.file_format = file_format
        self.variables = variables
        self.num_variables = num_variables
        self.sequence_length = sequence_length
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.shuffle = shuffle
        self.data_dir = Path(data_dir)
        self.stats_dir = Path(stats_dir) if stats_dir is not None else None
        self.process_rank = process_rank
        self.world_size = world_size
        self.cache_data = cache_data

        # if self.batch_size > 1:
        #     raise NotImplementedError("Batch size greater than 1 is not supported yet")

        # Set up device, needed for pipeline
        if isinstance(device, str):
            device = torch.device(device)
        # Need a index id if cuda
        if device.type == "cuda" and device.index is None:
            device = torch.device("cuda:0")
        self.device = device

        # check root directory exists
        if not self.data_dir.is_dir():
            raise IOError(f"Error, data directory {self.data_dir} does not exist")

        self.parse_dataset_files()
        self.load_statistics()

        self.pipe = self._create_pipeline()

    def parse_dataset_files(self) -> None:
        """Parses the data directory and builds a list of fixed-length sequences.

        Each sub-directory inside ``data_dir`` is assumed to correspond to one
        simulation run that contains an ordered series of mesh files.  For a
        chosen ``sequence_length`` this routine creates all sliding-window
        sequences and stores them in ``self.sequence_paths``.
        """
        # Determine file glob pattern
        match self.file_format:
            case "vtp":
                pattern = "*.vtp"
            case "vtu":
                pattern = "*.vtu"
            case "vtm":
                pattern = "*.vtm"
            case "cgns":
                pattern = "*.cgns"
            case _:
                raise NotImplementedError(
                    f"Data type {self.file_format} is not supported yet"
                )

        # Build the list of sequences.
        self.sequence_paths: List[List[str]] = []
        sim_dirs = [p for p in sorted(self.data_dir.iterdir()) if p.is_dir()]

        # Fallback: if no sub-directories are present but the current directory already
        # contains mesh files matching the pattern, treat *data_dir* itself as a single
        # simulation folder so that users can point the datapipe directly at one run.
        if not sim_dirs:
            raise IOError(
                f"No mesh files matching '{pattern}' found in {self.data_dir} and no sub-directories present."
            )

        for sim_dir in sim_dirs:
            files = sorted(str(fp) for fp in sim_dir.glob(pattern))
            if len(files) < self.sequence_length:
                self.logger.warning(
                    f"Skipping {sim_dir} – only {len(files)} files but sequence_length={self.sequence_length}"
                )
                continue
            for i in range(len(files) - self.sequence_length + 1):
                self.sequence_paths.append(files[i : i + self.sequence_length])

        self.logger.info(f"Total number of sequences: {len(self.sequence_paths)}")

    def load_statistics(
        self,
    ) -> None:  # TODO generalize and combine with climate/era5_hdf5 datapipes
        """Loads statistics from pre-computed numpy files

        The statistic files should be of name global_means.npy and global_std.npy with
        a shape of [1, C] located in the stat_dir.

        Raises
        ------
        IOError
            If mean or std numpy files are not found
        AssertionError
            If loaded numpy arrays are not of correct size
        """
        # If no stats dir we just skip loading the stats
        if self.stats_dir is None:
            self.mu = None
            self.std = None
            return
        # load normalisation values
        mean_stat_file = self.stats_dir / Path("global_means.npy")
        std_stat_file = self.stats_dir / Path("global_stds.npy")

        if not mean_stat_file.exists():
            raise IOError(f"Mean statistics file {mean_stat_file} not found")
        if not std_stat_file.exists():
            raise IOError(f"Std statistics file {std_stat_file} not found")

        # has shape [1, C]
        self.mu = np.load(str(mean_stat_file))[:, 0 : self.num_variables]
        # has shape [1, C]
        self.std = np.load(str(std_stat_file))[:, 0 : self.num_variables]

        if not self.mu.shape == self.std.shape == (1, self.num_variables):
            raise AssertionError("Error, normalisation arrays have wrong shape")

    def _create_pipeline(self) -> dali.Pipeline:
        """Create DALI pipeline

        Returns
        -------
        dali.Pipeline
            Mesh DALI pipeline
        """
        pipe = dali.Pipeline(
            batch_size=self.batch_size,
            num_threads=2,
            prefetch_queue_depth=2,
            py_num_workers=self.num_workers,
            device_id=self.device.index,
            py_start_method="spawn",
        )

        with pipe:
            source = TransientMeshDaliExternalSource(
                sequence_paths=self.sequence_paths,
                file_format=self.file_format,
                variables=self.variables,
                batch_size=self.batch_size,
                shuffle=self.shuffle,
                process_rank=self.process_rank,
                world_size=self.world_size,
                cache_data=self.cache_data,
            )
            # Update length of dataset
            self.length = len(source) // self.batch_size
            # Read current batch.
            vertices, attributes, edges = dali.fn.external_source(
                source,
                num_outputs=3,
                parallel=True,
                batch=False,
            )

            if self.device.type == "cuda":
                # Move tensors to GPU as external_source won't do that.
                vertices = vertices.gpu()
                attributes = attributes.gpu()
                edges = edges.gpu()

            # Normalize attributes if statistics are available.
            if self.stats_dir is not None:
                attributes = dali.fn.normalize(attributes, mean=self.mu, stddev=self.std)

            # Set outputs.
            pipe.set_outputs(vertices, attributes, edges)

        return pipe

    def __iter__(self):
        # Reset the pipeline before creating an iterator to enable epochs.
        self.pipe.reset()
        # Create DALI PyTorch iterator.
        return dali_pth.DALIGenericIterator([self.pipe], ["vertices", "x", "edges"])

    def __len__(self):
        return self.length


class TransientMeshDaliExternalSource:
    """DALI external source that yields fixed-length sequences of mesh data."""

    def __init__(
        self,
        sequence_paths: Iterable[Iterable[str]],
        file_format: str,
        variables: List[str],
        batch_size: int = 1,
        shuffle: bool = True,
        process_rank: int = 0,
        world_size: int = 1,
        cache_data: bool = False,
    ):
        self.sequence_paths = list(sequence_paths)
        self.file_format = file_format
        self.variables = variables
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.cache_data = cache_data

        self.last_epoch = None

        # Shard indices if running in parallel (e.g. DDP).
        all_indices = np.arange(len(self.sequence_paths))
        self.indices = np.array_split(all_indices, world_size)[process_rank]

        # Number of full batches (DALI does not support incomplete batches in parallel mode).
        self.num_batches = len(self.indices) // self.batch_size

        # Helpers for reading / parsing single mesh files.
        self.mesh_reader_fn = self.mesh_reader()
        self.parse_vtk_data_fn = self.parse_vtk_data()

        # Optional in-memory cache keyed by absolute file path.
        if self.cache_data:
            unique_files = {fp for seq in self.sequence_paths for fp in seq}
            self.data_cache = {fp: None for fp in unique_files}

    def __call__(self, sample_info: dali.types.SampleInfo) -> Tuple[Tensor, Tensor, Tensor]:
        if sample_info.iteration >= self.num_batches:
            raise StopIteration()

        # Epoch-wise shuffling.
        if self.shuffle and sample_info.epoch_idx != self.last_epoch:
            np.random.default_rng(seed=sample_info.epoch_idx).shuffle(self.indices)
            self.last_epoch = sample_info.epoch_idx

        idx = self.indices[sample_info.idx_in_epoch]
        seq_files = self.sequence_paths[idx]

        vertices_seq, attributes_seq, edges_seq = [], [], []
        for fp in seq_files:
            if self.cache_data:
                cached = self.data_cache.get(fp)
                if cached is None:
                    data = self.mesh_reader_fn(fp)
                    cached = self.parse_vtk_data_fn(data, self.variables)
                    self.data_cache[fp] = cached
                v, a, e = cached
            else:
                v, a, e = self.parse_vtk_data_fn(self.mesh_reader_fn(fp), self.variables)
            vertices_seq.append(v)
            attributes_seq.append(a)
            edges_seq.append(e)

        vertices = torch.stack(vertices_seq, dim=0)
        attributes = torch.stack(attributes_seq, dim=0)
        edges = torch.stack(edges_seq, dim=0)

        return vertices, attributes, edges

    def __len__(self):
        return len(self.indices)

    def mesh_reader(self):
        if self.file_format == "vtp":
            return read_vtp
        if self.file_format == "vtu":
            return read_vtu
        if self.file_format == "vtm":
            return read_vtm
        if self.file_format == "cgns":
            return read_cgns
        else:
            raise NotImplementedError(
                f"Data type {self.file_format} is not supported yet"
            )

    def parse_vtk_data(self):
        if self.file_format == "vtp":
            return parse_vtk_polydata
        elif self.file_format in ["vtu", "cgns", "vtm"]:
            return parse_vtk_unstructuredgrid
        else:
            raise NotImplementedError(
                f"Data type {self.file_format} is not supported yet"
            )