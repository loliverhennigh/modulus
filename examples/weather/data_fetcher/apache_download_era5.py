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

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import zarr
import datetime
import dask
import fsspec
import hydra
import numpy as np
import xarray as xr
from dask.diagnostics import ProgressBar
from omegaconf import DictConfig, OmegaConf
import pandas as pd

class ReadARCOChunks(beam.DoFn):
    def __init__(self, ):

        # Get ARCO ERA5 dataset
        arco_era5_filename = (
            "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3"
        )
        gs_fs = fsspec.filesystem("gs")
        arco_era5 = zarr.open(gs_fs.get_mapper(arco_era5_filename), mode="r")
        print(arco_era5["time"][0:10])
        date = pd.to_datetime(arco_era5["time"][0:10], unit="h", origin="1950-01-01")
        print(date)

    def process(self, index):
        # Get time stamp
        fs = fsspec.filesystem('s3')
        zarr_store = fsspec.get_mapper(element)
        ds = xr.open_zarr(zarr_store, consolidated=True)
        for chunk in ds.chunks.items():
            yield chunk

def run_pipeline():
    # Define the pipeline options
    options = PipelineOptions()

    # Create the pipeline
    with beam.Pipeline(options=options) as p:
        # Read the ARCO chunks
        arco_chunks = (
            p
            | "Read ARCO chunks" >> beam.Create(["gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3"])
            | "Process ARCO chunks" >> beam.ParDo(ReadARCOChunks())
        )

        # Write the ARCO chunks
        arco_chunks | "Write ARCO chunks" >> beam.io.WriteToText("arco_chunks.txt")


if __name__ == "__main__":
    ds = ReadARCOChunks()

    #run_pipeline()
