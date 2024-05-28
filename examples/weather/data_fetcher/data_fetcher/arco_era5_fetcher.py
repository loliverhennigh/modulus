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
from tqdm import tqdm
import datetime
import dask
import fsspec
import hydra
import numpy as np
import xarray as xr
from dask.diagnostics import ProgressBar
from omegaconf import DictConfig, OmegaConf
import pandas as pd

from .data_fetcher import DataFetcher

class ARCOERA5Fetcher(DataFetcher):
    """
    Fetches data from the ARCO ERA5 dataset and stores it in a Zarr store.
    """

    def __init__(self, 
        variables,
        dataset_filename,
        fetch_dt=1,
        store_dt=1,
        start_date="1900-01-01",
        end_date="2025-01-01"
    ):
        # Store the variables
        self.variables = variables

        # Open ARCO ERA5 dataset
        arco_era5_filename = (
            "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3"
        )
        gs_fs = fsspec.filesystem("gs")
        self.arco_era5 = zarr.open(gs_fs.get_mapper(arco_era5_filename), mode="r")

        # Call the parent constructor
        super().__init__(dataset_filename, fetch_dt, store_dt, start_date, end_date)

    def initialize_store(self):
        """
        Initializes the Zarr store with the coordinates and variables.
        """

        # Make coordinates (time, lattitude, longitude)
        # TODO: These might move to the constructor as more data fetchers are added
        if "time" not in self.ds:
            self.ds.create_dataset("time", data=self.store_timestamps, compressor=self.arco_era5["time"].compressor)
            for key, value in self.arco_era5["time"].attrs.items():
                self.ds["time"].attrs[key] = value
        if "latitude" not in self.ds:
            self.ds.create_dataset("latitude", data=self.arco_era5["latitude"], compressor=self.arco_era5["latitude"].compressor)
            for key, value in self.arco_era5["latitude"].attrs.items():
                self.ds["latitude"].attrs[key] = value
        if "longitude" not in self.ds:
            self.ds.create_dataset("longitude", data=self.arco_era5["longitude"], compressor=self.arco_era5["longitude"].compressor)
            for key, value in self.arco_era5["longitude"].attrs.items():
                self.ds["longitude"].attrs[key] = value

        # Make datasets for each variable
        print("Initializing Zarr store")
        for variable in tqdm(self.variables):
    
            # Make the variable name
            var_names = ARCOERA5Fetcher._variable_to_var_names(variable)
    
            # Create if variable is in ARCO ERA5 dataset
            if isinstance(variable, str):
                if variable not in self.arco_era5:
                    raise ValueError(f"Variable {variable} not found in ARCO ERA5 dataset")
            if isinstance(variable, tuple):
                if variable[0] not in self.arco_era5:
                    raise ValueError(f"Variable {variable[0]} not found in ARCO ERA5 dataset")
    
            # Create the dataset for the variable
            for var_name in var_names:
    
                # Check if variable already exists in Zarr store
                if var_name not in self.ds:

                    # Check if variable is already in dataset
                    if var_name in self.ds.variables:
                        print(f"Variable {var_name} already in dataset")
                        continue
    
                    # Single level variable
                    if isinstance(variable, str):
                        # Create the dataset for the variable
                        self.ds.create_dataset(var_name, shape=(len(self.store_timestamps), 721, 1440), dtype="f4", chunks=(1, 721, 1440), compressor=self.arco_era5[variable].compressor)
                        for key, value in self.arco_era5[variable].attrs.items():
                            self.ds[var_name].attrs[key] = value
    
                    # Multi level variable
                    if isinstance(variable, tuple):
                        for level in variable[1]:
                            # Create the dataset for the variable
                            self.ds.create_dataset(var_name, shape=(len(self.store_timestamps), 721, 1440), dtype="f4", chunks=(1, 721, 1440), compressor=self.arco_era5[variable[0]].compressor)
                            for key, value in self.arco_era5[variable[0]].attrs.items():
                                if key == "_ARRAY_DIMENSIONS":
                                    self.ds[var_name].attrs[key] = ["time", "latitude", "longitude"]
                                else:
                                    self.ds[var_name].attrs[key] = value
    
        # Consolidate the Zarr store
        zarr.consolidate_metadata(self.dataset_filename)

    def run_pipeline(self):
        """
        Runs the pipeline to fetch the data.
        """

        # Create list of timestamps and variables
        print("Creating list of timestamps and variables")
        timestamp_variables = []
        for timestamp in tqdm(self.fetch_timestamps):
            for variable in self.variables:
                timestamp_variables.append((timestamp, variable))
    
        # Define the pipeline options with multiple processors
        options = PipelineOptions(
            runner="DirectRunner",
            options=[
                "--direct_num_workers=16",
                "--direct_running_mode=multi_processing",
            ],
        )

        # Run the pipeline
        with beam.Pipeline(options=options) as p:
            arco_chunks = (
                p
                | "Create timestamp variables" >> beam.Create(timestamp_variables)
                | "Check if downloaded" >> beam.ParDo(CheckIfDownloaded(self.ds))
                | "Fetch ARCO chunks" >> beam.ParDo(FetchARCOChunk(self.arco_era5, self.ds))
            )

    @staticmethod
    def _variable_to_var_names(variable):
        # Helper functions for converting variables to variable names
        # Returns a list of strings for the variable names
    
        # list of variable names 
        var_names = []
    
        # If the variable is a string then its a single level variable
        if isinstance(variable, str):
            var_names.append(variable + "::")
    
        # If the variable is a tuple then its a multi level variable
        if isinstance(variable, tuple):
            for level in variable[1]:
                var_names.append(f"{variable[0]}::{level}")
    
        return var_names


# First step in the pipeline to check if the variable is downloaded
# TODO: Maybe move this inside the ARCOERA5Fetcher class
class CheckIfDownloaded(beam.DoFn):

    def __init__(self, store_ds):
        self.store_ds = store_ds

    def process(self, timestamp_variable):

        # Get the timestamp and variable
        timestamp, variable = timestamp_variable

        # Get the variable names
        var_names = ARCOERA5Fetcher._variable_to_var_names(variable)

        # Check if the variable is downloaded
        need_to_download = False
        for var_name in var_names:

            # Check if the variable is in the ARCO ERA5 dataset (Should always be true)
            if var_name in self.store_ds:

                # Check if timestamp is in the Zarr store is missing
                check_value = self.store_ds[var_name][timestamp]
                if np.all(check_value == self.store_ds[var_name].fill_value):
                    need_to_download = True
            else:
                raise ValueError(f"Variable {var_name} not found in Zarr store")

        # If the variable is not downloaded then yield the timestamp and variable
        if need_to_download:
            yield timestamp_variable

# Second step in the pipeline to read the ARCO chunks and store them in the Zarr store
# TODO: Maybe move this inside the ARCOERA5Fetcher class
class FetchARCOChunk(beam.DoFn):
    def __init__(self, arco_era5, store_ds):

        # Store the ARCO ERA5 dataset and Zarr store
        self.arco_era5 = arco_era5
        self.store_ds = store_ds

    def process(self, timestamp_variable):

        # Get the timestamp and variable
        timestamp, variable = timestamp_variable

        # Get the variable names
        var_names = ARCOERA5Fetcher._variable_to_var_names(variable)

        # Read the ARCO ERA5 data and store it in the Zarr store
        # Single level variable
        if isinstance(variable, str):
            arco_data = self.arco_era5[variable][timestamp] # (721, 1440)
            self.store_ds[var_names[0]][timestamp] = arco_data

        # Multi level variable
        if isinstance(variable, tuple):
            arco_data = self.arco_era5[variable[0]][timestamp] # (34, 721, 1440)
            levels = self.arco_era5["level"][:] # (34,)
            for var_name, l in zip(var_names, variable[1]):
                level_index = np.where(levels == l)[0][0]
                self.store_ds[var_name][timestamp] = arco_data[level_index]
