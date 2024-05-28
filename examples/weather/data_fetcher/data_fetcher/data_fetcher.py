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
import numpy as np
import pandas as pd

class DataFetcher:

    def __init__(self, 
        dataset_filename,
        fetch_dt=6,
        store_dt=1,
        start_date="1900-01-01",
        end_date="2025-01-01"
    ):
        self.dataset_filename = dataset_filename
        self.fetch_dt = fetch_dt
        self.store_dt = store_dt
        self.start_date = start_date
        self.end_date = end_date

        # Get timestamps
        reference_date = "1900-01-01 00:00:00"
        start_timestamp = pd.to_datetime(self.start_date)
        end_timestamp = pd.to_datetime(self.end_date)
        reference_timestamp = pd.to_datetime(reference_date)
        start_index = int((start_timestamp - reference_timestamp).total_seconds() // 3600)
        end_index = int((end_timestamp - reference_timestamp).total_seconds() // 3600)
        self.fetch_timestamps = np.arange(start_index, end_index, self.fetch_dt)
        self.store_timestamps = np.arange(start_index, end_index, self.store_dt)

        # Open the dataset
        self.ds = zarr.open(self.dataset_filename, mode="w")
        print(f"Dataset opened: {self.ds}")
        print(f"file: {self.dataset_filename}")

    def initialize_store(self):
        pass

    def run_pipeline(self):
        pass

    def __call__(self):

        # Initialize the store
        self.initialize_store()

        # Fetch the data
        self.run_pipeline()
