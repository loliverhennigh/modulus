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

import os
import fsspec
import hydra
from omegaconf import DictConfig, OmegaConf

from data_fetcher.arco_era5_fetcher import ARCOERA5Fetcher

# Add eval to OmegaConf TODO: Remove when OmegaConf is updated
OmegaConf.register_new_resolver("eval", eval)

@hydra.main(version_base="1.2", config_path="conf", config_name="config")
def main(cfg: DictConfig) -> None:
    # Resolve config so that all values are concrete
    OmegaConf.resolve(cfg)

    # Creating filesystem from config and dataset filename mapping
    if cfg.filesystem.type == "file":
        fs = fsspec.filesystem("file")
        dataset_filename = cfg.dataset_filename
    elif cfg.filesystem.type == "s3":
        fs = fsspec.filesystem(
            "s3",
            key=cfg.filesystem.key,
            secret=os.environ["AWS_SECRET_ACCESS_KEY"],
            client_kwargs={
                "endpoint_url": cfg.filesystem.endpoint_url,
                "region_name": cfg.filesystem.region_name,
            },
        )
        dataset_filename = fs.get_mapper("s3://" + cfg.filesystem.bucket + "/" + cfg.dataset_filename)

    # Make fetcher objects
    arco_era5_fetcher = ARCOERA5Fetcher(
        cfg.arco_era5_dataset.variables,
        dataset_filename,
        cfg.arco_era5_dataset.fetch_dt,
        cfg.store_dt,
        cfg.start_date,
        cfg.end_date
    )

    # Fetch data
    arco_era5_fetcher()


if __name__ == "__main__":
    main()
