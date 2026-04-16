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

from __future__ import annotations

try:
    from .op import mesh_to_voxel_fraction_warp
except Exception as exc:  # pragma: no cover - optional dependency path
    _WARP_IMPORT_ERROR = exc

    def mesh_to_voxel_fraction_warp(*args, **kwargs):
        raise ImportError(
            "mesh_to_voxel_fraction requires the optional Warp backend "
            "(warp-lang>=0.6.0)"
        ) from _WARP_IMPORT_ERROR


__all__ = ["mesh_to_voxel_fraction_warp"]
