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

import warnings

from .grid_to_point_interpolation import grid_to_point_interpolation


def interpolation(*args, **kwargs):
    """Deprecated alias for ``grid_to_point_interpolation``."""
    warnings.warn(
        "`interpolation` is deprecated and will be removed in a future release. "
        "Use `grid_to_point_interpolation` instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    # Preserve historical default behavior for the deprecated alias while still
    # allowing explicit backend selection overrides.
    kwargs.setdefault("implementation", "torch")
    return grid_to_point_interpolation(*args, **kwargs)


__all__ = ["interpolation"]
