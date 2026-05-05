# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

try:
    from .op import gather_fields_to_particles_warp
except ImportError as _warp_import_error:

    def gather_fields_to_particles_warp(*args, _import_error=_warp_import_error, **kwargs):
        raise ImportError(
            "gather_fields_to_particles warp implementation requires 'warp>=0.6.0'"
        ) from _import_error


__all__ = [
    "gather_fields_to_particles_warp",
]

