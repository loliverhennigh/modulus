# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

try:
    from .op import pml_phi_h_update_warp
except ImportError as _warp_import_error:

    def pml_phi_h_update_warp(*args, _import_error=_warp_import_error, **kwargs):
        raise ImportError(
            "pml_phi_h_update warp implementation requires 'warp>=0.6.0'"
        ) from _import_error


__all__ = ["pml_phi_h_update_warp"]
