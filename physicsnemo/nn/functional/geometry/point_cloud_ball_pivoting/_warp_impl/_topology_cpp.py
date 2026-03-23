# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path

from torch.utils.cpp_extension import load


@lru_cache(maxsize=1)
def load_topology_cpp_module():
    """Build and load the pybind11 topology extension used by BPA.

    The extension is compiled lazily and cached by torch in the local extension
    cache directory. This keeps the Python package layout simple while moving
    the topology loop out of Python.
    """

    source_path = Path(__file__).with_name("_topology_pybind.cpp").resolve()
    file_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()[:10]
    module_name = f"physicsnemo_ball_pivoting_topology_{file_hash}"

    return load(
        name=module_name,
        sources=[str(source_path)],
        extra_cflags=["-O3", "-std=c++17"],
        with_cuda=False,
        verbose=False,
    )


__all__ = ["load_topology_cpp_module"]
