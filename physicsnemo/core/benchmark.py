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

import json
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Tuple

import torch

class BenchmarkMixin:
    """Defines the hooks needed by the benchmark and validation helpers."""

    @classmethod
    def make_inputs(cls) -> Iterable[Tuple[str, dict[str, Any], Tuple[Any, ...]]]:
        """Yield (label, init_kwargs, run_args) tuples for benchmarking."""

        raise NotImplementedError(
            f"{cls.__name__}.make_inputs must be implemented by subclasses"
        )

    @classmethod
    def reference_impl(cls, *args, **kwargs):
        """Compute a reference output for correctness checks."""

        raise NotImplementedError(
            f"{cls.__name__}.reference_impl must be implemented by subclasses"
        )

    @classmethod
    def check(cls, *args, **kwargs) -> None:
        """Validate outputs against the reference implementation."""

        raise NotImplementedError(
            f"{cls.__name__}.check must be implemented by subclasses"
        )

    @classmethod
    def benchmark(
        cls,
        *,
        repeats: int = 10,
        warmup: int = 1,
    ) -> None:
        """Collect benchmark data for downstream consumers.

        Parameters
        ----------
        repeats
            Number of timed iterations recorded per benchmark case.
        warmup
            Runs discarded before timings are collected. Warmups help amortize
            one-time setup costs.
        """

        raise NotImplementedError(
            f"Benchmarking not supported yet"
        )