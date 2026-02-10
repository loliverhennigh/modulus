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

"""ASV benchmarks for PhysicsNeMo functionals."""

from __future__ import annotations

import os
from typing import Any, Iterable, Tuple

import torch

from benchmarks.physicsnemo.nn.functional.registry import FUNCTIONAL_SPECS

_MIN_CASE_COUNT = 3


def _resolve_device() -> torch.device:
    device_name = os.getenv("PHYSICSNEMO_ASV_DEVICE")
    if device_name:
        return torch.device(device_name)
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


def _filter_specs(specs: Iterable[type]) -> list[type]:
    spec_filter = os.getenv("PHYSICSNEMO_ASV_FUNCTIONALS")
    if not spec_filter:
        return list(specs)

    requested = {name.strip().lower() for name in spec_filter.split(",") if name.strip()}
    if not requested:
        return list(specs)

    selected = [spec for spec in specs if spec.__name__.lower() in requested]
    if not selected:
        available = ", ".join(sorted(spec.__name__ for spec in specs))
        raise ValueError(
            "PHYSICSNEMO_ASV_FUNCTIONALS did not match any FunctionSpec. "
            f"Requested: {spec_filter!r}. Available: {available}"
        )
    return selected


def _normalize_case(
    case: Tuple[str, Tuple[Any, ...], dict[str, Any]]
    | Tuple[str, Tuple[Any, ...]]
    | Tuple[str, Tuple[Any, ...], None]
) -> tuple[str, tuple[Any, ...], dict[str, Any]]:
    if len(case) == 2:
        label, args = case
        kwargs = {}
    elif len(case) == 3:
        label, args, kwargs = case
        kwargs = {} if kwargs is None else kwargs
    else:
        raise ValueError(
            "make_inputs must yield (label, args) or (label, args, kwargs)"
        )
    if not isinstance(label, str):
        raise TypeError("make_inputs labels must be strings")
    if not isinstance(args, tuple):
        raise TypeError("make_inputs args must be a tuple")
    if not isinstance(kwargs, dict):
        raise TypeError("make_inputs kwargs must be a dict")
    return label, args, kwargs


_DEVICE = _resolve_device()
_PARAMS: list[tuple[str, str, int]] = []
_SPEC_CASES: dict[str, list[tuple[str, tuple[Any, ...], dict[str, Any]]]] = {}
_SELECTED_SPECS = _filter_specs(FUNCTIONAL_SPECS)
_SPEC_LOOKUP = {spec.__name__: spec for spec in _SELECTED_SPECS}

for spec in _SELECTED_SPECS:
    implementations = spec.available_implementations()
    if not implementations:
        continue
    try:
        cases = list(spec.make_inputs(device=_DEVICE))
    except NotImplementedError as exc:
        raise RuntimeError(
            f"{spec.__name__}.make_inputs must be implemented before "
            "adding the spec to benchmarks."
        ) from exc
    if len(cases) < _MIN_CASE_COUNT:
        raise ValueError(
            f"{spec.__name__}.make_inputs must yield at least {_MIN_CASE_COUNT} cases."
        )
    normalized_cases = [_normalize_case(case) for case in cases]
    _SPEC_CASES[spec.__name__] = normalized_cases
    for impl in implementations:
        for case_index in range(len(normalized_cases)):
            _PARAMS.append((spec.__name__, impl, case_index))


class FunctionalBenchmarks:
    """Benchmark registered FunctionSpec implementations with ASV."""

    params = [_PARAMS]
    param_names = ["spec_impl_case"]
    timeout = 120

    def setup(self, spec_impl_case: tuple[str, str, int]) -> None:
        spec_name, implementation, case_index = spec_impl_case
        spec = _SPEC_LOOKUP[spec_name]

        cases = _SPEC_CASES[spec_name]
        label, args, kwargs = cases[case_index]

        self.spec = spec
        self.implementation = implementation
        self.args = args
        self.kwargs = kwargs
        self.case_label = label

        if _DEVICE.type == "cuda":
            torch.cuda.synchronize()

    def time_functional(self, spec_impl_case: tuple[str, str, int]) -> None:
        self.spec.dispatch(*self.args, **self.kwargs, implementation=self.implementation)
        if _DEVICE.type == "cuda":
            torch.cuda.synchronize()
