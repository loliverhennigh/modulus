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

"""Shared helpers for functional ASV benchmark scripts."""

from __future__ import annotations

from typing import Any

import torch

from physicsnemo.core.function_spec import FunctionSpec

PHASE_ORDER = ("forward", "backward")


def supports_backward_inputs(spec: type) -> bool:
    """Return True when a spec overrides backward input generation."""

    return spec.make_inputs_backward.__func__ is not FunctionSpec.make_inputs_backward


def _metadata_case_labels(spec: type) -> list[str]:
    """Return benchmark case labels from optional spec metadata."""

    benchmark_cases = getattr(spec, "_BENCHMARK_CASES", None)
    if isinstance(benchmark_cases, (list, tuple)):
        labels = [
            case[0]
            for case in benchmark_cases
            if isinstance(case, tuple) and case and isinstance(case[0], str)
        ]
        if labels:
            return labels

    benchmark_cases_fn = getattr(spec, "_benchmark_cases", None)
    if callable(benchmark_cases_fn):
        labels = [
            case[0]
            for case in benchmark_cases_fn()
            if isinstance(case, tuple) and case and isinstance(case[0], str)
        ]
        if labels:
            return labels

    return []


def case_labels(spec: type, phase: str, device: torch.device | str) -> list[str]:
    """Resolve labeled benchmark cases for one phase."""

    if phase not in PHASE_ORDER:
        raise ValueError(f"Unsupported benchmark phase: {phase}")
    if phase == "backward" and not supports_backward_inputs(spec):
        return []

    labels = _metadata_case_labels(spec)
    if labels:
        return labels

    if phase == "forward":
        return [label for label, _, _ in spec.make_inputs_forward(device=device)]
    return [label for label, _, _ in spec.make_inputs_backward(device=device)]


def case_by_index(
    spec: type,
    phase: str,
    case_index: int,
    device: torch.device | str,
) -> tuple[str, tuple[Any, ...], dict[str, Any]]:
    """Materialize one case from the phase-specific input generator."""

    if phase == "forward":
        case_iter = spec.make_inputs_forward(device=device)
    elif phase == "backward":
        case_iter = spec.make_inputs_backward(device=device)
    else:
        raise ValueError(f"Unsupported benchmark phase: {phase}")

    for index, case in enumerate(case_iter):
        if index == case_index:
            return case
    raise IndexError(
        f"Case index {case_index} out of range for {spec.__name__} phase={phase}"
    )


__all__ = ["PHASE_ORDER", "supports_backward_inputs", "case_labels", "case_by_index"]

