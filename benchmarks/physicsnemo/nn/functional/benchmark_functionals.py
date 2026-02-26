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

"""ASV benchmarks for PhysicsNeMo functionals."""
# TODO: This code will likely evolve with CI/CD integration.

from __future__ import annotations

import os
from typing import Any, Iterable

import torch

from benchmarks.physicsnemo.nn.functional._spec_utils import (
    PHASE_ORDER,
    case_by_index,
    case_labels,
)
from benchmarks.physicsnemo.nn.functional.registry import FUNCTIONAL_SPECS


def _resolve_device() -> torch.device:
    """Resolve the device to benchmark on."""

    # Allow the benchmark device to be overridden from the environment.
    device_name = os.getenv("PHYSICSNEMO_ASV_DEVICE")
    if device_name:
        return torch.device(device_name)

    # Prefer CUDA when available; otherwise default to CPU.
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


def _resolve_phases() -> tuple[str, ...]:
    """Resolve benchmark phases from environment configuration."""

    # Default to forward-only to keep benchmark runtime manageable.
    phase_filter = os.getenv("PHYSICSNEMO_ASV_PHASES", "forward")
    requested = {
        name.strip().lower() for name in phase_filter.split(",") if name.strip()
    }
    if not requested:
        return ("forward",)

    # Keep a stable phase order for reproducible ASV parameter vectors.
    selected = tuple(phase for phase in PHASE_ORDER if phase in requested)
    if not selected:
        raise ValueError(
            "PHYSICSNEMO_ASV_PHASES must contain one or both of: "
            f"{', '.join(PHASE_ORDER)}"
        )
    return selected


def _filter_specs(specs: Iterable[type]) -> list[type]:
    """Filter the specs to the requested subset, this is mostly used for debugging locally."""

    # Allow selecting a subset of functionals for quick benchmark iteration.
    spec_filter = os.getenv("PHYSICSNEMO_ASV_FUNCTIONALS")
    if not spec_filter:
        return list(specs)

    # Parse comma-separated spec names into a normalized lookup set.
    requested = {
        name.strip().lower() for name in spec_filter.split(",") if name.strip()
    }
    if not requested:
        return list(specs)

    # Keep only specs explicitly requested by name.
    selected = [spec for spec in specs if spec.__name__.lower() in requested]
    if not selected:
        available = ", ".join(sorted(spec.__name__ for spec in specs))
        raise ValueError(
            "PHYSICSNEMO_ASV_FUNCTIONALS did not match any FunctionSpec. "
            f"Requested: {spec_filter!r}. Available: {available}"
        )
    return selected


def _iter_tensors(value: Any):
    """Iterate tensor leaves from nested Python containers."""

    # Yield tensor leaves directly.
    if torch.is_tensor(value):
        yield value
        return

    # Recurse into tuples/lists.
    if isinstance(value, (tuple, list)):
        for item in value:
            yield from _iter_tensors(item)
        return

    # Recurse into dict values.
    if isinstance(value, dict):
        for item in value.values():
            yield from _iter_tensors(item)


def _clear_input_gradients(args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
    """Clear accumulated gradients for reusable benchmark inputs."""

    # ASV reuses inputs across timing calls, so clear stale gradients each time.
    for tensor in _iter_tensors((args, kwargs)):
        if tensor.grad is not None:
            tensor.grad = None


def _loss_from_output(output: Any) -> torch.Tensor:
    """Reduce the functional output to a scalar loss for backward timing."""

    # Collect differentiable output tensors from nested return structures.
    differentiable_outputs = [
        tensor
        for tensor in _iter_tensors(output)
        if tensor.requires_grad or tensor.grad_fn is not None
    ]
    if not differentiable_outputs:
        raise ValueError(
            "Backward benchmark output must contain at least one differentiable tensor."
        )

    # Build a stable scalar objective from output norms.
    # Use |z|^2 for complex tensors to avoid lossy casts.
    def _norm_term(tensor: torch.Tensor) -> torch.Tensor:
        if torch.is_complex(tensor):
            return tensor.abs().square().mean()
        return tensor.float().square().mean()

    loss = _norm_term(differentiable_outputs[0])
    for tensor in differentiable_outputs[1:]:
        loss = loss + _norm_term(tensor)
    return loss


# Resolve benchmark configuration and precompute all ASV parameter tuples.
_DEVICE = _resolve_device()
_PHASES = _resolve_phases()
_SELECTED_SPECS = _filter_specs(FUNCTIONAL_SPECS)
_PARAMS: list[tuple[str, str, str, int]] = []
_WORK_ITEMS: dict[tuple[str, str, str, int], tuple[type, str]] = {}

# Build ASV parameter tuples: (phase, spec_name, implementation_name, case_index).
for spec in _SELECTED_SPECS:
    # Skip specs that currently have no dispatchable implementations.
    implementations = spec.available_implementations()
    if not implementations:
        continue

    # Build phase-specific parameter tuples and cache label metadata.
    for phase in _PHASES:
        labels = case_labels(spec=spec, phase=phase, device=_DEVICE)
        if not labels:
            continue

        for implementation_name in implementations:
            for case_index, label in enumerate(labels):
                key = (phase, spec.__name__, implementation_name, case_index)
                _PARAMS.append(key)
                _WORK_ITEMS[key] = (spec, label)


class FunctionalBenchmarks:
    """Benchmark registered FunctionSpec implementations with ASV."""

    # ASV expects params to be a list of parameter axes.
    params = [_PARAMS]
    param_names = ["phase_spec_impl_case"]
    timeout = 120

    def setup(self, phase_spec_impl_case: tuple[str, str, str, int]) -> None:
        # Resolve the precomputed work item for this benchmark key.
        spec, _ = _WORK_ITEMS[phase_spec_impl_case]

        # Cache resolved objects on self to minimize per-iteration overhead.
        self.phase = phase_spec_impl_case[0]
        self.case_index = phase_spec_impl_case[3]
        self.spec = spec
        self.implementation = phase_spec_impl_case[2]
        _, self.args, self.kwargs = case_by_index(
            spec=self.spec,
            phase=self.phase,
            case_index=self.case_index,
            device=_DEVICE,
        )

        # Synchronize before timing so previous CUDA work is excluded.
        if _DEVICE.type == "cuda":
            torch.cuda.synchronize()

    def time_functional(self, phase_spec_impl_case: tuple[str, str, str, int]) -> None:
        # Benchmark the selected phase for the selected implementation/case.
        if self.phase == "forward":
            self.spec.dispatch(
                *self.args, **self.kwargs, implementation=self.implementation
            )
        else:
            _clear_input_gradients(args=self.args, kwargs=self.kwargs)
            output = self.spec.dispatch(
                *self.args, **self.kwargs, implementation=self.implementation
            )
            _loss_from_output(output).backward()

        # Synchronize to ensure the measured time includes kernel execution.
        if _DEVICE.type == "cuda":
            torch.cuda.synchronize()
