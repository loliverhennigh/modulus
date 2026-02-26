#!/usr/bin/env python3
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

"""Generate bar plots for functional benchmarks from ASV results."""
# TODO: This code is not meant to be a long term solution. As things progress we will
# update this script for better automated plot generation.

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any

from benchmarks.physicsnemo.nn.functional._spec_utils import PHASE_ORDER, case_labels
from benchmarks.physicsnemo.nn.functional.registry import FUNCTIONAL_SPECS

# Map each FunctionSpec to its docs output directory.
_SPEC_OUTPUT_SLUG = {
    "DropPath": "regularization_parameterization/drop_path",
    "ElectricFieldUpdate": "electromagnetics/electric_field_update",
    "KNN": "neighbors/knn",
    "RFFT": "fourier_spectral/rfft",
    "RFFT2": "fourier_spectral/rfft2",
    "MeshPoissonDiskSample": "geometry/mesh_poisson_disk_sample",
    "MeshToVoxelFraction": "geometry/mesh_to_voxel_fraction",
    "RadiusSearch": "neighbors/radius_search",
    "SignedDistanceField": "geometry/sdf",
    "IRFFT": "fourier_spectral/irfft",
    "IRFFT2": "fourier_spectral/irfft2",
    "GridToPointInterpolation": "interpolation/grid_to_point_interpolation",
    "PointToGridInterpolation": "interpolation/point_to_grid_interpolation",
    "WeightFact": "regularization_parameterization/weight_fact",
    "ViewAsComplex": "fourier_spectral/view_as_complex",
    "Real": "fourier_spectral/real",
    "Imag": "fourier_spectral/imag",
}

# Keep implementation order and colors stable across plots.
_IMPL_ORDER = ("warp", "cuml", "scipy", "torch")
_IMPL_COLORS = {
    "warp": "#76B900",
    "cuml": "#2E2E2E",
    "scipy": "#5A5A5A",
    "torch": "#111111",
    "unknown": "#8A8A8A",
}

# Match the ASV benchmark function used in benchmark_functionals.py.
_BENCHMARK_SUFFIX = "FunctionalBenchmarks.time_functional"


def _build_case_labels() -> dict[str, dict[str, list[str]]]:
    # Build phase->spec->case labels directly from FunctionSpec input generators.
    labels: dict[str, dict[str, list[str]]] = {phase: {} for phase in PHASE_ORDER}
    for spec in FUNCTIONAL_SPECS:
        if len(spec.available_implementations()) < 2:
            continue

        forward_labels = case_labels(spec=spec, phase="forward", device="cpu")
        backward_labels = case_labels(spec=spec, phase="backward", device="cpu")

        if forward_labels:
            labels["forward"][spec.__name__] = forward_labels
        if backward_labels:
            labels["backward"][spec.__name__] = backward_labels
    return labels


def _build_spec_implementations() -> dict[str, list[str]]:
    # Build implementation lists for each plottable spec.
    implementations: dict[str, list[str]] = {}
    for spec in FUNCTIONAL_SPECS:
        impls = spec.available_implementations()
        if len(impls) < 2:
            continue
        implementations[spec.__name__] = list(impls)
    return implementations


def _build_params(
    case_labels: dict[str, dict[str, list[str]]],
    spec_implementations: dict[str, list[str]],
) -> list[tuple[str, str, str, int]]:
    # Recreate ASV parameter ordering for fallback labels.
    params: list[tuple[str, str, str, int]] = []
    for phase in PHASE_ORDER:
        for spec_name, labels in case_labels[phase].items():
            for impl_name in spec_implementations.get(spec_name, []):
                params.extend(
                    (phase, spec_name, impl_name, case_index)
                    for case_index in range(len(labels))
                )
    return params


# Materialize spec metadata once.
_SPEC_CASE_LABELS = _build_case_labels()
_SPEC_IMPLEMENTATIONS = _build_spec_implementations()
_PARAMS = _build_params(_SPEC_CASE_LABELS, _SPEC_IMPLEMENTATIONS)


def _walk_dicts(value: Any):
    # Walk nested dict/list containers and yield dict nodes.
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from _walk_dicts(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _walk_dicts(nested)


def _latest_result_file(results_dir: Path) -> Path:
    # Pick the newest ASV result JSON, excluding metadata files.
    candidates = [
        path
        for path in results_dir.rglob("*.json")
        if path.name not in {"benchmarks.json", "machine.json"}
    ]
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _benchmark_entry(data: dict[str, Any]) -> Any:
    # Find the benchmark entry for the functional benchmark suite.
    for mapping in _walk_dicts(data):
        for key, value in mapping.items():
            if isinstance(key, str) and _BENCHMARK_SUFFIX in key:
                return value
    raise KeyError(f"Unable to find benchmark entry for {_BENCHMARK_SUFFIX}")


def _entry_vectors(entry: Any) -> tuple[list[float | None], list[str]]:
    # Normalize ASV payload into (values, labels).
    if isinstance(entry, dict):
        entry = entry.get("result", entry.get("results"))

    values = entry[0]
    labels = (
        entry[1] if len(entry) > 1 else [str(param) for param in _PARAMS[: len(values)]]
    )
    if labels and isinstance(labels[0], list):
        labels = labels[0]
    return values, labels


def _parse_benchmark_label(label: str) -> tuple[str, str, str, int]:
    # Parse ASV labels from either new (phase, spec, impl, case) or legacy format.
    parsed = ast.literal_eval(label)
    if isinstance(parsed, tuple) and len(parsed) == 4:
        phase, spec_name, impl_name, case_index = parsed
        return str(phase), str(spec_name), str(impl_name), int(case_index)
    if isinstance(parsed, tuple) and len(parsed) == 3:
        spec_name, impl_name, case_index = parsed
        return "forward", str(spec_name), str(impl_name), int(case_index)
    raise ValueError(f"Unsupported benchmark label format: {label}")


def _plot_benchmarks(
    values: list[float | None], labels: list[str], output_root: Path
) -> None:
    # Import plotting dependency only for plotting.
    import matplotlib.pyplot as plt

    # Build phase->spec->case->implementation->value map from ASV vectors.
    data: dict[str, dict[str, dict[str, dict[str, float]]]] = {
        phase: {} for phase in PHASE_ORDER
    }
    for label, value in zip(labels, values):
        if value is None:
            continue

        phase, spec_name, impl_name, case_index = _parse_benchmark_label(label)
        if phase not in _SPEC_CASE_LABELS:
            continue
        if spec_name not in _SPEC_CASE_LABELS[phase]:
            continue
        if case_index >= len(_SPEC_CASE_LABELS[phase][spec_name]):
            continue

        case_label = _SPEC_CASE_LABELS[phase][spec_name][case_index]
        data[phase].setdefault(spec_name, {}).setdefault(case_label, {})[impl_name] = (
            value
        )

    # Render one grouped bar chart per spec and benchmark phase.
    for phase in PHASE_ORDER:
        for spec_name, case_map in data[phase].items():
            output_dir = output_root / _SPEC_OUTPUT_SLUG.get(
                spec_name, spec_name.lower()
            )
            output_dir.mkdir(parents=True, exist_ok=True)

            # Build case order and implementation order for this spec.
            case_labels = [
                label
                for label in _SPEC_CASE_LABELS[phase][spec_name]
                if label in case_map
            ]
            impl_names = sorted(
                {impl for impl_map in case_map.values() for impl in impl_map},
                key=lambda name: (
                    _IMPL_ORDER.index(name) if name in _IMPL_ORDER else 99
                ),
            )
            if len(impl_names) < 2:
                continue

            # Create and style the figure.
            fig, ax = plt.subplots(figsize=(8, 4))
            fig.patch.set_facecolor("white")
            ax.set_facecolor("white")

            # Draw grouped bars for each implementation.
            bar_width = 0.8 / len(impl_names)
            x_positions = list(range(len(case_labels)))
            for idx, impl_name in enumerate(impl_names):
                offsets = [x + idx * bar_width for x in x_positions]
                y_values = [
                    case_map[label].get(impl_name, float("nan"))
                    for label in case_labels
                ]
                ax.bar(
                    offsets,
                    y_values,
                    width=bar_width,
                    color=_IMPL_COLORS.get(impl_name, _IMPL_COLORS["unknown"]),
                    label=impl_name,
                )

            # Configure axes and legend.
            tick_positions = [
                x + bar_width * (len(impl_names) - 1) / 2 for x in x_positions
            ]
            ax.set_xticks(tick_positions)
            ax.set_xticklabels(case_labels, rotation=20, ha="right")
            ax.set_ylabel("Time (s)")
            ax.set_title(f"{spec_name} {phase.title()} Benchmark", color="#111111")
            ax.grid(axis="y", linestyle=":", color="#E0E0E0")
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.tick_params(axis="x", colors="#111111")
            ax.tick_params(axis="y", colors="#111111")
            ax.legend(
                frameon=False,
                fontsize="small",
                loc="upper left",
                bbox_to_anchor=(1.02, 1),
            )

            # Save figure to docs image path.
            fig.tight_layout()
            output_name = (
                "benchmark_forward.png"
                if phase == "forward"
                else f"benchmark_{phase}.png"
            )
            fig.savefig(output_dir / output_name)
            plt.close(fig)


def main() -> int:
    # Parse command-line paths.
    parser = argparse.ArgumentParser(
        description="Generate functional benchmark bar plots from ASV results."
    )
    parser.add_argument("--results-dir", type=Path, default=Path(".asv/results"))
    parser.add_argument("--output-root", type=Path, default=Path("docs/nn/functional"))
    args = parser.parse_args()

    # Load the newest ASV result payload.
    results_file = _latest_result_file(args.results_dir)
    data = json.loads(results_file.read_text())

    # Extract benchmark vectors from the ASV payload.
    entry = _benchmark_entry(data)
    values, labels = _entry_vectors(entry)

    # Generate all benchmark plots.
    _plot_benchmarks(values, labels, args.output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
