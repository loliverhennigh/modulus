#!/usr/bin/env python3
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

"""Generate bar plots for functional benchmarks from ASV results."""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
from pathlib import Path
from typing import Any, Iterable

_DEFAULT_CASE_COUNT = 3
_SPEC_CASE_LABELS = {
    "DropPath": ["small-b8-f64", "medium-b16-f256", "large-b32-f1024"],
    "KNN": [
        "small-p1024-q256-k16",
        "medium-p4096-q1024-k32",
        "large-p8192-q2048-k32",
    ],
    "RFFT": ["small-n4096", "medium-n16384", "large-n65536"],
    "RFFT2": ["small-h128-w128", "medium-h256-w256", "large-h512-w512"],
    "RadiusSearch": [
        "small-p1024-q512-r0.1",
        "medium-p4096-q2048-r0.1",
        "large-p8192-q4096-r0.1",
    ],
    "SignedDistanceField": ["small-n4096", "medium-n16384", "large-n65536"],
    "IRFFT": ["small-n4096", "medium-n16384", "large-n65536"],
    "IRFFT2": ["small-h128-w128", "medium-h256-w256", "large-h512-w512"],
    "Interpolation": [
        "1d-nearest-g2048-n8192",
        "1d-linear-g2048-n8192",
        "2d-smooth1-g128-n1024",
        "2d-smooth2-g128-n1024",
        "3d-linear-g32-n512",
        "3d-smooth2-g32-n512",
        "3d-gaussian-g32-n512",
    ],
}
_SPEC_OUTPUT_SLUG = {
    "DropPath": "drop_path",
    "KNN": "knn",
    "RFFT": "rfft",
    "RFFT2": "rfft2",
    "RadiusSearch": "radius_search",
    "SignedDistanceField": "sdf",
    "IRFFT": "irfft",
    "IRFFT2": "irfft2",
    "Interpolation": "interpolation",
}
_SPEC_IMPLEMENTATIONS = {
    "DropPath": ("warp", "torch"),
    "KNN": ("cuml", "scipy", "torch"),
    "RFFT": ("torch",),
    "RFFT2": ("torch",),
    "RadiusSearch": ("warp", "torch"),
    "SignedDistanceField": ("warp",),
    "IRFFT": ("torch",),
    "IRFFT2": ("torch",),
    "Interpolation": ("warp", "torch"),
}
_IMPL_ORDER = ("warp", "cuml", "scipy", "torch")
_IMPL_COLORS = {
    "warp": "#76B900",  # NVIDIA green
    "cuml": "#2E2E2E",
    "scipy": "#5A5A5A",
    "torch": "#111111",
    "unknown": "#8A8A8A",
}

def _case_labels(spec_name: str) -> list[str]:
    labels = list(_SPEC_CASE_LABELS.get(spec_name, []))
    if not labels:
        labels = [f"case-{idx}" for idx in range(_DEFAULT_CASE_COUNT)]
    return labels


def _build_params() -> list[tuple[str, str, int]]:
    params: list[tuple[str, str, int]] = []
    for spec_name, implementations in _SPEC_IMPLEMENTATIONS.items():
        case_count = len(_case_labels(spec_name))
        for implementation in implementations:
            for case_index in range(case_count):
                params.append((spec_name, implementation, case_index))
    return params


_PARAMS = _build_params()

_BENCHMARK_SUFFIX = "FunctionalBenchmarks.time_functional"


def _find_latest_results(results_dir: Path) -> dict[str, Any] | None:
    candidates = [
        path
        for path in results_dir.rglob("*.json")
        if path.name not in {"benchmarks.json", "machine.json"}
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    for path in candidates:
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
    return None


def _iter_dicts(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from _iter_dicts(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _iter_dicts(nested)


def _find_benchmark_entry(data: dict[str, Any]) -> Any | None:
    for mapping in _iter_dicts(data):
        for key, value in mapping.items():
            if isinstance(key, str) and _BENCHMARK_SUFFIX in key:
                return value
    return None


def _coerce_value(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not math.isnan(value):
        return float(value)
    if isinstance(value, list):
        flattened: list[float] = []
        for item in value:
            if isinstance(item, (int, float)) and not math.isnan(item):
                flattened.append(float(item))
            elif isinstance(item, list):
                flattened.extend(
                    float(sub)
                    for sub in item
                    if isinstance(sub, (int, float)) and not math.isnan(sub)
                )
        if flattened:
            return float(statistics.median(flattened))
    return None


def _normalize_results(entry: Any) -> list[Any]:
    if isinstance(entry, dict):
        entry = entry.get("result", entry.get("results"))
    if entry is None:
        return []
    return entry if isinstance(entry, list) else [entry]


def _slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "case"


def _plot_benchmarks(results: list[float | None], output_root: Path) -> None:
    import matplotlib.pyplot as plt

    spec_names = _SPEC_CASE_LABELS.keys()
    case_labels = {spec_name: _case_labels(spec_name) for spec_name in spec_names}

    data: dict[str, dict[str, dict[str, float]]] = {}
    labels = results[1] if len(results) > 1 and isinstance(results[1], list) else None
    if labels and len(labels) == 1 and isinstance(labels[0], list):
        labels = labels[0]
    values = results[0] if results else []
    if labels is None or len(labels) != len(values):
        labels = [str(param) for param in _PARAMS[: len(values)]]
    for label, value in zip(labels, values):
        if value is None:
            continue
        spec_name = "Unknown"
        implementation = "unknown"
        case_index = 0
        if isinstance(label, str):
            try:
                spec_impl = eval(label, {}, {})
            except (SyntaxError, ValueError, NameError):
                spec_impl = None
            if (
                isinstance(spec_impl, tuple)
                and len(spec_impl) == 3
                and isinstance(spec_impl[0], str)
            ):
                spec_name, implementation, case_index = spec_impl
        label_list = case_labels.get(spec_name, [])
        case_label = (
            label_list[case_index]
            if case_index < len(label_list)
            else f"case-{case_index}"
        )
        spec_cases = data.setdefault(spec_name, {})
        impl_map = spec_cases.setdefault(case_label, {})
        impl_map[implementation] = value

    for spec_name, case_map in data.items():
        output_dir = output_root / _SPEC_OUTPUT_SLUG.get(spec_name, spec_name.lower())
        output_dir.mkdir(parents=True, exist_ok=True)

        case_labels_sorted = list(case_map.keys())
        impl_names: list[str] = []
        for impl_map in case_map.values():
            for impl_name in impl_map.keys():
                if impl_name not in impl_names:
                    impl_names.append(impl_name)
        impl_names.sort(
            key=lambda name: (_IMPL_ORDER.index(name) if name in _IMPL_ORDER else 99)
        )

        if not case_labels_sorted or not impl_names:
            continue

        fig, ax = plt.subplots(figsize=(8, 4))
        fig.patch.set_facecolor("white")
        ax.set_facecolor("white")

        group_count = len(case_labels_sorted)
        bar_width = 0.8 / max(len(impl_names), 1)
        x_positions = list(range(group_count))

        for idx, impl_name in enumerate(impl_names):
            offsets = [x + idx * bar_width for x in x_positions]
            values = [
                case_map[label].get(impl_name, float("nan"))
                for label in case_labels_sorted
            ]
            ax.bar(
                offsets,
                values,
                width=bar_width,
                color=_IMPL_COLORS.get(impl_name, _IMPL_COLORS["unknown"]),
                label=impl_name,
            )

        tick_positions = [
            x + bar_width * (len(impl_names) - 1) / 2 for x in x_positions
        ]
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(case_labels_sorted, rotation=20, ha="right")
        ax.set_ylabel("Time (s)")
        ax.set_title(f"{spec_name} Benchmark", color="#111111")
        ax.grid(axis="y", linestyle=":", color="#E0E0E0")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.tick_params(axis="x", colors="#111111")
        ax.tick_params(axis="y", colors="#111111")
        ax.legend(
            frameon=False, fontsize="small", loc="upper left", bbox_to_anchor=(1.02, 1)
        )
        fig.tight_layout()
        fig.savefig(output_dir / "benchmark.png")
        plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate functional benchmark bar plots from ASV results."
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(".asv/results"),
        help="Path to the ASV results directory.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("docs/nn/functional"),
        help="Root directory for generated plot images.",
    )
    args = parser.parse_args()

    if not args.results_dir.exists():
        print(f"ASV results directory not found: {args.results_dir}")
        return 0

    data = _find_latest_results(args.results_dir)
    if data is None:
        print(f"No ASV results found under: {args.results_dir}")
        return 0

    entry = _find_benchmark_entry(data)
    if entry is None:
        print("No functional benchmark results found in ASV data.")
        return 0

    results = _normalize_results(entry)
    if not results:
        print("Functional benchmark results are empty.")
        return 0

    _plot_benchmarks(results, args.output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
