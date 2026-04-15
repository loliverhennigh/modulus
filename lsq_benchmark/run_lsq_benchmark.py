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

"""Standalone benchmark for mesh_lsq_gradient on a complex unstructured case.

This script benchmarks forward latency for:
- Warp backend
- Eager torch backend
- torch.compile(mesh_lsq_gradient_torch)

and writes:
- CSV table with raw timings
- PNG line plot

Example
-------
python lsq_benchmark/run_lsq_benchmark.py \
    --device cuda \
    --num-points 512,1024,2048,3072 \
    --warmup 10 \
    --iters 50
"""

from __future__ import annotations

import argparse
import csv
import math
import time
from pathlib import Path

import matplotlib.pyplot as plt
import torch

from physicsnemo.nn.functional import mesh_lsq_gradient
from physicsnemo.nn.functional.derivatives.mesh_lsq_gradient._torch_impl import (
    mesh_lsq_gradient_torch,
)


def _parse_int_list(value: str) -> list[int]:
    parts = [x.strip() for x in value.split(",") if x.strip()]
    if not parts:
        raise argparse.ArgumentTypeError(
            "Expected a comma-separated non-empty int list."
        )
    try:
        out = [int(x) for x in parts]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid integer list: {value}") from exc
    if any(x <= 8 for x in out):
        raise argparse.ArgumentTypeError("Each point count must be > 8.")
    return out


def _sync_if_needed(device: torch.device) -> None:
    if device.type == "cuda":
        torch.cuda.synchronize(device=device)


def _pick_device(device_arg: str) -> torch.device:
    if device_arg == "auto":
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")
    device = torch.device(device_arg)
    if device.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA requested but torch.cuda.is_available() is False.")
    return device


def _choose_lattice_dims(n_points: int) -> tuple[int, int, int]:
    """Choose near-cubic lattice dimensions with nx*ny*nz >= n_points."""
    if n_points < 1:
        raise ValueError(f"n_points must be >= 1, got {n_points}.")
    nx = max(4, int(round(n_points ** (1.0 / 3.0))))
    ny = max(4, int(round((n_points / nx) ** 0.5)))
    nz = int(math.ceil(n_points / (nx * ny)))
    return nx, ny, nz


def _generate_complex_points_and_lattice(
    n_points: int,
    *,
    k_min: int,
    k_max: int,
    seed: int = 2026,
) -> tuple[torch.Tensor, tuple[int, int, int], torch.Tensor, torch.Tensor]:
    """Generate deterministic complex points plus lattice metadata.

    Returns
    -------
    points : torch.Tensor
        Permuted unstructured points, shape ``(n_points, 3)``.
    lattice_dims : tuple[int, int, int]
        Underlying toroidal lattice dimensions used for scalable neighborhood
        construction.
    perm : torch.Tensor
        New-to-old permutation indices, shape ``(n_points,)``.
    counts_old : torch.Tensor
        Per-point variable neighborhood counts in old lattice order.
    """
    g = torch.Generator(device="cpu")
    g.manual_seed(seed + n_points)
    nx, ny, nz = _choose_lattice_dims(n_points)

    old_idx = torch.arange(n_points, dtype=torch.int64)
    yz = ny * nz
    ix = torch.div(old_idx, yz, rounding_mode="floor")
    rem = old_idx - ix * yz
    iy = torch.div(rem, nz, rounding_mode="floor")
    iz = rem - iy * nz

    u = (ix.to(torch.float32) + 0.5) / float(nx)
    v = (iy.to(torch.float32) + 0.5) / float(ny)
    w = (iz.to(torch.float32) + 0.5) / float(nz)

    # Smooth warped field with anisotropy and nonlinear coupling.
    x = (
        1.7 * (u - 0.5)
        + 0.22 * torch.sin(2.0 * torch.pi * (3.0 * v + 0.7 * w))
        + 0.07 * torch.cos(2.0 * torch.pi * (5.0 * u - 1.3 * w))
    )
    y = (
        1.1 * (v - 0.5)
        + 0.19 * torch.sin(2.0 * torch.pi * (4.0 * w + 0.9 * u))
        + 0.08 * torch.cos(2.0 * torch.pi * (3.0 * v + 1.1 * u))
    )
    z = (
        1.4 * (w - 0.5)
        + 0.16 * torch.sin(2.0 * torch.pi * (4.0 * u - 0.8 * v))
        + 0.09 * torch.cos(2.0 * torch.pi * (3.0 * w + 1.7 * v))
    )

    points_old = torch.stack(
        (
            x + 0.25 * y * z,
            y + 0.18 * x * z,
            z + 0.15 * x * y,
        ),
        dim=-1,
    )
    jitter = 0.01 * (
        torch.rand(points_old.shape, generator=g, dtype=torch.float32) - 0.5
    )
    points_old = (points_old + jitter).contiguous()

    # Variable neighbor counts with smoothly varying complexity.
    raw = (
        0.5
        + 0.25 * torch.sin(2.0 * torch.pi * (2.0 * u + 1.0 * v))
        + 0.25 * torch.cos(2.0 * torch.pi * (1.0 * v + 2.0 * w))
    )
    raw = torch.clamp(raw, 0.0, 1.0)
    counts_old = (
        torch.round(float(k_min) + float(k_max - k_min) * raw)
        .to(dtype=torch.int64)
        .clamp_(k_min, k_max)
    )

    # Permute points to remove regular indexing.
    perm = torch.randperm(n_points, generator=g)
    points = points_old[perm].contiguous()
    return points, (nx, ny, nz), perm.contiguous(), counts_old.contiguous()


def _sorted_lattice_offsets(max_radius: int) -> list[tuple[int, int, int]]:
    offsets: list[tuple[int, int, int]] = []
    for dx in range(-max_radius, max_radius + 1):
        for dy in range(-max_radius, max_radius + 1):
            for dz in range(-max_radius, max_radius + 1):
                if dx == 0 and dy == 0 and dz == 0:
                    continue
                offsets.append((dx, dy, dz))
    offsets.sort(key=lambda d: d[0] * d[0] + d[1] * d[1] + d[2] * d[2])
    return offsets


def _build_variable_csr_local_torus(
    *,
    n_points: int,
    lattice_dims: tuple[int, int, int],
    perm: torch.Tensor,
    counts_old: torch.Tensor,
    k_min: int,
    k_max: int,
    max_radius: int = 3,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Build scalable variable CSR neighborhoods from local toroidal stencils."""
    if not (1 <= k_min <= k_max):
        raise ValueError(f"Require 1 <= k_min <= k_max, got {k_min}, {k_max}.")
    if k_max >= n_points:
        raise ValueError(f"k_max ({k_max}) must be < n_points ({n_points}).")

    nx, ny, nz = lattice_dims
    yz = ny * nz
    old_to_new = torch.empty((n_points,), dtype=torch.int64)
    old_to_new[perm] = torch.arange(n_points, dtype=torch.int64)
    counts_new = counts_old[perm].contiguous()

    offsets = torch.zeros((n_points + 1,), dtype=torch.int64)
    offsets[1:] = torch.cumsum(counts_new, dim=0)
    nnz = int(offsets[-1].item())
    indices = torch.empty((nnz,), dtype=torch.int64)

    lattice_offsets = _sorted_lattice_offsets(max_radius=max_radius)
    if len(lattice_offsets) < k_max:
        raise ValueError(
            f"max_radius={max_radius} yields only {len(lattice_offsets)} offsets; "
            f"need at least k_max={k_max}."
        )

    for new_i in range(n_points):
        old_i = int(perm[new_i].item())
        ix = old_i // yz
        rem = old_i - ix * yz
        iy = rem // nz
        iz = rem - iy * nz

        needed = int(counts_new[new_i].item())
        start = int(offsets[new_i].item())
        write = start

        for dx, dy, dz in lattice_offsets:
            if write - start >= needed:
                break
            jx = (ix + dx) % nx
            jy = (iy + dy) % ny
            jz = (iz + dz) % nz
            old_j = (jx * ny + jy) * nz + jz
            if old_j >= n_points or old_j == old_i:
                continue
            new_j = int(old_to_new[old_j].item())
            indices[write] = new_j
            write += 1

        # Fallback path in rare cases where tail masking under-fills.
        while write - start < needed:
            span = write - start + 1
            indices[write] = (new_i + span) % n_points
            if indices[write] == new_i:
                indices[write] = (indices[write] + 1) % n_points
            write += 1

    return offsets.contiguous(), indices.contiguous()


def _make_values(points: torch.Tensor, channels: int) -> torch.Tensor:
    """Generate smooth multi-channel scalar fields on points."""
    x = points[:, 0]
    y = points[:, 1]
    z = points[:, 2]
    features = [
        torch.sin(2.3 * x) + 0.4 * torch.cos(3.1 * y) + 0.15 * z.square(),
        torch.cos(1.7 * y + 0.3) + 0.25 * x * z,
        torch.sin(2.1 * z - 0.2) + 0.5 * x.square() - 0.3 * y,
        torch.sin(1.3 * x + 2.4 * y - 0.5 * z),
        torch.cos(2.6 * x - 1.1 * y + 0.8 * z),
        x * y + 0.5 * y * z - 0.3 * x * z,
    ]
    if channels < 1:
        raise ValueError("channels must be >= 1.")
    if channels == 1:
        return features[0].to(dtype=torch.float32).contiguous()
    if channels > len(features):
        # Extend deterministically using combinations.
        features.extend(
            (
                torch.sin((1.1 + 0.07 * i) * x)
                + torch.cos((1.7 + 0.05 * i) * y)
                + (0.1 + 0.01 * i) * z
            )
            for i in range(len(features), channels)
        )
    return torch.stack(features[:channels], dim=-1).to(dtype=torch.float32).contiguous()


def _time_callable(
    fn,
    *,
    warmup: int,
    iters: int,
    device: torch.device,
) -> float:
    for _ in range(warmup):
        _ = fn()
    _sync_if_needed(device)

    t0 = time.perf_counter()
    for _ in range(iters):
        _ = fn()
    _sync_if_needed(device)
    elapsed = time.perf_counter() - t0
    return 1.0e3 * elapsed / float(iters)


def _benchmark_one_size(
    *,
    n_points: int,
    device: torch.device,
    channels: int,
    k_min: int,
    k_max: int,
    weight_power: float,
    min_neighbors: int,
    safe_epsilon: float | None,
    warmup: int,
    iters: int,
    compile_mode: str,
    neighbor_build: str,
    local_radius: int,
) -> dict[str, float]:
    points_cpu, lattice_dims, perm, counts_old = _generate_complex_points_and_lattice(
        n_points,
        k_min=k_min,
        k_max=k_max,
    )
    build_mode = neighbor_build
    if build_mode == "auto":
        build_mode = "cdist" if n_points <= 4096 else "local"

    if build_mode == "cdist":
        # Exact KNN mode for smaller sizes.
        dists = torch.cdist(points_cpu, points_cpu)
        dists.fill_diagonal_(float("inf"))
        knn_dists, knn_indices = torch.topk(dists, k=k_max, largest=False, dim=1)
        k_ref = max(1, min(k_max - 1, (k_min + k_max) // 2))
        shell = knn_dists[:, k_ref]
        shell_norm = (shell - shell.min()) / (shell.max() - shell.min() + 1.0e-12)
        counts = torch.round(float(k_min) + float(k_max - k_min) * (1.0 - shell_norm))
        counts = counts.to(dtype=torch.int64).clamp_(k_min, k_max)
        offsets_cpu = torch.zeros((n_points + 1,), dtype=torch.int64)
        offsets_cpu[1:] = torch.cumsum(counts, dim=0)
        nnz = int(offsets_cpu[-1].item())
        indices_cpu = torch.empty((nnz,), dtype=torch.int64)
        cursor = 0
        for i in range(n_points):
            count = int(counts[i].item())
            indices_cpu[cursor : cursor + count] = knn_indices[i, :count]
            cursor += count
    else:
        offsets_cpu, indices_cpu = _build_variable_csr_local_torus(
            n_points=n_points,
            lattice_dims=lattice_dims,
            perm=perm,
            counts_old=counts_old,
            k_min=k_min,
            k_max=k_max,
            max_radius=local_radius,
        )

    values_cpu = _make_values(points_cpu, channels=channels)

    points = points_cpu.to(device=device, dtype=torch.float32).contiguous()
    values = values_cpu.to(device=device, dtype=torch.float32).contiguous()
    offsets = offsets_cpu.to(device=device, dtype=torch.int64).contiguous()
    indices = indices_cpu.to(device=device, dtype=torch.int64).contiguous()

    results: dict[str, float] = {"num_points": float(n_points)}

    def _warp():
        return mesh_lsq_gradient(
            points,
            values,
            offsets,
            indices,
            weight_power=weight_power,
            min_neighbors=min_neighbors,
            safe_epsilon=safe_epsilon,
            implementation="warp",
        )

    def _torch():
        return mesh_lsq_gradient(
            points,
            values,
            offsets,
            indices,
            weight_power=weight_power,
            min_neighbors=min_neighbors,
            safe_epsilon=safe_epsilon,
            implementation="torch",
        )

    def _torch_compiled_base():
        return mesh_lsq_gradient_torch(
            points,
            values,
            offsets,
            indices,
            weight_power=weight_power,
            min_neighbors=min_neighbors,
            safe_epsilon=safe_epsilon,
        )

    # Eager torch baseline is expected to exist.
    results["torch_ms"] = _time_callable(
        _torch, warmup=warmup, iters=iters, device=device
    )

    # Warp may not be available in all environments.
    try:
        results["warp_ms"] = _time_callable(
            _warp, warmup=warmup, iters=iters, device=device
        )
    except Exception as exc:  # pragma: no cover - runtime environment dependent
        print(f"[warn] warp benchmark failed at n={n_points}: {exc}")
        results["warp_ms"] = float("nan")

    # Compile and benchmark compiled torch if supported.
    if not hasattr(torch, "compile"):
        results["torch_compiled_ms"] = float("nan")
    else:
        try:
            compiled = torch.compile(
                _torch_compiled_base,
                mode=compile_mode,
                fullgraph=False,
                dynamic=False,
            )
            results["torch_compiled_ms"] = _time_callable(
                compiled,
                warmup=warmup + 1,  # one extra warmup for compile stabilization
                iters=iters,
                device=device,
            )
        except Exception as exc:  # pragma: no cover - runtime environment dependent
            print(f"[warn] compiled torch benchmark failed at n={n_points}: {exc}")
            results["torch_compiled_ms"] = float("nan")

    return results


def _write_csv(rows: list[dict[str, float]], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["num_points", "warp_ms", "torch_ms", "torch_compiled_ms"]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _plot_results(rows: list[dict[str, float]], png_path: Path) -> None:
    png_path.parent.mkdir(parents=True, exist_ok=True)
    xs = [int(r["num_points"]) for r in rows]
    warp = [r["warp_ms"] for r in rows]
    eager = [r["torch_ms"] for r in rows]
    compiled = [r["torch_compiled_ms"] for r in rows]

    plt.figure(figsize=(9.5, 5.8))
    plt.plot(xs, eager, marker="o", linewidth=2.2, label="torch")
    plt.plot(xs, compiled, marker="s", linewidth=2.2, label="compiled torch")
    plt.plot(xs, warp, marker="^", linewidth=2.2, label="warp")
    plt.xlabel("Number of points")
    plt.ylabel("Time per call (ms)")
    plt.title("mesh_lsq_gradient Forward Benchmark (Complex Unstructured Case)")
    plt.grid(alpha=0.35)
    plt.legend()
    plt.tight_layout()
    plt.savefig(png_path, dpi=180)
    plt.close()


def _print_table(rows: list[dict[str, float]]) -> None:
    print("\nBenchmark results (ms per call):")
    print(
        f"{'num_points':>10}  {'warp_ms':>12}  {'torch_ms':>12}  {'torch_compiled_ms':>18}"
    )
    for r in rows:
        print(
            f"{int(r['num_points']):>10}  "
            f"{r['warp_ms']:>12.4f}  "
            f"{r['torch_ms']:>12.4f}  "
            f"{r['torch_compiled_ms']:>18.4f}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--num-points",
        type=_parse_int_list,
        default=[512, 1024, 2048, 3072, 4096],
        help="Comma-separated point counts, e.g. 512,1024,2048",
    )
    parser.add_argument(
        "--device",
        type=str,
        default="auto",
        help="Device: auto|cpu|cuda|cuda:0",
    )
    parser.add_argument(
        "--channels", type=int, default=4, help="Number of value channels."
    )
    parser.add_argument(
        "--k-min",
        type=int,
        default=12,
        help="Minimum neighborhood size in variable CSR.",
    )
    parser.add_argument(
        "--k-max",
        type=int,
        default=48,
        help="Maximum neighborhood size in variable CSR.",
    )
    parser.add_argument("--weight-power", type=float, default=2.0)
    parser.add_argument("--min-neighbors", type=int, default=0)
    parser.add_argument(
        "--safe-epsilon",
        type=float,
        default=None,
        help="Distance epsilon floor. Default uses functional dtype-derived value.",
    )
    parser.add_argument("--warmup", type=int, default=8)
    parser.add_argument("--iters", type=int, default=40)
    parser.add_argument(
        "--compile-mode",
        type=str,
        default="reduce-overhead",
        choices=("default", "reduce-overhead", "max-autotune"),
    )
    parser.add_argument(
        "--neighbor-build",
        type=str,
        default="auto",
        choices=("auto", "local", "cdist"),
        help="Neighborhood build strategy: auto uses cdist for small sizes, local otherwise.",
    )
    parser.add_argument(
        "--local-radius",
        type=int,
        default=3,
        help="Toroidal lattice radius for local neighborhood construction.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("lsq_benchmark/lsq_benchmark_results.csv"),
    )
    parser.add_argument(
        "--output-png",
        type=Path,
        default=Path("lsq_benchmark/lsq_benchmark_plot.png"),
    )
    args = parser.parse_args()

    device = _pick_device(args.device)
    print(f"[info] device={device} points={args.num_points}")
    print(
        f"[info] channels={args.channels} k_min={args.k_min} k_max={args.k_max} "
        f"warmup={args.warmup} iters={args.iters} compile_mode={args.compile_mode} "
        f"neighbor_build={args.neighbor_build} local_radius={args.local_radius}"
    )

    rows: list[dict[str, float]] = []
    for n_points in args.num_points:
        print(f"[info] benchmarking n_points={n_points} ...")
        row = _benchmark_one_size(
            n_points=n_points,
            device=device,
            channels=args.channels,
            k_min=args.k_min,
            k_max=args.k_max,
            weight_power=args.weight_power,
            min_neighbors=args.min_neighbors,
            safe_epsilon=args.safe_epsilon,
            warmup=args.warmup,
            iters=args.iters,
            compile_mode=args.compile_mode,
            neighbor_build=args.neighbor_build,
            local_radius=args.local_radius,
        )
        rows.append(row)

    _write_csv(rows, args.output_csv)
    _plot_results(rows, args.output_png)
    _print_table(rows)
    print(f"\n[info] wrote CSV: {args.output_csv}")
    print(f"[info] wrote plot: {args.output_png}")


if __name__ == "__main__":
    main()
