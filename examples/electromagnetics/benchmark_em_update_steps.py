# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass

import numpy as np
import torch

from physicsnemo.nn.functional import (
    electric_field_update,
    magnetic_field_update,
    pml_electric_field_update,
    pml_initializer,
    pml_magnetic_field_update,
    pml_phi_e_update,
    pml_phi_h_update,
)


@dataclass
class BenchmarkResult:
    name: str
    elapsed_s: float
    steps: int
    domain_cells: int
    pml_cells_per_step: int

    @property
    def steps_per_s(self) -> float:
        return self.steps / self.elapsed_s

    @property
    def domain_mcell_s(self) -> float:
        return (self.domain_cells * self.steps_per_s) / 1.0e6

    @property
    def core_field_updates_mcell_s(self) -> float:
        # One H update + one E update each timestep.
        return (2 * self.domain_cells * self.steps_per_s) / 1.0e6

    @property
    def effective_kernel_updates_per_step(self) -> int:
        # No-PML path: H + E on full domain.
        # PML path adds phi_h + pml_h + phi_e + pml_e on each PML slab.
        return (2 * self.domain_cells) + (4 * self.pml_cells_per_step)

    @property
    def effective_kernel_mcell_s(self) -> float:
        return (self.effective_kernel_updates_per_step * self.steps_per_s) / 1.0e6


def _build_pml_layers(
    n: int,
    pml_thickness: int,
    dt: float,
    spacing: torch.Tensor,
    implementation: str | None,
    device: torch.device,
) -> list[tuple[torch.Tensor, tuple[int, int, int], int]]:
    courant_number = float(dt / float(torch.min(spacing).item()))
    boundaries = [
        ((1.0, 0.0, 0.0), (pml_thickness, n, n), (0, 0, 0)),
        ((-1.0, 0.0, 0.0), (pml_thickness, n, n), (n - pml_thickness, 0, 0)),
        ((0.0, 1.0, 0.0), (n, pml_thickness, n), (0, 0, 0)),
        ((0.0, -1.0, 0.0), (n, pml_thickness, n), (0, n - pml_thickness, 0)),
        ((0.0, 0.0, 1.0), (n, n, pml_thickness), (0, 0, 0)),
        ((0.0, 0.0, -1.0), (n, n, pml_thickness), (0, 0, n - pml_thickness)),
    ]

    layers: list[tuple[torch.Tensor, tuple[int, int, int], int]] = []
    for direction, shape, offset in boundaries:
        axis = int(np.argmax(np.abs(np.asarray(direction, dtype=np.float32))))
        thickness = int(shape[axis])
        layer = torch.zeros((36, *shape), device=device, dtype=torch.float32)
        layer = pml_initializer(
            layer,
            direction=direction,
            thickness=thickness,
            courant_number=courant_number,
            kappa=1.0,
            a=1.0e-8,
            implementation=implementation,
            inplace=True,
        )
        layers.append((layer, offset, shape[0] * shape[1] * shape[2]))
    return layers


def _synchronize(device: torch.device) -> None:
    if device.type == "cuda":
        torch.cuda.synchronize(device)


def _time_loop(
    step_fn,
    warmup_steps: int,
    timed_steps: int,
    device: torch.device,
) -> float:
    for _ in range(warmup_steps):
        step_fn()
    _synchronize(device)

    start = time.perf_counter()
    for _ in range(timed_steps):
        step_fn()
    _synchronize(device)
    end = time.perf_counter()
    return end - start


def run_benchmark(
    n: int,
    timed_steps: int,
    warmup_steps: int,
    pml_thickness: int,
    dt: float,
    implementation: str | None,
    seed: int,
    mode: str,
) -> dict:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    if device.type != "cuda":
        raise RuntimeError("This benchmark is intended for GPU execution but CUDA is unavailable.")

    torch.manual_seed(seed)
    np.random.seed(seed)

    spacing = torch.tensor([1.0, 1.0, 1.0], device=device, dtype=torch.float32)
    domain_cells = n * n * n

    electric0 = torch.randn((3, n, n, n), device=device, dtype=torch.float32)
    magnetic0 = torch.randn((3, n, n, n), device=device, dtype=torch.float32)

    results: list[BenchmarkResult] = []

    if mode in ("both", "no_pml"):
        electric = electric0.clone()
        magnetic = magnetic0.clone()

        def step_no_pml() -> None:
            magnetic_field_update(
                electric,
                magnetic,
                mu=1.0,
                sigma_m=0.0,
                spacing=spacing,
                dt=dt,
                implementation=implementation,
                inplace=True,
            )
            electric_field_update(
                electric,
                magnetic,
                eps=1.0,
                sigma_e=0.0,
                spacing=spacing,
                dt=dt,
                implementation=implementation,
                inplace=True,
            )

        no_pml_elapsed = _time_loop(
            step_fn=step_no_pml,
            warmup_steps=warmup_steps,
            timed_steps=timed_steps,
            device=device,
        )
        results.append(
            BenchmarkResult(
                name="no_pml",
                elapsed_s=no_pml_elapsed,
                steps=timed_steps,
                domain_cells=domain_cells,
                pml_cells_per_step=0,
            )
        )

    if mode in ("both", "with_pml"):
        electric = electric0.clone()
        magnetic = magnetic0.clone()
        pml_layers = _build_pml_layers(
            n=n,
            pml_thickness=pml_thickness,
            dt=dt,
            spacing=spacing,
            implementation=implementation,
            device=device,
        )
        pml_cells_per_step = sum(layer_cells for _, _, layer_cells in pml_layers)

        def step_with_pml() -> None:
            magnetic_field_update(
                electric,
                magnetic,
                mu=1.0,
                sigma_m=0.0,
                spacing=spacing,
                dt=dt,
                implementation=implementation,
                inplace=True,
            )
            for layer, offset, _ in pml_layers:
                pml_phi_h_update(
                    electric,
                    layer,
                    pml_layer_offset=offset,
                    implementation=implementation,
                    inplace=True,
                )
                pml_magnetic_field_update(
                    magnetic,
                    layer,
                    mu=1.0,
                    spacing=spacing,
                    pml_layer_offset=offset,
                    dt=dt,
                    implementation=implementation,
                    inplace=True,
                )

            electric_field_update(
                electric,
                magnetic,
                eps=1.0,
                sigma_e=0.0,
                spacing=spacing,
                dt=dt,
                implementation=implementation,
                inplace=True,
            )
            for layer, offset, _ in pml_layers:
                pml_phi_e_update(
                    magnetic,
                    layer,
                    pml_layer_offset=offset,
                    implementation=implementation,
                    inplace=True,
                )
                pml_electric_field_update(
                    electric,
                    layer,
                    eps=1.0,
                    spacing=spacing,
                    pml_layer_offset=offset,
                    dt=dt,
                    implementation=implementation,
                    inplace=True,
                )

        with_pml_elapsed = _time_loop(
            step_fn=step_with_pml,
            warmup_steps=warmup_steps,
            timed_steps=timed_steps,
            device=device,
        )
        results.append(
            BenchmarkResult(
                name="with_pml",
                elapsed_s=with_pml_elapsed,
                steps=timed_steps,
                domain_cells=domain_cells,
                pml_cells_per_step=pml_cells_per_step,
            )
        )

    return {
        "device": str(device),
        "grid": [n, n, n],
        "timed_steps": timed_steps,
        "warmup_steps": warmup_steps,
        "pml_thickness": pml_thickness,
        "dt": dt,
        "implementation": implementation if implementation is not None else "default",
        "results": [
            {
                "name": result.name,
                "elapsed_s": result.elapsed_s,
                "steps_per_s": result.steps_per_s,
                "domain_mcell_s": result.domain_mcell_s,
                "core_field_updates_mcell_s": result.core_field_updates_mcell_s,
                "effective_kernel_updates_mcell_s": result.effective_kernel_mcell_s,
                "effective_kernel_updates_per_step": result.effective_kernel_updates_per_step,
                **(
                    {"pml_cells_per_step": result.pml_cells_per_step}
                    if result.name == "with_pml"
                    else {}
                ),
            }
            for result in results
        ],
    }


def _print_human_readable(report: dict) -> None:
    print("EM update throughput benchmark")
    print(f"device: {report['device']}")
    print(f"grid: {report['grid'][0]}^3")
    print(f"timed_steps: {report['timed_steps']} (warmup={report['warmup_steps']})")
    print(f"implementation: {report['implementation']}")
    print("")
    for row in report["results"]:
        print(f"[{row['name']}]")
        print(f"  elapsed_s: {row['elapsed_s']:.6f}")
        print(f"  steps_per_s: {row['steps_per_s']:.2f}")
        print(f"  domain_mcell_s: {row['domain_mcell_s']:.2f}")
        print(f"  core_field_updates_mcell_s (E+H): {row['core_field_updates_mcell_s']:.2f}")
        print(
            "  effective_kernel_updates_mcell_s "
            f"(includes PML aux and correction updates): "
            f"{row['effective_kernel_updates_mcell_s']:.2f}"
        )
        if "pml_cells_per_step" in row:
            print(f"  pml_cells_per_step: {row['pml_cells_per_step']}")
        print("")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Throwaway throughput benchmark for EM updates with/without PML. "
            "Reports million cell updates per second."
        )
    )
    parser.add_argument("--n", type=int, default=192, help="Grid edge length (nx=ny=nz=n)")
    parser.add_argument("--timed-steps", type=int, default=400)
    parser.add_argument("--warmup-steps", type=int, default=80)
    parser.add_argument("--pml-thickness", type=int, default=12)
    parser.add_argument("--dt", type=float, default=0.35)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument(
        "--implementation",
        choices=("warp", "torch"),
        default=None,
        help="Optional backend override. Default uses FunctionSpec dispatch.",
    )
    parser.add_argument(
        "--mode",
        choices=("both", "no_pml", "with_pml"),
        default="both",
        help="Benchmark mode selection.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print final report as JSON only.",
    )
    args = parser.parse_args()

    report = run_benchmark(
        n=args.n,
        timed_steps=args.timed_steps,
        warmup_steps=args.warmup_steps,
        pml_thickness=args.pml_thickness,
        dt=args.dt,
        implementation=args.implementation,
        seed=args.seed,
        mode=args.mode,
    )

    if args.json:
        print(json.dumps(report, indent=2))
        return

    _print_human_readable(report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
