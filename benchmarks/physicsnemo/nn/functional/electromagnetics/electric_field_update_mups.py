#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import time

import torch

from physicsnemo.nn.functional import electric_field_update


def build_inputs(n: int, device: torch.device):
    electric_field = torch.randn(3, n, n, n, device=device, dtype=torch.float32)
    magnetic_field = torch.randn(3, n, n, n, device=device, dtype=torch.float32)
    eps = torch.empty(n, n, n, device=device, dtype=torch.float32).uniform_(1.0, 6.0)
    sigma_e = torch.empty(n, n, n, device=device, dtype=torch.float32).uniform_(
        0.0, 0.03
    )
    spacing = torch.tensor([0.01, 0.01, 0.01], device=device, dtype=torch.float32)
    dt = 0.001
    return electric_field, magnetic_field, eps, sigma_e, spacing, dt


def synchronize(device: torch.device):
    if device.type == "cuda":
        torch.cuda.synchronize(device)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Measure electric_field_update throughput"
    )
    parser.add_argument("--grid", type=int, default=256)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--steps", type=int, default=20)
    parser.add_argument(
        "--device", type=str, default="cuda" if torch.cuda.is_available() else "cpu"
    )
    parser.add_argument(
        "--implementation", type=str, default="warp", choices=["warp", "torch"]
    )
    args = parser.parse_args()

    device = torch.device(args.device)
    inputs = build_inputs(args.grid, device)

    for _ in range(args.warmup):
        electric_field_update(*inputs, implementation=args.implementation, inplace=True)
    synchronize(device)

    start = time.perf_counter()
    for _ in range(args.steps):
        electric_field_update(*inputs, implementation=args.implementation, inplace=True)
    synchronize(device)
    elapsed = time.perf_counter() - start

    updates = args.grid * args.grid * args.grid * args.steps
    mups = updates / elapsed / 1e6

    print(f"implementation={args.implementation}")
    print(f"device={device}")
    print(f"grid={args.grid}^3")
    print(f"steps={args.steps}")
    print(f"elapsed_s={elapsed:.6f}")
    print(f"mups={mups:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
