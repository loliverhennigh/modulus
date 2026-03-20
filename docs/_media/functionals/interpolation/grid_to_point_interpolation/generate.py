#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
import numpy as np
import torch
from PIL import Image

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from physicsnemo.nn.functional import grid_to_point_interpolation


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def _default_output_dir() -> Path:
    return _repo_root() / "docs/nn/functional/interpolation/grid_to_point_interpolation"


def _fig_to_rgb(fig: plt.Figure) -> np.ndarray:
    fig.canvas.draw()
    rgba = np.asarray(fig.canvas.buffer_rgba(), dtype=np.uint8)
    return rgba[..., :3].copy()


def _save_gif(frames: list[np.ndarray], path: Path, duration_ms: int = 90) -> None:
    images = [Image.fromarray(frame) for frame in frames]
    images[0].save(
        path,
        save_all=True,
        append_images=images[1:],
        duration=duration_ms,
        loop=0,
    )


def _build_context_grid(device: torch.device, n: int = 96) -> tuple[torch.Tensor, list[tuple[float, float, int]]]:
    grid = [(-1.0, 1.0, n), (-1.0, 1.0, n)]
    x = torch.linspace(-1.0, 1.0, n, device=device)
    y = torch.linspace(-1.0, 1.0, n, device=device)
    xx, yy = torch.meshgrid(x, y, indexing="ij")
    field = (
        torch.sin(2.2 * np.pi * xx)
        + 0.55 * torch.cos(1.8 * np.pi * yy)
        + 0.25 * torch.sin(2.0 * np.pi * xx * yy)
    )
    return field.unsqueeze(0).to(torch.float32).contiguous(), grid


def _generate_query_points(device: torch.device, n: int = 1800) -> torch.Tensor:
    t = torch.linspace(0.0, 1.0, n, device=device)
    x = 0.9 * torch.sin(3.0 * np.pi * t) * (0.2 + 0.8 * t)
    y = 0.9 * torch.cos(2.5 * np.pi * t) * (0.25 + 0.75 * t)
    jitter = 0.025 * torch.randn(n, 2, device=device)
    return torch.stack((x, y), dim=-1).add_(jitter).clamp_(-1.0, 1.0)


def main() -> None:
    # Parse command-line options for output location and compute device.
    parser = argparse.ArgumentParser(description="Generate grid-to-point interpolation visuals")
    parser.add_argument("--output-dir", type=Path, default=_default_output_dir())
    parser.add_argument(
        "--device",
        default="cuda" if torch.cuda.is_available() else "cpu",
        choices=("cpu", "cuda"),
    )
    args = parser.parse_args()

    # Build one representative grid field and query set.
    args.output_dir.mkdir(parents=True, exist_ok=True)
    device = torch.device(args.device)
    torch.manual_seed(2026)

    context_grid, grid = _build_context_grid(device=device, n=96)
    query_points = _generate_query_points(device=device, n=2200)

    sampled = grid_to_point_interpolation(
        query_points,
        context_grid,
        grid,
        interpolation_type="smooth_step_2",
    )

    grid_np = context_grid[0].detach().cpu().numpy()
    sampled_np = sampled[:, 0].detach().cpu().numpy()
    value_abs = max(
        float(np.max(np.abs(grid_np))),
        float(np.max(np.abs(sampled_np))),
        1.0e-6,
    )

    # Save animation of moving query points over a more pronounced grid background.
    fig, ax = plt.subplots(figsize=(5.4, 5.0), dpi=150)
    frames: list[np.ndarray] = []
    offsets = np.linspace(-0.45, 0.45, 22)

    for offset in offsets:
        shifted = query_points.clone()
        shifted[:, 0] = ((shifted[:, 0] + float(offset) + 1.0) % 2.0) - 1.0
        shifted_sampled = grid_to_point_interpolation(
            shifted,
            context_grid,
            grid,
            interpolation_type="smooth_step_2",
        )

        shifted_np = shifted.detach().cpu().numpy()
        shifted_val_np = shifted_sampled[:, 0].detach().cpu().numpy()

        ax.clear()
        ax.imshow(
            grid_np.T,
            origin="lower",
            extent=(-1, 1, -1, 1),
            cmap="gray",
            vmin=-value_abs,
            vmax=value_abs,
            alpha=0.95,
            interpolation="nearest",
        )
        ax.scatter(
            shifted_np[:, 0],
            shifted_np[:, 1],
            c=shifted_val_np,
            s=5,
            cmap="bwr",
            vmin=-value_abs,
            vmax=value_abs,
            alpha=0.95,
            edgecolors="k",
            linewidths=0.08,
        )
        ax.set_title("grid_to_point_interpolation: moving queries")
        ax.set_xticks([])
        ax.set_yticks([])
        frames.append(_fig_to_rgb(fig))

    _save_gif(frames, args.output_dir / "grid_to_point_queries.gif", duration_ms=85)
    plt.close(fig)


if __name__ == "__main__":
    main()
