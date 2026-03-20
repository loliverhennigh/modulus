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

from physicsnemo.nn.functional import point_to_grid_interpolation


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def _default_output_dir() -> Path:
    return _repo_root() / "docs/nn/functional/interpolation/point_to_grid_interpolation"


def _fig_to_rgb(fig: plt.Figure) -> np.ndarray:
    fig.canvas.draw()
    rgba = np.asarray(fig.canvas.buffer_rgba(), dtype=np.uint8)
    return rgba[..., :3].copy()


def _save_gif(frames: list[np.ndarray], path: Path, duration_ms: int = 95) -> None:
    images = [Image.fromarray(frame) for frame in frames]
    images[0].save(
        path,
        save_all=True,
        append_images=images[1:],
        duration=duration_ms,
        loop=0,
    )


def _sample_points(device: torch.device, n: int = 2400) -> tuple[torch.Tensor, torch.Tensor]:
    t = torch.linspace(0.0, 1.0, n, device=device)
    x = 0.85 * torch.sin(6.0 * np.pi * t)
    y = (1.6 * t - 0.8) + 0.22 * torch.sin(9.0 * np.pi * t)
    points = torch.stack((x, y), dim=-1)
    points = points + 0.03 * torch.randn_like(points)
    points = points.clamp_(-1.0, 1.0)

    values = (
        0.8 * torch.sin(2.5 * np.pi * points[:, 0])
        + 0.5 * torch.cos(2.0 * np.pi * points[:, 1])
        + 0.25 * torch.sin(2.0 * np.pi * points[:, 0] * points[:, 1])
    )
    return points.contiguous(), values.unsqueeze(-1).to(torch.float32).contiguous()


def main() -> None:
    # Parse command-line options for output location and compute device.
    parser = argparse.ArgumentParser(description="Generate point-to-grid interpolation visuals")
    parser.add_argument("--output-dir", type=Path, default=_default_output_dir())
    parser.add_argument(
        "--device",
        default="cuda" if torch.cuda.is_available() else "cpu",
        choices=("cpu", "cuda"),
    )
    args = parser.parse_args()

    # Build one representative point cloud and rasterize it onto a grid.
    args.output_dir.mkdir(parents=True, exist_ok=True)
    device = torch.device(args.device)
    torch.manual_seed(2026)

    grid = [(-1.0, 1.0, 96), (-1.0, 1.0, 96)]
    points, values = _sample_points(device=device, n=2600)

    gridded = point_to_grid_interpolation(
        points,
        values,
        grid,
        interpolation_type="smooth_step_2",
        implementation="torch",
    )

    points_np = points.detach().cpu().numpy()
    values_np = values[:, 0].detach().cpu().numpy()
    grid_np = gridded[0].detach().cpu().numpy()
    value_abs = max(
        float(np.max(np.abs(values_np))),
        float(np.max(np.abs(grid_np))),
        1.0e-6,
    )

    # Save overview image with point samples and rasterized field.
    fig, axes = plt.subplots(1, 2, figsize=(11.0, 4.5), dpi=180)

    sc = axes[0].scatter(
        points_np[:, 0],
        points_np[:, 1],
        c=values_np,
        s=5,
        cmap="coolwarm",
        vmin=-value_abs,
        vmax=value_abs,
        alpha=0.9,
    )
    axes[0].set_xlim(-1.0, 1.0)
    axes[0].set_ylim(-1.0, 1.0)
    axes[0].set_title("Input Point Values")
    axes[0].set_xlabel("x")
    axes[0].set_ylabel("y")
    fig.colorbar(sc, ax=axes[0], fraction=0.046, pad=0.04)

    im = axes[1].imshow(
        grid_np.T,
        origin="lower",
        extent=(-1, 1, -1, 1),
        cmap="coolwarm",
        vmin=-value_abs,
        vmax=value_abs,
        interpolation="nearest",
    )
    axes[1].set_title("Rasterized Grid Output")
    axes[1].set_xlabel("x")
    axes[1].set_ylabel("y")
    fig.colorbar(im, ax=axes[1], fraction=0.046, pad=0.04)

    fig.suptitle("point_to_grid_interpolation", y=1.02)
    fig.tight_layout()
    fig.savefig(args.output_dir / "point_to_grid_overview.png", bbox_inches="tight")
    plt.close(fig)

    # Save animation showing convergence as more points are rasterized.
    fig, axes = plt.subplots(1, 2, figsize=(9.8, 4.4), dpi=150)
    frames: list[np.ndarray] = []
    counts = np.linspace(200, points.shape[0], 18, dtype=int)

    for count in counts:
        subset_points = points[:count]
        subset_values = values[:count]
        subset_grid = point_to_grid_interpolation(
            subset_points,
            subset_values,
            grid,
            interpolation_type="smooth_step_2",
            implementation="torch",
        )[0]

        subset_points_np = subset_points.detach().cpu().numpy()
        subset_values_np = subset_values[:, 0].detach().cpu().numpy()
        subset_grid_np = subset_grid.detach().cpu().numpy()

        axes[0].clear()
        axes[1].clear()

        axes[0].scatter(
            subset_points_np[:, 0],
            subset_points_np[:, 1],
            c=subset_values_np,
            s=4,
            cmap="coolwarm",
            vmin=-value_abs,
            vmax=value_abs,
            alpha=0.9,
        )
        axes[0].set_xlim(-1.0, 1.0)
        axes[0].set_ylim(-1.0, 1.0)
        axes[0].set_title(f"Points used: {count}")
        axes[0].set_xticks([])
        axes[0].set_yticks([])

        axes[1].imshow(
            subset_grid_np.T,
            origin="lower",
            extent=(-1, 1, -1, 1),
            cmap="coolwarm",
            vmin=-value_abs,
            vmax=value_abs,
            interpolation="nearest",
        )
        axes[1].set_title("Rasterized field")
        axes[1].set_xticks([])
        axes[1].set_yticks([])

        frames.append(_fig_to_rgb(fig))

    _save_gif(frames, args.output_dir / "point_to_grid_convergence.gif", duration_ms=100)
    plt.close(fig)


if __name__ == "__main__":
    main()
