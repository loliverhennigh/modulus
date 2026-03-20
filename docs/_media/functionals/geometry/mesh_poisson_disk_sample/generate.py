#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
import numpy as np
import open3d as o3d
import torch
from PIL import Image

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from physicsnemo.nn.functional import mesh_poisson_disk_sample


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def _default_output_dir() -> Path:
    return _repo_root() / "docs/nn/functional/geometry/mesh_poisson_disk_sample"


def _set_equal_axes(ax: plt.Axes, vertices: np.ndarray) -> None:
    mins = vertices.min(axis=0)
    maxs = vertices.max(axis=0)
    center = (mins + maxs) * 0.5
    radius = float((maxs - mins).max() * 0.55)
    ax.set_xlim(center[0] - radius, center[0] + radius)
    ax.set_ylim(center[1] - radius, center[1] + radius)
    ax.set_zlim(center[2] - radius, center[2] + radius)


def _plot_mesh(ax: plt.Axes, vertices: np.ndarray, faces: np.ndarray) -> None:
    # Draw the underlying mesh with visible triangle edges.
    ax.plot_trisurf(
        vertices[:, 0],
        vertices[:, 1],
        vertices[:, 2],
        triangles=faces,
        color=(0.72, 0.76, 0.86),
        alpha=0.42,
        linewidth=0.24,
        edgecolor=(0.22, 0.24, 0.30),
    )


def _fig_to_rgb(fig: plt.Figure) -> np.ndarray:
    fig.canvas.draw()
    rgba = np.asarray(fig.canvas.buffer_rgba(), dtype=np.uint8)
    return rgba[..., :3].copy()


def _save_gif(frames: list[np.ndarray], path: Path, duration_ms: int = 85) -> None:
    images = [Image.fromarray(frame) for frame in frames]
    images[0].save(
        path,
        save_all=True,
        append_images=images[1:],
        duration=duration_ms,
        loop=0,
    )


def _load_bunny_mesh(device: torch.device) -> tuple[torch.Tensor, torch.Tensor]:
    # Load the canonical Stanford Bunny mesh from Open3D sample data.
    bunny = o3d.data.BunnyMesh()
    mesh = o3d.io.read_triangle_mesh(bunny.path)
    vertices = np.asarray(mesh.vertices, dtype=np.float32)
    faces = np.asarray(mesh.triangles, dtype=np.int32)

    # Normalize bunny coordinates to a stable roughly [-1, 1] box.
    bbox_min = vertices.min(axis=0)
    bbox_max = vertices.max(axis=0)
    center = (bbox_min + bbox_max) * 0.5
    extent = float((bbox_max - bbox_min).max())
    extent = extent if extent > 0.0 else 1.0
    vertices = (vertices - center) * (2.0 / extent)

    # Re-orient bunny so original +y maps to plot +z (upright in matplotlib).
    rot_x_pos_90 = np.array(
        [[1.0, 0.0, 0.0], [0.0, 0.0, -1.0], [0.0, 1.0, 0.0]], dtype=np.float32
    )
    vertices = vertices @ rot_x_pos_90.T

    return (
        torch.from_numpy(vertices).to(device=device, dtype=torch.float32).contiguous(),
        torch.from_numpy(faces).to(device=device, dtype=torch.int32).contiguous(),
    )


def main() -> None:
    # Parse command-line options for output location and compute device.
    parser = argparse.ArgumentParser(description="Generate mesh Poisson-disk visuals")
    parser.add_argument("--output-dir", type=Path, default=_default_output_dir())
    parser.add_argument(
        "--device",
        default="cuda" if torch.cuda.is_available() else "cpu",
        choices=("cpu", "cuda"),
    )
    args = parser.parse_args()

    # Build a representative mesh and run both sampling modes at similar point count.
    args.output_dir.mkdir(parents=True, exist_ok=True)
    device = torch.device(args.device)
    vertices, faces = _load_bunny_mesh(device)

    target_count = 700
    dart_samples = mesh_poisson_disk_sample(
        vertices,
        faces,
        min_distance=0.07,
        batch_size=16_384,
        max_points=target_count,
        max_iterations=20,
        random_seed=2026,
        mode="dart_throwing",
    )
    wse_samples = mesh_poisson_disk_sample(
        vertices,
        faces,
        min_distance=0.07,
        mode="weighted_sample_elimination",
        batch_size=16_384,
        max_points=2_800,
        target_num_points=target_count,
        max_iterations=20,
        random_seed=2026,
        open3d_init_factor=4,
    )

    vertices_np = vertices.detach().cpu().numpy()
    faces_np = faces.detach().cpu().numpy().reshape(-1, 3)
    dart_np = dart_samples.detach().cpu().numpy()
    wse_np = wse_samples.detach().cpu().numpy()

    # Save one comparison image for docs.
    fig = plt.figure(figsize=(12, 5), dpi=180)
    ax1 = fig.add_subplot(1, 2, 1, projection="3d")
    _plot_mesh(ax1, vertices_np, faces_np)
    ax1.scatter(
        dart_np[:, 0],
        dart_np[:, 1],
        dart_np[:, 2],
        s=5,
        c="#194c7f",
        alpha=0.95,
        depthshade=True,
    )
    _set_equal_axes(ax1, vertices_np)
    ax1.view_init(elev=20, azim=30)
    ax1.set_title(f"Dart Throwing ({dart_np.shape[0]} samples)")
    ax1.set_axis_off()

    ax2 = fig.add_subplot(1, 2, 2, projection="3d")
    _plot_mesh(ax2, vertices_np, faces_np)
    ax2.scatter(
        wse_np[:, 0],
        wse_np[:, 1],
        wse_np[:, 2],
        s=5,
        c="#194c7f",
        alpha=0.95,
        depthshade=True,
    )
    _set_equal_axes(ax2, vertices_np)
    ax2.view_init(elev=20, azim=30)
    ax2.set_title(f"Weighted Elimination ({wse_np.shape[0]} samples)")
    ax2.set_axis_off()

    fig.suptitle("mesh_poisson_disk_sample", y=0.98)
    fig.tight_layout()
    fig.savefig(args.output_dir / "mesh_poisson_modes.png", bbox_inches="tight")
    plt.close(fig)

    # Save a rotating gif for side-by-side visual comparison.
    fig = plt.figure(figsize=(12, 5), dpi=140)
    ax1 = fig.add_subplot(1, 2, 1, projection="3d")
    ax2 = fig.add_subplot(1, 2, 2, projection="3d")
    frames: list[np.ndarray] = []
    for azim in np.linspace(20.0, 140.0, 24):
        ax1.clear()
        ax2.clear()

        _plot_mesh(ax1, vertices_np, faces_np)
        ax1.scatter(
            dart_np[:, 0],
            dart_np[:, 1],
            dart_np[:, 2],
            s=5,
            c="#194c7f",
            alpha=0.95,
            depthshade=True,
        )
        _set_equal_axes(ax1, vertices_np)
        ax1.view_init(elev=18, azim=float(azim))
        ax1.set_title("Dart Throwing")
        ax1.set_axis_off()

        _plot_mesh(ax2, vertices_np, faces_np)
        ax2.scatter(
            wse_np[:, 0],
            wse_np[:, 1],
            wse_np[:, 2],
            s=5,
            c="#194c7f",
            alpha=0.95,
            depthshade=True,
        )
        _set_equal_axes(ax2, vertices_np)
        ax2.view_init(elev=18, azim=float(azim))
        ax2.set_title("Weighted Elimination")
        ax2.set_axis_off()
        frames.append(_fig_to_rgb(fig))
    _save_gif(frames, args.output_dir / "mesh_poisson_modes.gif")
    plt.close(fig)


if __name__ == "__main__":
    main()
