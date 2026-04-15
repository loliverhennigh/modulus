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

from physicsnemo.nn.functional import mesh_to_voxel_fraction


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def _default_output_dir() -> Path:
    return _repo_root() / "docs/nn/functional/geometry/mesh_to_voxel_fraction"


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


def _set_equal_axes(ax: plt.Axes, points: np.ndarray) -> None:
    mins = points.min(axis=0)
    maxs = points.max(axis=0)
    center = (mins + maxs) * 0.5
    radius = float((maxs - mins).max() * 0.56)
    ax.set_xlim(center[0] - radius, center[0] + radius)
    ax.set_ylim(center[1] - radius, center[1] + radius)
    ax.set_zlim(center[2] - radius, center[2] + radius)


def _plot_mesh(ax: plt.Axes, vertices: np.ndarray, faces: np.ndarray) -> None:
    ax.plot_trisurf(
        vertices[:, 0],
        vertices[:, 1],
        vertices[:, 2],
        triangles=faces,
        color=(0.70, 0.74, 0.85),
        alpha=0.38,
        linewidth=0.24,
        edgecolor=(0.22, 0.24, 0.30),
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
    parser = argparse.ArgumentParser(description="Generate mesh-to-voxel visuals")
    parser.add_argument("--output-dir", type=Path, default=_default_output_dir())
    parser.add_argument(
        "--device",
        default="cuda" if torch.cuda.is_available() else "cpu",
        choices=("cpu", "cuda"),
    )
    args = parser.parse_args()

    # Build one representative mesh and voxelize it.
    args.output_dir.mkdir(parents=True, exist_ok=True)
    device = torch.device(args.device)

    vertices, faces = _load_bunny_mesh(device)

    bbox_min = vertices.min(dim=0).values
    bbox_max = vertices.max(dim=0).values
    extent = float((bbox_max - bbox_min).amax().detach().cpu().item())
    padding = 0.12 * extent
    grid_n = 40

    origin = (bbox_min - padding).to(torch.float32).contiguous()
    voxel_size = (extent + 2.0 * padding) / float(grid_n)

    voxels = mesh_to_voxel_fraction(
        vertices,
        faces,
        origin,
        voxel_size,
        (grid_n, grid_n, grid_n),
        n_samples=24,
        seed=2026,
        open_mesh=True,
        winding_number_threshold=0.5,
        winding_number_accuracy=2.0,
    )
    voxels_np = voxels.detach().cpu().numpy()
    vertices_np = vertices.detach().cpu().numpy()
    faces_np = faces.detach().cpu().numpy().reshape(-1, 3)

    # Build world-space occupied voxel centers for side-by-side 3D comparison.
    occupied = np.argwhere(voxels_np > 0.35)
    occ_values = voxels_np[occupied[:, 0], occupied[:, 1], occupied[:, 2]]
    occ_xyz = np.stack(
        (
            origin[0].item() + (occupied[:, 2] + 0.5) * voxel_size,
            origin[1].item() + (occupied[:, 1] + 0.5) * voxel_size,
            origin[2].item() + (occupied[:, 0] + 0.5) * voxel_size,
        ),
        axis=1,
    )

    # Save a rotating side-by-side GIF: mesh triangles (left) vs occupied voxels (right).
    fig = plt.figure(figsize=(10.0, 4.8), dpi=150)
    ax_mesh = fig.add_subplot(1, 2, 1, projection="3d")
    ax_vox = fig.add_subplot(1, 2, 2, projection="3d")
    frames: list[np.ndarray] = []
    for azim in np.linspace(22.0, 138.0, 24):
        ax_mesh.clear()
        ax_vox.clear()

        _plot_mesh(ax_mesh, vertices_np, faces_np)
        _set_equal_axes(ax_mesh, vertices_np)
        ax_mesh.view_init(elev=20, azim=float(azim))
        ax_mesh.set_title("Input Mesh (triangles visible)")
        ax_mesh.set_axis_off()

        if occ_xyz.shape[0] > 0:
            ax_vox.scatter(
                occ_xyz[:, 0],
                occ_xyz[:, 1],
                occ_xyz[:, 2],
                c=occ_values,
                s=4,
                alpha=0.55,
                cmap="viridis",
                vmin=0.0,
                vmax=1.0,
            )
        _set_equal_axes(ax_vox, vertices_np)
        ax_vox.view_init(elev=20, azim=float(azim))
        ax_vox.set_title("Occupied Voxels")
        ax_vox.set_axis_off()
        frames.append(_fig_to_rgb(fig))
    _save_gif(frames, args.output_dir / "mesh_to_voxel_rotation.gif", duration_ms=85)
    plt.close(fig)


if __name__ == "__main__":
    main()
