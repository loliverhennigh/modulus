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

from physicsnemo.nn.functional import signed_distance_field


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def _default_output_dir() -> Path:
    return _repo_root() / "docs/nn/functional/geometry/sdf"


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
        edgecolor=(0.20, 0.22, 0.30),
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


def _evaluate_slice(
    mesh_vertices: torch.Tensor,
    mesh_indices: torch.Tensor,
    z_value: float,
    n: int = 120,
    extent: float = 1.4,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    x = torch.linspace(-extent, extent, n, device=mesh_vertices.device)
    y = torch.linspace(-extent, extent, n, device=mesh_vertices.device)
    xx, yy = torch.meshgrid(x, y, indexing="ij")
    zz = torch.full_like(xx, float(z_value))
    points = torch.stack((xx, yy, zz), dim=-1).reshape(-1, 3)

    sdf_values, _ = signed_distance_field(
        mesh_vertices,
        mesh_indices,
        points,
        use_sign_winding_number=True,
    )
    sdf_slice = sdf_values.reshape(n, n).detach().cpu().numpy()
    return xx.detach().cpu().numpy(), yy.detach().cpu().numpy(), sdf_slice


def main() -> None:
    # Parse command-line options for output location and compute device.
    parser = argparse.ArgumentParser(description="Generate signed-distance-field visuals")
    parser.add_argument("--output-dir", type=Path, default=_default_output_dir())
    parser.add_argument(
        "--device",
        default="cuda" if torch.cuda.is_available() else "cpu",
        choices=("cpu", "cuda"),
    )
    args = parser.parse_args()

    # Build one representative mesh and evaluate SDF slices.
    args.output_dir.mkdir(parents=True, exist_ok=True)
    device = torch.device(args.device)

    mesh_vertices, mesh_indices = _load_bunny_mesh(device)
    verts_np = mesh_vertices.detach().cpu().numpy()
    faces_np = mesh_indices.detach().cpu().numpy().reshape(-1, 3)

    xx, yy, sdf0 = _evaluate_slice(mesh_vertices, mesh_indices, z_value=0.0)

    # Save a centered slice image with signed-distance contouring.
    fig, ax = plt.subplots(figsize=(6.5, 5.5), dpi=180)
    contour = ax.contourf(xx, yy, sdf0, levels=41, cmap="coolwarm")
    # Draw a high-contrast double contour so the slice/mesh intersection is visible.
    ax.contour(xx, yy, sdf0, levels=[0.0], colors=["white"], linewidths=2.4)
    ax.contour(xx, yy, sdf0, levels=[0.0], colors=["black"], linewidths=1.4)
    ax.set_title("signed_distance_field (z=0 slice)")
    ax.set_xlabel("x")
    ax.set_ylabel("y")
    ax.set_aspect("equal")
    cbar = fig.colorbar(contour, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("Signed Distance")
    fig.tight_layout()
    fig.savefig(args.output_dir / "sdf_slice_overview.png", bbox_inches="tight")
    plt.close(fig)

    # Save a split-view GIF: mesh with sweeping plane (left), SDF slice image (right).
    fig = plt.figure(figsize=(10.8, 4.8), dpi=150)
    ax_mesh = fig.add_subplot(1, 2, 1, projection="3d")
    ax_sdf = fig.add_subplot(1, 2, 2)

    z_values = np.linspace(-0.9, 0.9, 16)
    sdf_max = 0.0
    for z_value in z_values:
        _, _, sdf_slice = _evaluate_slice(
            mesh_vertices, mesh_indices, z_value=float(z_value), n=96
        )
        sdf_max = max(sdf_max, float(np.max(np.abs(sdf_slice))))
    sdf_max = max(sdf_max, 1.0e-6)

    frames: list[np.ndarray] = []
    cmap = plt.get_cmap("coolwarm")
    for z_value in z_values:
        xx, yy, sdf_slice = _evaluate_slice(
            mesh_vertices, mesh_indices, z_value=float(z_value), n=96
        )

        ax_mesh.clear()
        _plot_mesh(ax_mesh, verts_np, faces_np)
        plane_z = np.full_like(xx, float(z_value))
        face_values = np.clip((sdf_slice + sdf_max) / (2.0 * sdf_max), 0.0, 1.0)
        ax_mesh.plot_surface(
            xx,
            yy,
            plane_z,
            facecolors=cmap(face_values),
            alpha=0.58,
            linewidth=0.0,
            antialiased=False,
            shade=False,
        )
        # Overlay the zero-level contour to highlight where the slice cuts the surface.
        ax_mesh.contour(
            xx,
            yy,
            sdf_slice,
            levels=[0.0],
            zdir="z",
            offset=float(z_value),
            colors=["white"],
            linewidths=2.2,
        )
        ax_mesh.contour(
            xx,
            yy,
            sdf_slice,
            levels=[0.0],
            zdir="z",
            offset=float(z_value),
            colors=["black"],
            linewidths=1.3,
        )
        _set_equal_axes(ax_mesh, verts_np)
        ax_mesh.view_init(elev=18, azim=42)
        ax_mesh.set_title("Mesh + sweeping slice plane")
        ax_mesh.set_axis_off()

        ax_sdf.clear()
        ax_sdf.imshow(
            sdf_slice.T,
            origin="lower",
            extent=(xx.min(), xx.max(), yy.min(), yy.max()),
            cmap="coolwarm",
            vmin=-sdf_max,
            vmax=sdf_max,
        )
        ax_sdf.contour(xx, yy, sdf_slice, levels=[0.0], colors=["white"], linewidths=2.0)
        ax_sdf.contour(xx, yy, sdf_slice, levels=[0.0], colors=["black"], linewidths=1.2)
        ax_sdf.set_title(f"SDF slice at z={z_value:+.2f}")
        ax_sdf.set_xticks([])
        ax_sdf.set_yticks([])
        ax_sdf.set_aspect("equal")
        frames.append(_fig_to_rgb(fig))
    _save_gif(frames, args.output_dir / "sdf_slice_sweep.gif", duration_ms=95)
    plt.close(fig)


if __name__ == "__main__":
    main()
