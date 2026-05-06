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

"""Second-order unstructured finite-volume Euler example.

This example intentionally keeps the gas-dynamics pieces local to the example
while using ``physicsnemo.mesh.Mesh`` for the static simplicial mesh. The time
integrator is a MUSCL-Hancock scheme:

1. reconstruct primitive-variable gradients on cells,
2. compute a CFL timestep from Euler wave speeds,
3. run an Euler update that applies Barth-Jespersen limiting, predicts
   primitive variables by a half timestep, reconstructs face states, solves a
   normal Rusanov problem, and updates conserved variables.
"""

from __future__ import annotations

from pathlib import Path

import hydra
import numpy as np
import torch
from omegaconf import DictConfig

try:
    import matplotlib.pyplot as plt
except ImportError:
    plt = None

try:
    import imageio.v2 as imageio
except ImportError:
    imageio = None

from physicsnemo.mesh import Mesh
from physicsnemo.nn.functional import (
    mesh_green_gauss_gradient,
)
from physicsnemo.nn.functional.derivatives.mesh_green_gauss_gradient.utils import (
    build_neighbors,
)

try:
    from euler_finite_volume import (
        euler_cfl_timestep,
        euler_conservative_to_primitive,
        euler_update,
        primitive_to_conservative_torch,
    )
except ModuleNotFoundError:
    from .euler_finite_volume import (
        euler_cfl_timestep,
        euler_conservative_to_primitive,
        euler_update,
        primitive_to_conservative_torch,
    )

INTERIOR = 0
WALL_SLIP = 1
INFLOW_SUPERSONIC = 2
OUTFLOW_ZERO_GRAD = 3


def make_step_mesh_2d(cfg, device: torch.device) -> Mesh:
    """Structured triangular mesh with the forward-facing-step solid removed."""
    nx, ny = int(cfg.nx), int(cfg.ny)
    xs = torch.linspace(0.0, float(cfg.length), nx + 1, device=device)
    ys = torch.linspace(0.0, float(cfg.height), ny + 1, device=device)
    yy, xx = torch.meshgrid(ys, xs, indexing="ij")
    points = torch.stack((xx.reshape(-1), yy.reshape(-1)), dim=1)

    def pid(i: int, j: int) -> int:
        return j * (nx + 1) + i

    cells: list[tuple[int, int, int]] = []
    for j in range(ny):
        for i in range(nx):
            cx = 0.5 * (xs[i] + xs[i + 1])
            cy = 0.5 * (ys[j] + ys[j + 1])
            if cx >= float(cfg.step_x) and cy <= float(cfg.step_height):
                continue
            p00, p10 = pid(i, j), pid(i + 1, j)
            p01, p11 = pid(i, j + 1), pid(i + 1, j + 1)
            cells.append((p00, p10, p11))
            cells.append((p00, p11, p01))
    return Mesh(
        points=points, cells=torch.tensor(cells, dtype=torch.int64, device=device)
    )


def make_step_mesh_3d(cfg, device: torch.device) -> Mesh:
    """Extruded tetrahedral forward-facing-step mesh."""
    nx, ny, nz = int(cfg.nx), int(cfg.ny), int(cfg.nz)
    xs = torch.linspace(0.0, float(cfg.length), nx + 1, device=device)
    ys = torch.linspace(0.0, float(cfg.height), ny + 1, device=device)
    zs = torch.linspace(0.0, float(cfg.depth), nz + 1, device=device)
    zz, yy, xx = torch.meshgrid(zs, ys, xs, indexing="ij")
    points = torch.stack((xx.reshape(-1), yy.reshape(-1), zz.reshape(-1)), dim=1)

    def pid(i: int, j: int, k: int) -> int:
        return k * (ny + 1) * (nx + 1) + j * (nx + 1) + i

    cells: list[tuple[int, int, int, int]] = []
    for k in range(nz):
        for j in range(ny):
            for i in range(nx):
                cx = 0.5 * (xs[i] + xs[i + 1])
                cy = 0.5 * (ys[j] + ys[j + 1])
                if cx >= float(cfg.step_x) and cy <= float(cfg.step_height):
                    continue
                p000 = pid(i, j, k)
                p100 = pid(i + 1, j, k)
                p010 = pid(i, j + 1, k)
                p110 = pid(i + 1, j + 1, k)
                p001 = pid(i, j, k + 1)
                p101 = pid(i + 1, j, k + 1)
                p011 = pid(i, j + 1, k + 1)
                p111 = pid(i + 1, j + 1, k + 1)
                cells.extend(
                    [
                        (p000, p001, p011, p111),
                        (p000, p011, p010, p111),
                        (p000, p010, p110, p111),
                        (p000, p110, p100, p111),
                        (p000, p100, p101, p111),
                        (p000, p101, p001, p111),
                    ]
                )
    return Mesh(
        points=points, cells=torch.tensor(cells, dtype=torch.int64, device=device)
    )


def local_face_centroids(mesh: Mesh) -> torch.Tensor:
    """Compute centroids for each local simplex face opposite a local vertex."""
    cell_points = mesh.points[mesh.cells.to(dtype=torch.int64)]
    n_faces = mesh.cells.shape[1]
    face_centroids = []
    for face_idx in range(n_faces):
        face_local = [idx for idx in range(n_faces) if idx != face_idx]
        face_centroids.append(cell_points[:, face_local].mean(dim=1))
    return torch.stack(face_centroids, dim=1)


def tag_step_boundaries(
    case_cfg,
    mesh: Mesh,
    neighbors: torch.Tensor,
) -> torch.Tensor:
    """Assign Euler boundary tags to local cell faces by face-centroid location."""
    tags = torch.full_like(neighbors, INTERIOR)
    boundary = neighbors < 0
    x = local_face_centroids(mesh)[:, :, 0]
    tol = 1.0e-6
    tags[boundary] = WALL_SLIP
    tags[boundary & (x <= tol)] = INFLOW_SUPERSONIC
    tags[boundary & (x >= float(case_cfg.length) - tol)] = OUTFLOW_ZERO_GRAD
    return tags


def build_case(
    case_cfg,
    device: torch.device,
) -> tuple[Mesh, torch.Tensor, torch.Tensor]:
    """Build mesh connectivity, local cell adjacency, and boundary tags."""
    if int(case_cfg.dimension) == 2:
        mesh = make_step_mesh_2d(case_cfg, device)
    elif int(case_cfg.dimension) == 3:
        mesh = make_step_mesh_3d(case_cfg, device)
    else:
        raise ValueError("case.dimension must be 2 or 3")

    neighbors = build_neighbors(mesh.cells).to(device=device)
    boundary_tags = tag_step_boundaries(case_cfg, mesh, neighbors)
    return mesh, neighbors, boundary_tags


def initial_state(
    solver_cfg,
    mesh: Mesh,
) -> torch.Tensor:
    """Uniform supersonic inflow primitive state over all fluid cells."""
    dims = mesh.n_spatial_dims
    W = torch.zeros(
        (mesh.n_cells, dims + 2),
        device=mesh.points.device,
        dtype=mesh.points.dtype,
    )
    W[:, 0] = float(solver_cfg.inflow_density)
    W[:, 1] = (
        float(solver_cfg.inflow_mach)
        * (float(solver_cfg.gamma) * float(solver_cfg.inflow_pressure) / W[:, 0]) ** 0.5
    )
    W[:, -1] = float(solver_cfg.inflow_pressure)
    return W


def run_case(case_cfg, solver_cfg, output_dir: Path) -> None:
    """Run one configured finite-volume Euler case."""
    device = torch.device(str(solver_cfg.device))
    mesh, cell_neighbors, boundary_tags = build_case(case_cfg, device)
    gamma = float(solver_cfg.gamma)
    density_floor = float(solver_cfg.density_floor)
    pressure_floor = float(solver_cfg.pressure_floor)
    implementation = str(solver_cfg.implementation)
    W0_initial = initial_state(solver_cfg, mesh)
    inflow = W0_initial[0]
    U = primitive_to_conservative_torch(W0_initial, gamma)
    frame_paths: list[Path] = []
    frame_dir = output_dir / f"{case_cfg.name}_frames"
    plot_every = int(getattr(case_cfg, "plot_every", 0))
    time = 0.0
    if plot_every > 0:
        frame_dir.mkdir(parents=True, exist_ok=True)
        W0 = euler_conservative_to_primitive(
            U, gamma, density_floor, pressure_floor, implementation
        )
        path = frame_dir / "frame_00000.png"
        plot_density(
            case_cfg,
            mesh,
            W0,
            path,
            title=f"{case_cfg.name} step 0, t=0.000",
        )
        frame_paths.append(path)

    for step in range(int(case_cfg.steps)):
        W = euler_conservative_to_primitive(
            U, gamma, density_floor, pressure_floor, implementation
        )
        dt = euler_cfl_timestep(
            W,
            mesh.points,
            mesh.cells,
            cell_neighbors,
            boundary_tags,
            inflow,
            gamma,
            float(solver_cfg.cfl),
            density_floor,
            pressure_floor,
            implementation,
        )
        grad_W = mesh_green_gauss_gradient(
            mesh.points,
            mesh.cells,
            cell_neighbors,
            W,
            implementation=implementation,
        )
        U = euler_update(
            U,
            W,
            grad_W,
            mesh.points,
            mesh.cells,
            cell_neighbors,
            boundary_tags,
            inflow,
            dt,
            gamma,
            density_floor,
            pressure_floor,
            implementation,
        )
        time += float(dt)

        if step % int(solver_cfg.log_every) == 0:
            print(
                f"{case_cfg.name}: step={step:04d} t={time:.4f} dt={float(dt):.4e} "
                f"rho=[{float(W[:, 0].min()):.3f}, {float(W[:, 0].max()):.3f}]"
            )
        if plot_every > 0 and (step + 1) % plot_every == 0:
            W_plot = euler_conservative_to_primitive(
                U, gamma, density_floor, pressure_floor, implementation
            )
            path = frame_dir / f"frame_{step + 1:05d}.png"
            plot_density(
                case_cfg,
                mesh,
                W_plot,
                path,
                title=f"{case_cfg.name} step {step + 1}, t={time:.3f}",
            )
            frame_paths.append(path)

    output_dir.mkdir(parents=True, exist_ok=True)
    W = euler_conservative_to_primitive(
        U, gamma, density_floor, pressure_floor, implementation
    )
    torch.save(
        {
            "points": mesh.points.detach().cpu(),
            "cells": mesh.cells.detach().cpu(),
            "primitive": W.detach().cpu(),
            "time": time,
        },
        output_dir / f"{case_cfg.name}.pt",
    )
    plot_density(
        case_cfg,
        mesh,
        W,
        output_dir / f"{case_cfg.name}_density.png",
        title=f"{case_cfg.name} final, t={time:.3f}",
    )
    if bool(getattr(case_cfg, "make_animation", False)) and frame_paths:
        write_animation(frame_paths, output_dir / f"{case_cfg.name}_density.gif")
        write_animation(frame_paths, output_dir / f"{case_cfg.name}_density.mp4")


def plot_density(
    case_cfg,
    mesh: Mesh,
    W: torch.Tensor,
    path: Path,
    title: str | None = None,
) -> None:
    """Save a compact density plot for 2D and a mid-plane slice for 3D."""
    if plt is None:
        print("matplotlib is not installed; skipping density plot output")
        return
    centroids = mesh.cell_centroids.detach().cpu()
    density = W[:, 0].detach().cpu()
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(9, 3.4))
    if int(case_cfg.dimension) == 2:
        mesh_points = mesh.points.detach().cpu()
        coll = plt.tripcolor(
            mesh_points[:, 0],
            mesh_points[:, 1],
            mesh.cells.detach().cpu()[:, :3],
            facecolors=density,
            shading="flat",
            cmap="viridis",
            vmin=float(getattr(case_cfg, "density_vmin", 0.6)),
            vmax=float(getattr(case_cfg, "density_vmax", 6.5)),
        )
        plt.gca().set_aspect("equal")
    else:
        # The 3D mesh is an extruded tetrahedralization. Plotting raw tet
        # centroids produces clustered dots because each hexahedral bin is
        # split into six tetrahedra. Average the tetrahedral cell data back to
        # the underlying x-y bins for a clean z-averaged cross-section.
        field, x_edges, y_edges = _xy_bin_average(case_cfg, centroids, density)
        field = np.ma.masked_invalid(field.numpy())
        coll = plt.pcolormesh(
            x_edges.numpy(),
            y_edges.numpy(),
            field,
            shading="flat",
            cmap="viridis",
            vmin=float(getattr(case_cfg, "density_vmin", 0.6)),
            vmax=float(getattr(case_cfg, "density_vmax", 6.5)),
        )
        plt.gca().set_aspect("equal")
    plt.colorbar(coll, label="density")
    if title is not None:
        plt.title(title)
    plt.xlabel("x")
    plt.ylabel("y")
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()


def _xy_bin_average(case_cfg, centroids: torch.Tensor, values: torch.Tensor):
    """Average 3D tetrahedral cell data into x-y bins for cross-section plots."""
    nx = int(case_cfg.nx)
    ny = int(case_cfg.ny)
    length = float(case_cfg.length)
    height = float(case_cfg.height)

    x_edges = torch.linspace(0.0, length, nx + 1)
    y_edges = torch.linspace(0.0, height, ny + 1)
    x = centroids[:, 0].clamp(0.0, length)
    y = centroids[:, 1].clamp(0.0, height)
    ix = torch.clamp((x / length * nx).floor().to(torch.int64), 0, nx - 1)
    iy = torch.clamp((y / height * ny).floor().to(torch.int64), 0, ny - 1)

    flat = iy * nx + ix
    accum = torch.zeros((ny * nx,), dtype=values.dtype)
    counts = torch.zeros((ny * nx,), dtype=values.dtype)
    accum.index_add_(0, flat, values)
    counts.index_add_(0, flat, torch.ones_like(values))
    field = accum / counts.clamp_min(1.0)
    field[counts == 0] = float("nan")
    return field.reshape(ny, nx), x_edges, y_edges


def write_animation(frame_paths: list[Path], output_path: Path) -> None:
    """Write an animation from saved PNG frames when imageio is installed."""
    if imageio is None:
        print("imageio is not installed; skipping animation output")
        return
    images = [imageio.imread(path) for path in frame_paths if path.exists()]
    if images:
        if output_path.suffix == ".mp4":
            imageio.mimsave(output_path, images, fps=8)
        else:
            imageio.mimsave(output_path, images, duration=0.12)
        print(f"wrote animation: {output_path}")


@hydra.main(version_base="1.3", config_path="conf", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run all finite-volume Euler cases from the Hydra config."""
    output_dir = Path("outputs")
    for case_cfg in cfg.cases:
        run_case(case_cfg, cfg.solver, output_dir)


if __name__ == "__main__":
    main()
