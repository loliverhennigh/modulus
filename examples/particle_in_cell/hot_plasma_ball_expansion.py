# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import csv
import json
import math
import time
from pathlib import Path

import imageio.v2 as imageio
import numpy as np
import torch

from physicsnemo.nn.functional import (
    deposit_current_charge_conserving,
    electric_field_update,
    gather_fields_to_particles,
    magnetic_field_update,
    particle_push_boris,
)

EPS0 = 8.8541878128e-12
MU0 = 1.25663706212e-6
C0 = 299792458.0
QE = 1.602176634e-19
ME = 9.1093837015e-31
MP = 1.67262192369e-27
EV_TO_J = QE


def _as_impl(value: str) -> str | None:
    if value == "auto":
        return None
    return value


def _select_device(device: str) -> torch.device:
    if device == "auto":
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")
    return torch.device(device)


def _wrap_periodic(
    positions: torch.Tensor,
    origin: torch.Tensor,
    box_size: torch.Tensor,
) -> torch.Tensor:
    return origin + torch.remainder(positions - origin, box_size)


def _sample_positions_in_ball(
    num_particles: int,
    center: torch.Tensor,
    radius: float,
    device: torch.device,
    generator: torch.Generator,
) -> torch.Tensor:
    u = torch.rand(num_particles, device=device, dtype=torch.float32, generator=generator)
    cos_theta = 2.0 * torch.rand(
        num_particles, device=device, dtype=torch.float32, generator=generator
    ) - 1.0
    phi = 2.0 * math.pi * torch.rand(
        num_particles, device=device, dtype=torch.float32, generator=generator
    )

    r = radius * torch.pow(u, 1.0 / 3.0)
    sin_theta = torch.sqrt(torch.clamp(1.0 - cos_theta * cos_theta, min=0.0))

    x = r * sin_theta * torch.cos(phi)
    y = r * sin_theta * torch.sin(phi)
    z = r * cos_theta

    return center.unsqueeze(0) + torch.stack((x, y, z), dim=1)


def _sample_maxwellian_velocity(
    num_particles: int,
    temperature_ev: float,
    mass_kg: float,
    device: torch.device,
    generator: torch.Generator,
) -> torch.Tensor:
    thermal_speed = math.sqrt(max(temperature_ev, 0.0) * EV_TO_J / mass_kg)
    velocity = torch.randn(
        num_particles, 3, device=device, dtype=torch.float32, generator=generator
    )
    velocity = thermal_speed * velocity
    velocity = velocity - velocity.mean(dim=0, keepdim=True)
    return velocity


def _momentum_from_velocity(velocity: torch.Tensor) -> torch.Tensor:
    v2 = torch.sum(velocity * velocity, dim=-1, keepdim=True)
    beta2 = torch.clamp(v2 / (C0 * C0), min=0.0, max=1.0 - 1.0e-7)
    gamma = torch.rsqrt(1.0 - beta2)
    return gamma * velocity


def _velocity_from_momentum(momentum: torch.Tensor) -> torch.Tensor:
    gamma = torch.sqrt(1.0 + torch.sum(momentum * momentum, dim=-1, keepdim=True) / (C0 * C0))
    return momentum / gamma


def _deposit_charge_density_cic(
    particle_position: torch.Tensor,
    particle_weight: torch.Tensor,
    particle_charge: float,
    grid_shape: tuple[int, int, int],
    origin: torch.Tensor,
    spacing: torch.Tensor,
    periodic: bool,
) -> torch.Tensor:
    nx, ny, nz = grid_shape
    device = particle_position.device
    dtype = particle_position.dtype

    dx, dy, dz = float(spacing[0]), float(spacing[1]), float(spacing[2])
    cell_volume = dx * dy * dz

    grid = (particle_position - origin.unsqueeze(0)) / spacing.unsqueeze(0)
    if periodic:
        domain = torch.tensor(
            [float(nx), float(ny), float(nz)], device=device, dtype=dtype
        )
        grid = torch.remainder(grid, domain.unsqueeze(0))

    i0 = torch.floor(grid).to(torch.int64)
    frac = grid - i0.to(dtype=dtype)

    wx0 = 1.0 - frac[:, 0]
    wy0 = 1.0 - frac[:, 1]
    wz0 = 1.0 - frac[:, 2]
    wx1 = frac[:, 0]
    wy1 = frac[:, 1]
    wz1 = frac[:, 2]

    charge_density_weight = (particle_charge * particle_weight) / cell_volume
    rho_flat = torch.zeros(nx * ny * nz, device=device, dtype=dtype)

    for ox, wx in ((0, wx0), (1, wx1)):
        ix = i0[:, 0] + ox
        for oy, wy in ((0, wy0), (1, wy1)):
            iy = i0[:, 1] + oy
            for oz, wz in ((0, wz0), (1, wz1)):
                iz = i0[:, 2] + oz

                if periodic:
                    ix_eff = torch.remainder(ix, nx)
                    iy_eff = torch.remainder(iy, ny)
                    iz_eff = torch.remainder(iz, nz)
                else:
                    ix_eff = torch.clamp(ix, min=0, max=nx - 1)
                    iy_eff = torch.clamp(iy, min=0, max=ny - 1)
                    iz_eff = torch.clamp(iz, min=0, max=nz - 1)

                linear = ix_eff * (ny * nz) + iy_eff * nz + iz_eff
                contrib = charge_density_weight * wx * wy * wz
                rho_flat.scatter_add_(0, linear, contrib)

    return rho_flat.view(nx, ny, nz)


def _divergence_current(current_density: torch.Tensor, spacing: tuple[float, float, float]) -> torch.Tensor:
    jx = current_density[0]
    jy = current_density[1]
    jz = current_density[2]
    dx, dy, dz = spacing

    return (
        (jx - torch.roll(jx, shifts=1, dims=0)) / dx
        + (jy - torch.roll(jy, shifts=1, dims=1)) / dy
        + (jz - torch.roll(jz, shifts=1, dims=2)) / dz
    )


def _write_structured_points_vtk(
    path: Path,
    origin: tuple[float, float, float],
    spacing: tuple[float, float, float],
    electric_field: torch.Tensor,
    magnetic_field: torch.Tensor,
    current_density: torch.Tensor,
    charge_density: torch.Tensor,
) -> None:
    electric = electric_field.detach().cpu().numpy()
    magnetic = magnetic_field.detach().cpu().numpy()
    current = current_density.detach().cpu().numpy()
    rho = charge_density.detach().cpu().numpy()

    nx, ny, nz = int(electric.shape[1]), int(electric.shape[2]), int(electric.shape[3])
    n_points = nx * ny * nz

    def _write_vector(name: str, vec: np.ndarray, handle) -> None:
        handle.write(f"VECTORS {name} float\n")
        for k in range(nz):
            for j in range(ny):
                for i in range(nx):
                    handle.write(
                        f"{vec[0, i, j, k]:.8e} {vec[1, i, j, k]:.8e} {vec[2, i, j, k]:.8e}\n"
                    )

    def _write_scalar(name: str, values: np.ndarray, handle) -> None:
        handle.write(f"SCALARS {name} float 1\n")
        handle.write("LOOKUP_TABLE default\n")
        for k in range(nz):
            for j in range(ny):
                for i in range(nx):
                    handle.write(f"{values[i, j, k]:.8e}\n")

    with path.open("w", encoding="utf-8") as handle:
        handle.write("# vtk DataFile Version 3.0\n")
        handle.write("PhysicsNemo PIC hot plasma sphere fields\n")
        handle.write("ASCII\n")
        handle.write("DATASET STRUCTURED_POINTS\n")
        handle.write(f"DIMENSIONS {nx} {ny} {nz}\n")
        handle.write(f"ORIGIN {origin[0]:.8e} {origin[1]:.8e} {origin[2]:.8e}\n")
        handle.write(f"SPACING {spacing[0]:.8e} {spacing[1]:.8e} {spacing[2]:.8e}\n")
        handle.write(f"POINT_DATA {n_points}\n")
        _write_vector("electric_field", electric, handle)
        _write_vector("magnetic_field", magnetic, handle)
        _write_vector("current_density", current, handle)
        _write_scalar("charge_density", rho, handle)


def _write_particles_vtk(
    path: Path,
    electron_position: torch.Tensor,
    proton_position: torch.Tensor,
    electron_velocity: torch.Tensor,
    proton_velocity: torch.Tensor,
    electron_weight: torch.Tensor,
    proton_weight: torch.Tensor,
    particle_max: int,
    generator: torch.Generator,
) -> None:
    pos = torch.cat((electron_position, proton_position), dim=0)
    vel = torch.cat((electron_velocity, proton_velocity), dim=0)
    weight = torch.cat((electron_weight, proton_weight), dim=0)

    species = torch.cat(
        (
            torch.zeros(electron_position.shape[0], device=pos.device, dtype=torch.int32),
            torch.ones(proton_position.shape[0], device=pos.device, dtype=torch.int32),
        ),
        dim=0,
    )
    charge = torch.where(species == 0, -QE, QE).to(dtype=torch.float32)

    num_particles = int(pos.shape[0])
    if particle_max > 0 and num_particles > particle_max:
        perm = torch.randperm(num_particles, generator=generator, device=pos.device)
        keep = perm[:particle_max]
        pos = pos[keep]
        vel = vel[keep]
        weight = weight[keep]
        species = species[keep]
        charge = charge[keep]
        num_particles = particle_max

    pos_np = pos.detach().cpu().numpy()
    vel_np = vel.detach().cpu().numpy()
    species_np = species.detach().cpu().numpy()
    weight_np = weight.detach().cpu().numpy()
    charge_np = charge.detach().cpu().numpy()

    with path.open("w", encoding="utf-8") as handle:
        handle.write("# vtk DataFile Version 3.0\n")
        handle.write("PhysicsNemo PIC hot plasma sphere particles\n")
        handle.write("ASCII\n")
        handle.write("DATASET POLYDATA\n")
        handle.write(f"POINTS {num_particles} float\n")
        for i in range(num_particles):
            handle.write(f"{pos_np[i, 0]:.8e} {pos_np[i, 1]:.8e} {pos_np[i, 2]:.8e}\n")

        handle.write(f"VERTICES {num_particles} {2 * num_particles}\n")
        for i in range(num_particles):
            handle.write(f"1 {i}\n")

        handle.write(f"POINT_DATA {num_particles}\n")
        handle.write("VECTORS velocity float\n")
        for i in range(num_particles):
            handle.write(f"{vel_np[i, 0]:.8e} {vel_np[i, 1]:.8e} {vel_np[i, 2]:.8e}\n")

        handle.write("SCALARS species int 1\n")
        handle.write("LOOKUP_TABLE default\n")
        for i in range(num_particles):
            handle.write(f"{int(species_np[i])}\n")

        handle.write("SCALARS weight float 1\n")
        handle.write("LOOKUP_TABLE default\n")
        for i in range(num_particles):
            handle.write(f"{weight_np[i]:.8e}\n")

        handle.write("SCALARS charge float 1\n")
        handle.write("LOOKUP_TABLE default\n")
        for i in range(num_particles):
            handle.write(f"{charge_np[i]:.8e}\n")


def _save_diagnostic_plot(rows: list[dict], output_path: Path) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return

    steps = np.asarray([row["step"] for row in rows], dtype=np.int32)
    total_energy = np.asarray([row["total_energy"] for row in rows], dtype=np.float64)
    charge_error = np.asarray([row["charge_error_abs"] for row in rows], dtype=np.float64)

    fig, axes = plt.subplots(1, 2, figsize=(12.0, 4.2))

    axes[0].plot(steps, total_energy, color="tab:blue", lw=1.8)
    axes[0].set_title("Total Energy vs Step")
    axes[0].set_xlabel("step")
    axes[0].set_ylabel("J")
    axes[0].grid(alpha=0.25)

    axes[1].plot(steps, charge_error, color="tab:orange", lw=1.8)
    axes[1].set_title("|Q_grid - Q_particles| vs Step")
    axes[1].set_xlabel("step")
    axes[1].set_ylabel("C")
    axes[1].set_yscale("log")
    axes[1].grid(alpha=0.25)

    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def _write_animation(
    output_dir: Path,
    step_ids: list[int],
    time_ids: list[float],
    ey_slices: list[np.ndarray],
    hz_slices: list[np.ndarray],
    rho_slices: list[np.ndarray],
    fps: int,
) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return

    if not ey_slices:
        return

    frames_dir = output_dir / "animation_frames"
    frames_dir.mkdir(parents=True, exist_ok=True)

    ey_scale = max(float(np.max(np.abs(ey))) for ey in ey_slices)
    hz_scale = max(float(np.max(np.abs(hz))) for hz in hz_slices)
    rho_scale = max(float(np.max(np.abs(rho))) for rho in rho_slices)
    ey_scale = max(ey_scale, 1.0e-12)
    hz_scale = max(hz_scale, 1.0e-12)
    rho_scale = max(rho_scale, 1.0e-20)

    frame_paths: list[Path] = []
    for frame_idx, (step, time_s, ey, hz, rho) in enumerate(
        zip(step_ids, time_ids, ey_slices, hz_slices, rho_slices)
    ):
        fig, axes = plt.subplots(1, 3, figsize=(13.8, 4.3))
        fig.suptitle(
            f"Hot Plasma Sphere Expansion | step={step} | t={time_s*1.0e12:.3f} ps",
            fontsize=12,
        )

        panels = (
            ("Ey (z-mid)", ey, "RdBu_r", -ey_scale, ey_scale),
            ("Hz (z-mid)", hz, "RdBu_r", -hz_scale, hz_scale),
            ("rho (z-mid)", rho, "coolwarm", -rho_scale, rho_scale),
        )
        for ax, (title, arr, cmap, vmin, vmax) in zip(axes, panels):
            image = ax.imshow(arr.T, origin="lower", cmap=cmap, vmin=vmin, vmax=vmax)
            ax.set_title(title)
            ax.set_xlabel("x index")
            ax.set_ylabel("y index")
            fig.colorbar(image, ax=ax, shrink=0.82)

        fig.tight_layout()
        frame_path = frames_dir / f"frame_{frame_idx:05d}.png"
        fig.savefig(frame_path, dpi=130)
        plt.close(fig)
        frame_paths.append(frame_path)

    gif_path = output_dir / "hot_plasma_ball.gif"
    frames = [imageio.imread(path) for path in frame_paths]
    imageio.mimsave(gif_path, frames, fps=max(int(fps), 1), loop=0)


def run_simulation(args: argparse.Namespace) -> dict:
    torch.manual_seed(args.seed)
    np.random.seed(args.seed)

    device = _select_device(args.device)
    implementation = _as_impl(args.implementation)

    grid_n = int(args.grid_n)
    if grid_n < 8:
        raise ValueError("grid_n must be >= 8")

    box_size_m = float(args.box_size_um) * 1.0e-6
    radius_m = float(args.radius_um) * 1.0e-6
    if radius_m <= 0.0:
        raise ValueError("radius_um must be positive")
    if 2.0 * radius_m > box_size_m:
        raise ValueError("sphere diameter cannot exceed periodic box size")

    dx = box_size_m / float(grid_n)
    spacing = (dx, dx, dx)
    spacing_tensor = torch.tensor(spacing, device=device, dtype=torch.float32)
    origin_tensor = torch.tensor((0.0, 0.0, 0.0), device=device, dtype=torch.float32)
    box_tensor = torch.tensor((box_size_m, box_size_m, box_size_m), device=device, dtype=torch.float32)
    center_tensor = origin_tensor + 0.5 * box_tensor

    electron_mass = float(args.electron_mass_scale) * ME
    proton_mass = MP

    skin_depth_target = float(args.skin_cells_target) * dx
    density_max = EPS0 * electron_mass * (C0 / skin_depth_target) ** 2 / (QE * QE)
    density = float(args.density_fraction) * density_max

    omega_pe = math.sqrt(density * QE * QE / (EPS0 * electron_mass))
    skin_depth = C0 / omega_pe
    resolved_skin_cells = skin_depth / dx

    inv_cfl = C0 * math.sqrt(3.0) / dx
    dt_cfl = float(args.cfl) / inv_cfl
    dt_plasma = float(args.omega_pe_dt_max) / omega_pe
    dt = min(dt_cfl, dt_plasma)

    sphere_volume = (4.0 / 3.0) * math.pi * (radius_m**3)
    cell_volume = dx**3
    estimated_ball_cells = sphere_volume / cell_volume
    num_particles_species = max(
        int(args.min_particles_species),
        int(round(float(args.particles_per_cell) * estimated_ball_cells)),
    )
    particle_weight_value = density * sphere_volume / float(num_particles_species)

    generator = torch.Generator(device=device)
    generator.manual_seed(args.seed + 11)

    electron_position = _sample_positions_in_ball(
        num_particles_species, center_tensor, radius_m, device, generator
    )
    proton_position = _sample_positions_in_ball(
        num_particles_species, center_tensor, radius_m, device, generator
    )
    electron_velocity = _sample_maxwellian_velocity(
        num_particles_species,
        float(args.electron_temperature_ev),
        electron_mass,
        device,
        generator,
    )
    proton_velocity = _sample_maxwellian_velocity(
        num_particles_species,
        float(args.proton_temperature_ev),
        proton_mass,
        device,
        generator,
    )
    electron_momentum = _momentum_from_velocity(electron_velocity)
    proton_momentum = _momentum_from_velocity(proton_velocity)

    electron_weight = torch.full(
        (num_particles_species,),
        particle_weight_value,
        device=device,
        dtype=torch.float32,
    )
    proton_weight = torch.full(
        (num_particles_species,),
        particle_weight_value,
        device=device,
        dtype=torch.float32,
    )

    electric_field = torch.zeros(
        (3, grid_n, grid_n, grid_n), device=device, dtype=torch.float32
    )
    magnetic_field = torch.zeros_like(electric_field)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    field_dir = output_dir / "vtk_fields"
    particle_dir = output_dir / "vtk_particles"
    field_dir.mkdir(parents=True, exist_ok=True)
    particle_dir.mkdir(parents=True, exist_ok=True)

    diagnostics_rows: list[dict] = []
    rho_prev: torch.Tensor | None = None
    t_start = time.perf_counter()
    animation_step_ids: list[int] = []
    animation_time_ids: list[float] = []
    animation_ey_slices: list[np.ndarray] = []
    animation_hz_slices: list[np.ndarray] = []
    animation_rho_slices: list[np.ndarray] = []

    for step in range(int(args.steps)):
        step_start = time.perf_counter()

        electron_pos_old = electron_position
        proton_pos_old = proton_position

        electric_e, magnetic_e = gather_fields_to_particles(
            electron_pos_old,
            electric_field,
            magnetic_field,
            origin=(0.0, 0.0, 0.0),
            spacing=spacing,
            periodic=True,
            implementation=implementation,
        )
        electric_p, magnetic_p = gather_fields_to_particles(
            proton_pos_old,
            electric_field,
            magnetic_field,
            origin=(0.0, 0.0, 0.0),
            spacing=spacing,
            periodic=True,
            implementation=implementation,
        )

        electron_position, electron_momentum = particle_push_boris(
            electron_pos_old,
            electron_momentum,
            electric_e,
            magnetic_e,
            charge_to_mass=-QE / electron_mass,
            dt=dt,
            inplace=False,
            implementation=implementation,
        )
        proton_position, proton_momentum = particle_push_boris(
            proton_pos_old,
            proton_momentum,
            electric_p,
            magnetic_p,
            charge_to_mass=QE / proton_mass,
            dt=dt,
            inplace=False,
            implementation=implementation,
        )

        electron_position = _wrap_periodic(electron_position, origin_tensor, box_tensor)
        proton_position = _wrap_periodic(proton_position, origin_tensor, box_tensor)

        current_density = deposit_current_charge_conserving(
            electron_pos_old,
            electron_position,
            electron_weight,
            particle_charge=-QE,
            dt=dt,
            grid_shape=(grid_n, grid_n, grid_n),
            origin=(0.0, 0.0, 0.0),
            spacing=spacing,
            periodic=True,
            implementation=implementation,
            current_density=None,
        )
        current_density = deposit_current_charge_conserving(
            proton_pos_old,
            proton_position,
            proton_weight,
            particle_charge=QE,
            dt=dt,
            grid_shape=(grid_n, grid_n, grid_n),
            origin=(0.0, 0.0, 0.0),
            spacing=spacing,
            periodic=True,
            implementation=implementation,
            current_density=current_density,
        )

        magnetic_field = magnetic_field_update(
            electric_field,
            magnetic_field,
            mu=MU0,
            sigma_m=0.0,
            spacing=spacing,
            dt=dt,
            inplace=True,
            implementation=implementation,
        )
        electric_field = electric_field_update(
            electric_field,
            magnetic_field,
            eps=EPS0,
            sigma_e=0.0,
            spacing=spacing,
            dt=dt,
            impressed_current=current_density,
            impressed_current_offset=(0, 0, 0),
            inplace=True,
            implementation=implementation,
        )

        rho = _deposit_charge_density_cic(
            electron_position,
            electron_weight,
            -QE,
            (grid_n, grid_n, grid_n),
            origin_tensor,
            spacing_tensor,
            periodic=True,
        )
        rho += _deposit_charge_density_cic(
            proton_position,
            proton_weight,
            QE,
            (grid_n, grid_n, grid_n),
            origin_tensor,
            spacing_tensor,
            periodic=True,
        )

        div_j = _divergence_current(current_density, spacing)
        continuity_rms = float("nan")
        continuity_rel = float("nan")
        if rho_prev is not None:
            continuity = (rho - rho_prev) / dt + div_j
            continuity_rms = float(torch.sqrt(torch.mean(continuity * continuity)).item())
            rho_dt_rms = float(
                torch.sqrt(torch.mean(((rho - rho_prev) / dt) ** 2)).item()
            )
            div_j_rms = float(torch.sqrt(torch.mean(div_j * div_j)).item())
            continuity_rel = continuity_rms / max(rho_dt_rms + div_j_rms, 1.0e-30)
        rho_prev = rho

        particle_charge_total = float(
            (-QE * electron_weight.sum() + QE * proton_weight.sum()).item()
        )
        particle_abs_charge_total = float(
            (QE * (electron_weight.sum() + proton_weight.sum())).item()
        )
        grid_charge_total = float((rho.sum() * cell_volume).item())
        charge_error_abs = abs(grid_charge_total - particle_charge_total)
        charge_error_rel_abs_charge = charge_error_abs / max(
            particle_abs_charge_total, 1.0e-30
        )

        electron_velocity = _velocity_from_momentum(electron_momentum)
        proton_velocity = _velocity_from_momentum(proton_momentum)
        kinetic_e = float((0.5 * electron_mass * electron_weight * (electron_velocity**2).sum(dim=1)).sum().item())
        kinetic_p = float((0.5 * proton_mass * proton_weight * (proton_velocity**2).sum(dim=1)).sum().item())
        electric_energy = float(
            (0.5 * EPS0 * (electric_field * electric_field).sum() * cell_volume).item()
        )
        magnetic_energy = float(
            (0.5 * MU0 * (magnetic_field * magnetic_field).sum() * cell_volume).item()
        )
        total_energy = kinetic_e + kinetic_p + electric_energy + magnetic_energy

        step_wall_s = time.perf_counter() - step_start
        diagnostics_rows.append(
            {
                "step": step,
                "time_s": step * dt,
                "step_wall_s": step_wall_s,
                "charge_particles_C": particle_charge_total,
                "charge_grid_C": grid_charge_total,
                "charge_error_abs": charge_error_abs,
                "charge_error_rel_abs_charge": charge_error_rel_abs_charge,
                "continuity_rms": continuity_rms,
                "continuity_rel": continuity_rel,
                "kinetic_e_J": kinetic_e,
                "kinetic_p_J": kinetic_p,
                "electric_field_energy_J": electric_energy,
                "magnetic_field_energy_J": magnetic_energy,
                "total_energy": total_energy,
            }
        )

        should_dump = (
            step % int(args.vtk_stride) == 0 or step == int(args.steps) - 1
        )
        should_capture_frame = (
            step % int(args.frame_stride) == 0 or step == int(args.steps) - 1
        )
        if should_capture_frame:
            z_mid = grid_n // 2
            animation_step_ids.append(step)
            animation_time_ids.append(step * dt)
            animation_ey_slices.append(
                electric_field[1, :, :, z_mid].detach().cpu().numpy()
            )
            animation_hz_slices.append(
                magnetic_field[2, :, :, z_mid].detach().cpu().numpy()
            )
            animation_rho_slices.append(rho[:, :, z_mid].detach().cpu().numpy())

        if should_dump:
            _write_structured_points_vtk(
                field_dir / f"fields_step_{step:06d}.vtk",
                origin=(0.0, 0.0, 0.0),
                spacing=spacing,
                electric_field=electric_field,
                magnetic_field=magnetic_field,
                current_density=current_density,
                charge_density=rho,
            )
            _write_particles_vtk(
                particle_dir / f"particles_step_{step:06d}.vtk",
                electron_position=electron_position,
                proton_position=proton_position,
                electron_velocity=electron_velocity,
                proton_velocity=proton_velocity,
                electron_weight=electron_weight,
                proton_weight=proton_weight,
                particle_max=int(args.vtk_particle_max),
                generator=generator,
            )

        if step % int(args.log_stride) == 0 or step == int(args.steps) - 1:
            print(
                f"step={step:5d} "
                f"Qerr={charge_error_abs:.3e} C "
                f"Etot={total_energy:.3e} J "
                f"cont_rel={continuity_rel:.3e} "
                f"wall={step_wall_s:.3f}s"
            )

    total_wall_s = time.perf_counter() - t_start

    csv_path = output_dir / "diagnostics.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(diagnostics_rows[0].keys()))
        writer.writeheader()
        writer.writerows(diagnostics_rows)

    _save_diagnostic_plot(diagnostics_rows, output_dir / "diagnostics.png")

    total_energy_initial = diagnostics_rows[0]["total_energy"]
    total_energy_final = diagnostics_rows[-1]["total_energy"]
    energy_rel_drift = (
        (total_energy_final - total_energy_initial) / total_energy_initial
        if total_energy_initial != 0.0
        else float("nan")
    )

    summary = {
        "device": str(device),
        "implementation": "auto" if implementation is None else implementation,
        "grid_n": grid_n,
        "box_size_um": args.box_size_um,
        "sphere_radius_um": args.radius_um,
        "time_step_s": dt,
        "num_steps": int(args.steps),
        "electron_mass_scale": args.electron_mass_scale,
        "electron_temperature_ev": args.electron_temperature_ev,
        "proton_temperature_ev": args.proton_temperature_ev,
        "num_particles_species": num_particles_species,
        "num_particles_total": 2 * num_particles_species,
        "macro_weight_per_particle": particle_weight_value,
        "density_m3": density,
        "density_max_for_skin_target_m3": density_max,
        "skin_depth_m": skin_depth,
        "skin_depth_cells": resolved_skin_cells,
        "skin_target_cells": args.skin_cells_target,
        "omega_pe_rad_s": omega_pe,
        "omega_pe_dt": omega_pe * dt,
        "dt_cfl_s": dt_cfl,
        "dt_plasma_s": dt_plasma,
        "charge_error_abs_max_C": max(row["charge_error_abs"] for row in diagnostics_rows),
        "charge_error_abs_last_C": diagnostics_rows[-1]["charge_error_abs"],
        "charge_error_rel_abs_charge_max": max(
            row["charge_error_rel_abs_charge"] for row in diagnostics_rows
        ),
        "charge_error_rel_abs_charge_last": diagnostics_rows[-1][
            "charge_error_rel_abs_charge"
        ],
        "continuity_rms_last": diagnostics_rows[-1]["continuity_rms"],
        "continuity_rel_last": diagnostics_rows[-1]["continuity_rel"],
        "total_energy_initial_J": total_energy_initial,
        "total_energy_final_J": total_energy_final,
        "total_energy_relative_drift": energy_rel_drift,
        "runtime_total_s": total_wall_s,
        "runtime_step_mean_s": float(
            np.mean([row["step_wall_s"] for row in diagnostics_rows])
        ),
        "output_dir": str(output_dir),
    }

    with (output_dir / "run_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    if not bool(args.skip_animation):
        _write_animation(
            output_dir=output_dir,
            step_ids=animation_step_ids,
            time_ids=animation_time_ids,
            ey_slices=animation_ey_slices,
            hz_slices=animation_hz_slices,
            rho_slices=animation_rho_slices,
            fps=int(args.animation_fps),
        )

    print("\nRun summary:")
    print(json.dumps(summary, indent=2))
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hot plasma sphere expansion PIC demo (periodic box)."
    )
    parser.add_argument("--output-dir", type=str, default="examples/particle_in_cell/output_hot_plasma_ball")
    parser.add_argument("--device", type=str, default="auto", help="auto, cpu, cuda, cuda:0, ...")
    parser.add_argument("--implementation", type=str, default="auto", choices=("auto", "torch", "warp"))
    parser.add_argument("--seed", type=int, default=20264)

    parser.add_argument("--grid-n", type=int, default=128, help="Uniform grid resolution per axis.")
    parser.add_argument("--steps", type=int, default=120)
    parser.add_argument("--vtk-stride", type=int, default=10)
    parser.add_argument(
        "--frame-stride",
        type=int,
        default=2,
        help="Stride for animation frame capture.",
    )
    parser.add_argument(
        "--animation-fps",
        type=int,
        default=12,
        help="Frames-per-second for GIF animation.",
    )
    parser.add_argument(
        "--skip-animation",
        action="store_true",
        help="Skip writing the animation GIF and frame PNGs.",
    )
    parser.add_argument("--log-stride", type=int, default=5)
    parser.add_argument(
        "--vtk-particle-max",
        type=int,
        default=400000,
        help="Max particles written per particle VTK dump; <=0 writes all.",
    )

    parser.add_argument("--box-size-um", type=float, default=200.0)
    parser.add_argument(
        "--radius-um",
        type=float,
        default=50.0,
        help="Sphere radius in microns (50 um = 100 um diameter).",
    )

    parser.add_argument("--electron-mass-scale", type=float, default=100.0)
    parser.add_argument("--electron-temperature-ev", type=float, default=3000.0)
    parser.add_argument("--proton-temperature-ev", type=float, default=1000.0)
    parser.add_argument("--particles-per-cell", type=float, default=2.0)
    parser.add_argument("--min-particles-species", type=int, default=50000)

    parser.add_argument(
        "--skin-cells-target",
        type=float,
        default=2.5,
        help="Requested electron skin depth in grid-cell units.",
    )
    parser.add_argument(
        "--density-fraction",
        type=float,
        default=0.95,
        help="Fraction of max density allowed by skin-depth target.",
    )
    parser.add_argument("--cfl", type=float, default=0.95)
    parser.add_argument(
        "--omega-pe-dt-max",
        type=float,
        default=0.2,
        help="Upper bound for omega_pe * dt.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run_simulation(parse_args())
