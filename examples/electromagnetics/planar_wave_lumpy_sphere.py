# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import imageio.v2 as imageio
import matplotlib.pyplot as plt
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


def _make_lumpy_sphere_mask(
    nx: int,
    ny: int,
    nz: int,
    device: torch.device,
) -> torch.Tensor:
    x = torch.arange(nx, device=device, dtype=torch.float32)
    y = torch.arange(ny, device=device, dtype=torch.float32)
    z = torch.arange(nz, device=device, dtype=torch.float32)
    xx, yy, zz = torch.meshgrid(x, y, z, indexing="ij")

    cx = 0.62 * (nx - 1)
    cy = 0.50 * (ny - 1)
    cz = 0.50 * (nz - 1)

    dx = xx - cx
    dy = yy - cy
    dz = zz - cz
    r = torch.sqrt(dx * dx + dy * dy + dz * dz + 1.0e-12)

    theta = torch.atan2(torch.sqrt(dy * dy + dz * dz), dx)
    phi = torch.atan2(dz, dy + 1.0e-12)

    base_radius = 0.18 * min(nx, ny, nz)
    perturb = 0.14 * torch.sin(3.0 * theta) * torch.cos(2.0 * phi) + 0.08 * torch.sin(
        5.0 * phi
    )
    local_radius = base_radius * (1.0 + perturb)
    inside = r <= local_radius
    return inside


def _build_pml_layers(
    nx: int,
    ny: int,
    nz: int,
    pml_thickness: int,
    dt: float,
    spacing: torch.Tensor,
    device: torch.device,
) -> list[dict]:
    courant_number = float(dt / float(torch.min(spacing).item()))
    boundaries = [
        ("x-min", (1.0, 0.0, 0.0), (pml_thickness, ny, nz), (0, 0, 0)),
        (
            "x-max",
            (-1.0, 0.0, 0.0),
            (pml_thickness, ny, nz),
            (nx - pml_thickness, 0, 0),
        ),
        ("y-min", (0.0, 1.0, 0.0), (nx, pml_thickness, nz), (0, 0, 0)),
        (
            "y-max",
            (0.0, -1.0, 0.0),
            (nx, pml_thickness, nz),
            (0, ny - pml_thickness, 0),
        ),
        ("z-min", (0.0, 0.0, 1.0), (nx, ny, pml_thickness), (0, 0, 0)),
        (
            "z-max",
            (0.0, 0.0, -1.0),
            (nx, ny, pml_thickness),
            (0, 0, nz - pml_thickness),
        ),
    ]

    pml_layers: list[dict] = []
    for name, direction, shape, offset in boundaries:
        axis = int(np.argmax(np.abs(np.asarray(direction, dtype=np.float32))))
        layer = torch.zeros((36, *shape), device=device, dtype=torch.float32)
        layer = pml_initializer(
            layer,
            direction=direction,
            thickness=int(shape[axis]),
            courant_number=courant_number,
            kappa=1.0,
            a=1.0e-8,
            inplace=True,
        )
        pml_layers.append({"name": name, "layer": layer, "offset": offset})
    return pml_layers


def _save_line_plot(values: np.ndarray, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.2))
    ax.plot(values, color="tab:blue", lw=1.5)
    ax.set_title("Mean Electromagnetic Energy vs Step")
    ax.set_xlabel("Step")
    ax.set_ylabel("mean(E^2 + H^2)")
    ax.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def _save_material_map(material_slice: np.ndarray, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(6.4, 5.4))
    image = ax.imshow(material_slice.T, origin="lower", cmap="viridis")
    ax.set_title("Lumpy Sphere Permittivity Map (z mid-plane)")
    ax.set_xlabel("x index")
    ax.set_ylabel("y index")
    fig.colorbar(image, ax=ax, shrink=0.88, label="relative permittivity")
    fig.tight_layout()
    fig.savefig(output_path, dpi=170)
    plt.close(fig)


def _save_final_fields(
    ey_slice: np.ndarray,
    hz_slice: np.ndarray,
    emag_slice: np.ndarray,
    mask_slice: np.ndarray,
    output_path: Path,
) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(14.0, 4.3))
    panels = [
        ("Ey (z mid-plane)", ey_slice, "RdBu_r"),
        ("Hz (z mid-plane)", hz_slice, "RdBu_r"),
        ("|E| (z mid-plane)", emag_slice, "magma"),
    ]
    for ax, (title, arr, cmap) in zip(axes, panels):
        image = ax.imshow(arr.T, origin="lower", cmap=cmap)
        ax.contour(mask_slice.T, levels=[0.5], colors="cyan", linewidths=0.9)
        ax.set_title(title)
        ax.set_xlabel("x index")
        ax.set_ylabel("y index")
        fig.colorbar(image, ax=ax, shrink=0.82)

    fig.tight_layout()
    fig.savefig(output_path, dpi=170)
    plt.close(fig)


def _frame_to_uint8(
    field_slice: np.ndarray,
    mask_slice: np.ndarray,
    title: str,
    cmap: str,
) -> np.ndarray:
    fig, ax = plt.subplots(figsize=(5.7, 4.8))
    vmax = max(float(np.max(np.abs(field_slice))), 1.0e-6)
    image = ax.imshow(field_slice.T, origin="lower", cmap=cmap, vmin=-vmax, vmax=vmax)
    ax.contour(mask_slice.T, levels=[0.5], colors="yellow", linewidths=0.7)
    ax.set_title(title)
    ax.set_xlabel("x")
    ax.set_ylabel("y")
    fig.colorbar(image, ax=ax, shrink=0.82)
    fig.tight_layout()

    fig.canvas.draw()
    width, height = fig.canvas.get_width_height()
    rgb = np.frombuffer(fig.canvas.buffer_rgba(), dtype=np.uint8).reshape(
        height, width, 4
    )[:, :, :3]
    plt.close(fig)
    return rgb.copy()


def run_demo(
    output_dir: Path,
    n: int,
    pml_thickness: int,
    n_steps: int,
    frame_stride: int,
    seed: int,
) -> dict:
    torch.manual_seed(seed)
    np.random.seed(seed)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    nx = ny = nz = n
    spacing = torch.tensor([1.0, 1.0, 1.0], device=device, dtype=torch.float32)
    dt = 0.35
    source_x = pml_thickness + 2

    lump_mask = _make_lumpy_sphere_mask(nx, ny, nz, device)
    eps = torch.full((nx, ny, nz), 1.0, device=device, dtype=torch.float32)
    eps[lump_mask] = 5.2
    sigma_e = torch.zeros((nx, ny, nz), device=device, dtype=torch.float32)
    sigma_e[lump_mask] = 0.003

    electric = torch.zeros((3, nx, ny, nz), device=device, dtype=torch.float32)
    magnetic = torch.zeros((3, nx, ny, nz), device=device, dtype=torch.float32)

    pml_layers = _build_pml_layers(
        nx=nx,
        ny=ny,
        nz=nz,
        pml_thickness=pml_thickness,
        dt=dt,
        spacing=spacing,
        device=device,
    )

    yy = torch.arange(ny, device=device, dtype=torch.float32).view(ny, 1)
    zz = torch.arange(nz, device=device, dtype=torch.float32).view(1, nz)
    y0 = 0.5 * (ny - 1)
    z0 = 0.5 * (nz - 1)
    w = 0.23 * min(ny, nz)
    source_window = torch.exp(-(((yy - y0) ** 2 + (zz - z0) ** 2) / (w * w)))
    source_current = torch.zeros((3, 1, ny, nz), device=device, dtype=torch.float32)

    wavelength = 14.0
    omega = 2.0 * math.pi / wavelength
    pulse_center = 30.0
    pulse_width = 15.0

    ey_frames: list[np.ndarray] = []
    hz_frames: list[np.ndarray] = []
    energy_trace = np.zeros((n_steps,), dtype=np.float64)

    mask_slice = lump_mask[:, :, nz // 2].detach().cpu().numpy()

    for step in range(n_steps):
        t = step * dt

        # H update + PML magnetic correction.
        magnetic = magnetic_field_update(
            electric,
            magnetic,
            mu=1.0,
            sigma_m=0.0,
            spacing=spacing,
            dt=dt,
            inplace=True,
        )
        for boundary in pml_layers:
            layer = pml_phi_h_update(
                electric,
                boundary["layer"],
                pml_layer_offset=boundary["offset"],
                inplace=True,
            )
            boundary["layer"] = layer
            magnetic = pml_magnetic_field_update(
                magnetic,
                boundary["layer"],
                mu=1.0,
                spacing=spacing,
                pml_layer_offset=boundary["offset"],
                dt=dt,
                inplace=True,
            )

        # Source current pulse (Ey at x = source_x plane).
        source_envelope = math.exp(-0.5 * ((step - pulse_center) / pulse_width) ** 2)
        source_value = 0.45 * source_envelope * math.sin(omega * t)
        source_current.zero_()
        source_current[1, 0] = source_value * source_window

        # E update + PML electric correction.
        electric = electric_field_update(
            electric,
            magnetic,
            eps=eps,
            sigma_e=sigma_e,
            spacing=spacing,
            dt=dt,
            impressed_current=source_current,
            impressed_current_offset=(source_x, 0, 0),
            inplace=True,
        )
        for boundary in pml_layers:
            layer = pml_phi_e_update(
                magnetic,
                boundary["layer"],
                pml_layer_offset=boundary["offset"],
                inplace=True,
            )
            boundary["layer"] = layer
            electric = pml_electric_field_update(
                electric,
                boundary["layer"],
                eps=eps,
                spacing=spacing,
                pml_layer_offset=boundary["offset"],
                dt=dt,
                inplace=True,
            )

        energy = torch.mean(electric * electric + magnetic * magnetic)
        energy_trace[step] = float(energy.item())

        if step % frame_stride == 0:
            ey_slice = electric[1, :, :, nz // 2].detach().cpu().numpy()
            hz_slice = magnetic[2, :, :, nz // 2].detach().cpu().numpy()

            ey_frames.append(
                _frame_to_uint8(
                    ey_slice,
                    mask_slice,
                    title=f"Ey z-mid (step {step})",
                    cmap="RdBu_r",
                )
            )
            hz_frames.append(
                _frame_to_uint8(
                    hz_slice,
                    mask_slice,
                    title=f"Hz z-mid (step {step})",
                    cmap="RdBu_r",
                )
            )

    output_dir.mkdir(parents=True, exist_ok=True)

    imageio.mimsave(output_dir / "ey_z_mid.gif", ey_frames, fps=10, loop=0)
    imageio.mimsave(output_dir / "hz_z_mid.gif", hz_frames, fps=10, loop=0)

    ey_last = electric[1, :, :, nz // 2].detach().cpu().numpy()
    hz_last = magnetic[2, :, :, nz // 2].detach().cpu().numpy()
    emag_last = (
        torch.linalg.vector_norm(electric[:, :, :, nz // 2], dim=0)
        .detach()
        .cpu()
        .numpy()
    )

    _save_material_map(
        eps[:, :, nz // 2].detach().cpu().numpy(),
        output_dir / "material_map.png",
    )
    _save_line_plot(energy_trace, output_dir / "energy_timeseries.png")
    _save_final_fields(
        ey_last,
        hz_last,
        emag_last,
        mask_slice,
        output_dir / "final_fields.png",
    )

    # Save first/last frames as PNGs for easy inspection.
    imageio.imwrite(output_dir / "ey_frame_first.png", ey_frames[0])
    imageio.imwrite(output_dir / "ey_frame_last.png", ey_frames[-1])
    imageio.imwrite(output_dir / "hz_frame_first.png", hz_frames[0])
    imageio.imwrite(output_dir / "hz_frame_last.png", hz_frames[-1])

    summary = {
        "device": str(device),
        "dispatch_mode": "default",
        "grid": [nx, ny, nz],
        "steps": n_steps,
        "frame_stride": frame_stride,
        "pml_thickness": pml_thickness,
        "energy_min": float(np.min(energy_trace)),
        "energy_max": float(np.max(energy_trace)),
        "energy_final": float(energy_trace[-1]),
    }
    with (output_dir / "run_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Planar-wave / lumpy-sphere FDTD demo using PhysicsNeMo EM functionals."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("examples/electromagnetics/output/planar_wave_lumpy_sphere"),
    )
    parser.add_argument("--n", type=int, default=72, help="Grid size (nx=ny=nz=n)")
    parser.add_argument("--pml-thickness", type=int, default=8)
    parser.add_argument("--steps", type=int, default=180)
    parser.add_argument("--frame-stride", type=int, default=3)
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args()

    summary = run_demo(
        output_dir=args.output_dir,
        n=args.n,
        pml_thickness=args.pml_thickness,
        n_steps=args.steps,
        frame_stride=args.frame_stride,
        seed=args.seed,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
