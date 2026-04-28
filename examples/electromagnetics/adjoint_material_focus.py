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
from torch.utils.checkpoint import checkpoint

from physicsnemo.nn.functional import (
    electric_field_update,
    magnetic_field_update,
    pml_electric_field_update,
    pml_initializer,
    pml_magnetic_field_update,
    pml_phi_e_update,
    pml_phi_h_update,
)


def _build_source_window(ny: int, nz: int, device: torch.device) -> torch.Tensor:
    yy = torch.arange(ny, device=device, dtype=torch.float32).view(ny, 1)
    zz = torch.arange(nz, device=device, dtype=torch.float32).view(1, nz)
    y0 = 0.5 * (ny - 1)
    z0 = 0.5 * (nz - 1)
    width = 0.22 * min(ny, nz)
    return torch.exp(-(((yy - y0) ** 2 + (zz - z0) ** 2) / (width * width)))


def _build_target_window(
    ny: int,
    nz: int,
    device: torch.device,
    y_shift: float,
    z_shift: float,
    width_scale: float = 0.18,
) -> torch.Tensor:
    yy = torch.arange(ny, device=device, dtype=torch.float32).view(ny, 1)
    zz = torch.arange(nz, device=device, dtype=torch.float32).view(1, nz)
    y0 = 0.5 * (ny - 1) + y_shift
    z0 = 0.5 * (nz - 1) + z_shift
    width = width_scale * min(ny, nz)
    window = torch.exp(-(((yy - y0) ** 2 + (zz - z0) ** 2) / (width * width)))
    return window / torch.clamp(window.sum(), min=1.0e-12)


def _build_background_window(focus_window: torch.Tensor) -> torch.Tensor:
    focus_norm = focus_window / torch.clamp(torch.max(focus_window), min=1.0e-12)
    background = torch.clamp(1.0 - focus_norm, min=0.0)
    return background / torch.clamp(background.sum(), min=1.0e-12)


def _source_value(step: int, dt: float, wavelength: float, amplitude: float) -> float:
    time = step * dt
    omega = 2.0 * math.pi / wavelength
    pulse_center = 0.35 * (1.0 / dt) * wavelength
    pulse_width = 0.25 * (1.0 / dt) * wavelength
    envelope = math.exp(-0.5 * ((step - pulse_center) / pulse_width) ** 2)
    return amplitude * envelope * math.sin(omega * time)


def _build_base_pml_layers(
    nx: int,
    ny: int,
    nz: int,
    pml_thickness: int,
    dt: float,
    spacing: torch.Tensor,
    device: torch.device,
    implementation: str,
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

    base_layers: list[dict] = []
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
            implementation=implementation,
            inplace=True,
        )
        base_layers.append({"name": name, "offset": offset, "layer": layer})
    return base_layers


def _render_field_frame(
    field_slice: np.ndarray,
    design_slice: np.ndarray,
    title: str,
    cmap: str = "RdBu_r",
    vmin: float | None = None,
    vmax: float | None = None,
    symmetric: bool = True,
    contour_color: str = "yellow",
) -> np.ndarray:
    fig, ax = plt.subplots(figsize=(5.8, 4.8))
    if vmin is None or vmax is None:
        if symmetric:
            vmax_local = max(float(np.max(np.abs(field_slice))), 1.0e-7)
            vmin_local = -vmax_local
        else:
            vmin_local = float(np.min(field_slice))
            vmax_local = float(np.max(field_slice))
            if abs(vmax_local - vmin_local) < 1.0e-12:
                vmax_local = vmin_local + 1.0e-12
    else:
        vmin_local = float(vmin)
        vmax_local = float(vmax)
    image = ax.imshow(
        field_slice.T,
        origin="lower",
        cmap=cmap,
        vmin=vmin_local,
        vmax=vmax_local,
    )
    ax.contour(design_slice.T, levels=[0.5], colors=contour_color, linewidths=0.8)
    ax.set_title(title)
    ax.set_xlabel("x")
    ax.set_ylabel("y")
    fig.colorbar(image, ax=ax, shrink=0.82)
    fig.tight_layout()

    fig.canvas.draw()
    width, height = fig.canvas.get_width_height()
    frame = np.frombuffer(fig.canvas.buffer_rgba(), dtype=np.uint8).reshape(
        height, width, 4
    )[:, :, :3]
    plt.close(fig)
    return frame.copy()


def _render_wave_comparison_frame(
    initial_slice: np.ndarray,
    optimized_slice: np.ndarray,
    design_slice: np.ndarray,
    title: str,
    vlim: float,
) -> np.ndarray:
    fig, axes = plt.subplots(1, 3, figsize=(13.6, 4.2))
    delta_slice = optimized_slice - initial_slice
    delta_lim = max(float(np.max(np.abs(delta_slice))), 1.0e-8)

    panels = [
        ("Initial Ey", initial_slice, -vlim, vlim),
        ("Optimized Ey", optimized_slice, -vlim, vlim),
        ("Delta (Opt - Init)", delta_slice, -delta_lim, delta_lim),
    ]
    for ax, (panel_title, arr, panel_vmin, panel_vmax) in zip(axes, panels):
        image = ax.imshow(
            arr.T,
            origin="lower",
            cmap="RdBu_r",
            vmin=panel_vmin,
            vmax=panel_vmax,
        )
        ax.contour(design_slice.T, levels=[0.5], colors="cyan", linewidths=0.7)
        ax.set_title(panel_title)
        ax.set_xlabel("x")
        ax.set_ylabel("y")
        fig.colorbar(image, ax=ax, shrink=0.82)

    fig.suptitle(title)
    fig.tight_layout()
    fig.canvas.draw()
    width, height = fig.canvas.get_width_height()
    frame = np.frombuffer(fig.canvas.buffer_rgba(), dtype=np.uint8).reshape(
        height, width, 4
    )[:, :, :3]
    plt.close(fig)
    return frame.copy()


def _save_material_panel(
    eps_initial: np.ndarray,
    eps_final: np.ndarray,
    output_path: Path,
) -> None:
    vmin = min(float(eps_initial.min()), float(eps_final.min()))
    vmax = max(float(eps_initial.max()), float(eps_final.max()))

    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.4))
    image0 = axes[0].imshow(eps_initial.T, origin="lower", vmin=vmin, vmax=vmax)
    axes[0].set_title("Initial Permittivity (z mid-plane)")
    axes[0].set_xlabel("x index")
    axes[0].set_ylabel("y index")

    image1 = axes[1].imshow(eps_final.T, origin="lower", vmin=vmin, vmax=vmax)
    axes[1].set_title("Optimized Permittivity (z mid-plane)")
    axes[1].set_xlabel("x index")
    axes[1].set_ylabel("y index")

    fig.colorbar(image1, ax=axes, shrink=0.82, label="relative permittivity")
    _ = image0
    fig.subplots_adjust(left=0.07, right=0.91, bottom=0.11, top=0.90, wspace=0.24)
    fig.savefig(output_path, dpi=170)
    plt.close(fig)


def _save_objective_plot(
    objective_history: list[float],
    signal_history: list[float],
    output_path: Path,
) -> None:
    steps = np.arange(len(objective_history), dtype=np.int64)
    fig, axes = plt.subplots(1, 2, figsize=(11.6, 4.2))

    axes[0].plot(steps, objective_history, color="tab:blue", lw=1.7)
    axes[0].set_title("Objective vs Iteration")
    axes[0].set_xlabel("Iteration")
    axes[0].set_ylabel("loss")
    axes[0].grid(alpha=0.25)

    axes[1].plot(steps, signal_history, color="tab:orange", lw=1.7)
    axes[1].set_title("Composite Signal vs Iteration")
    axes[1].set_xlabel("Iteration")
    axes[1].set_ylabel("signal")
    axes[1].grid(alpha=0.25)

    fig.tight_layout()
    fig.savefig(output_path, dpi=170)
    plt.close(fig)


def _save_forward_comparison(
    ey_initial: np.ndarray,
    ey_final: np.ndarray,
    hz_initial: np.ndarray,
    hz_final: np.ndarray,
    design_slice: np.ndarray,
    output_path: Path,
) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(10.8, 8.2))
    panels = [
        ("Initial Ey (z mid-plane)", ey_initial, "RdBu_r"),
        ("Final Ey (z mid-plane)", ey_final, "RdBu_r"),
        ("Initial Hz (z mid-plane)", hz_initial, "RdBu_r"),
        ("Final Hz (z mid-plane)", hz_final, "RdBu_r"),
    ]

    for ax, (title, arr, cmap) in zip(axes.flatten(), panels):
        vmax = max(float(np.max(np.abs(arr))), 1.0e-7)
        image = ax.imshow(arr.T, origin="lower", cmap=cmap, vmin=-vmax, vmax=vmax)
        ax.contour(design_slice.T, levels=[0.5], colors="cyan", linewidths=0.7)
        ax.set_title(title)
        ax.set_xlabel("x index")
        ax.set_ylabel("y index")
        fig.colorbar(image, ax=ax, shrink=0.82)

    fig.tight_layout()
    fig.savefig(output_path, dpi=170)
    plt.close(fig)


def _simulate_forward(
    eps: torch.Tensor,
    sigma_e: torch.Tensor,
    base_pml_layers: list[dict],
    spacing: torch.Tensor,
    dt: float,
    source_x: int,
    source_window: torch.Tensor,
    target_x: int,
    focus_window: torch.Tensor,
    suppression_window: torch.Tensor,
    suppression_weight: float,
    source_amplitude: float,
    n_steps: int,
    objective_start: int,
    record_frames: bool,
    frame_stride: int,
    implementation: str,
    checkpoint_segments: int = 0,
) -> dict:
    device = eps.device
    nx, ny, nz = tuple(int(v) for v in eps.shape)
    electric = torch.zeros((3, nx, ny, nz), device=device, dtype=torch.float32)
    magnetic = torch.zeros((3, nx, ny, nz), device=device, dtype=torch.float32)

    layer_offsets = [entry["offset"] for entry in base_pml_layers]
    layer_names = [entry["name"] for entry in base_pml_layers]
    layer_tensors = tuple(entry["layer"].clone() for entry in base_pml_layers)
    source_offset = (source_x, 0, 0)

    objective_trace = np.zeros((n_steps,), dtype=np.float64)
    objective_sum = electric.new_zeros(())
    objective_count = max(int(n_steps - objective_start), 1)
    ey_frames: list[np.ndarray] = []
    hz_frames: list[np.ndarray] = []

    def _single_step(
        step: int,
        electric_in: torch.Tensor,
        magnetic_in: torch.Tensor,
        layers_in: tuple[torch.Tensor, ...],
    ) -> tuple[torch.Tensor, torch.Tensor, tuple[torch.Tensor, ...], torch.Tensor]:
        magnetic_after_main = magnetic_field_update(
            electric_in,
            magnetic_in,
            mu=1.0,
            sigma_m=0.0,
            spacing=spacing,
            dt=dt,
            implementation=implementation,
            inplace=False,
        )

        magnetic_curr = magnetic_after_main
        layers_after_h: list[torch.Tensor] = []
        for offset, layer_before in zip(layer_offsets, layers_in):
            layer_phi_h = pml_phi_h_update(
                electric_in,
                layer_before,
                pml_layer_offset=offset,
                implementation=implementation,
                inplace=False,
            )
            magnetic_curr = pml_magnetic_field_update(
                magnetic_curr,
                layer_phi_h,
                mu=1.0,
                spacing=spacing,
                pml_layer_offset=offset,
                dt=dt,
                implementation=implementation,
                inplace=False,
            )
            layers_after_h.append(layer_phi_h)
        magnetic_after_pml = magnetic_curr

        source_current = torch.zeros((3, 1, ny, nz), device=device, dtype=torch.float32)
        source_current[1, 0] = (
            _source_value(
                step,
                dt,
                wavelength=12.0,
                amplitude=source_amplitude,
            )
            * source_window
        )

        electric_after_main = electric_field_update(
            electric_in,
            magnetic_after_pml,
            eps=eps,
            sigma_e=sigma_e,
            spacing=spacing,
            dt=dt,
            impressed_current=source_current,
            impressed_current_offset=source_offset,
            implementation=implementation,
            inplace=False,
        )

        electric_curr = electric_after_main
        layers_after_e: list[torch.Tensor] = []
        for offset, layer_before in zip(layer_offsets, layers_after_h):
            layer_phi_e = pml_phi_e_update(
                magnetic_after_pml,
                layer_before,
                pml_layer_offset=offset,
                implementation=implementation,
                inplace=False,
            )
            electric_curr = pml_electric_field_update(
                electric_curr,
                layer_phi_e,
                eps=eps,
                spacing=spacing,
                pml_layer_offset=offset,
                dt=dt,
                implementation=implementation,
                inplace=False,
            )
            layers_after_e.append(layer_phi_e)

        ey_target_plane = electric_curr[1, target_x]
        ey_sq = ey_target_plane * ey_target_plane
        focus_energy = torch.sum(ey_sq * focus_window)
        suppression_energy = torch.sum(ey_sq * suppression_window)
        step_signal = focus_energy - suppression_weight * suppression_energy
        return electric_curr, magnetic_after_pml, tuple(layers_after_e), step_signal

    use_checkpoint = checkpoint_segments > 0 and not record_frames
    if use_checkpoint:
        segment_len = max(1, int(math.ceil(float(n_steps) / float(checkpoint_segments))))
        for seg_start in range(0, n_steps, segment_len):
            seg_end = min(n_steps, seg_start + segment_len)

            def _segment_fn(
                electric_seg: torch.Tensor,
                magnetic_seg: torch.Tensor,
                *layers_seg: torch.Tensor,
                _seg_start: int = seg_start,
                _seg_end: int = seg_end,
            ) -> tuple[torch.Tensor, ...]:
                electric_curr = electric_seg
                magnetic_curr = magnetic_seg
                layers_curr: tuple[torch.Tensor, ...] = tuple(layers_seg)
                signal_sum = electric_seg.new_zeros(())
                for step in range(_seg_start, _seg_end):
                    electric_curr, magnetic_curr, layers_curr, step_signal = _single_step(
                        step=step,
                        electric_in=electric_curr,
                        magnetic_in=magnetic_curr,
                        layers_in=layers_curr,
                    )
                    if step >= objective_start:
                        signal_sum = signal_sum + step_signal
                return (electric_curr, magnetic_curr, *layers_curr, signal_sum)

            segment_out = checkpoint(
                _segment_fn,
                electric,
                magnetic,
                *layer_tensors,
                use_reentrant=False,
            )
            electric = segment_out[0]
            magnetic = segment_out[1]
            layer_tensors = tuple(segment_out[2:-1])
            objective_sum = objective_sum + segment_out[-1]
    else:
        for step in range(n_steps):
            electric, magnetic, layer_tensors, step_signal = _single_step(
                step=step,
                electric_in=electric,
                magnetic_in=magnetic,
                layers_in=layer_tensors,
            )
            objective_trace[step] = float(step_signal.detach().item())
            if step >= objective_start:
                objective_sum = objective_sum + step_signal

            if record_frames and step % max(frame_stride, 1) == 0:
                z_mid = nz // 2
                ey_frames.append(electric[1, :, :, z_mid].detach().cpu().numpy())
                hz_frames.append(magnetic[2, :, :, z_mid].detach().cpu().numpy())

    signal_tensor = objective_sum / float(objective_count)
    return {
        "electric": electric,
        "magnetic": magnetic,
        "layers": [
            {
                "name": name,
                "offset": offset,
                "layer": layer,
            }
            for name, offset, layer in zip(layer_names, layer_offsets, layer_tensors)
        ],
        "objective_trace": objective_trace,
        "signal_tensor": signal_tensor,
        "ey_frames": ey_frames,
        "hz_frames": hz_frames,
        "source_offset": source_offset,
    }


def run_adjoint_optimization(
    output_dir: Path,
    n: int,
    pml_thickness: int,
    n_steps: int,
    n_iters: int,
    frame_stride: int,
    seed: int,
    suppression_weight: float,
    signal_weight: float,
    eps_max: float,
    regularization_lambda: float,
    source_x_override: int | None,
    target_x_override: int | None,
    objective_start_frac: float,
    target_y_shift_frac: float,
    target_z_shift_frac: float,
    source_amplitude: float,
    checkpoint_segments: int,
) -> dict:
    torch.manual_seed(seed)
    np.random.seed(seed)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    if device.type == "cuda":
        torch.cuda.reset_peak_memory_stats(device)
    nx = ny = nz = n

    spacing = torch.tensor([1.0, 1.0, 1.0], device=device, dtype=torch.float32)
    dt = 0.35
    implementation = "torch"
    source_x = pml_thickness + 1

    eps_bg = 1.0
    eps_min = 1.0
    eps_max = float(eps_max)
    sigma_e = torch.zeros((nx, ny, nz), device=device, dtype=torch.float32)
    sigma_e += 0.002

    x0, x1 = nx // 3, nx // 3 + max(4, nx // 5)
    y0, y1 = ny // 4, ny - ny // 4
    z0, z1 = nz // 4, nz - nz // 4
    design_slices = (slice(x0, x1), slice(y0, y1), slice(z0, z1))
    if source_x_override is not None:
        source_x = int(np.clip(source_x_override, pml_thickness + 1, nx - pml_thickness - 2))
    target_x = min(nx - pml_thickness - 3, x1 + 2)
    if target_x_override is not None:
        target_x = int(np.clip(target_x_override, pml_thickness + 1, nx - pml_thickness - 2))
    objective_start = int(objective_start_frac * n_steps)
    focus_window = _build_target_window(
        ny=ny,
        nz=nz,
        device=device,
        y_shift=target_y_shift_frac * ny,
        z_shift=target_z_shift_frac * nz,
        width_scale=0.14,
    )
    suppression_window = _build_background_window(focus_window)

    theta = (
        torch.full(
            (x1 - x0, y1 - y0, z1 - z0),
            0.15,
            device=device,
            dtype=torch.float32,
        )
        + 0.35
        * torch.randn(
            (x1 - x0, y1 - y0, z1 - z0),
            device=device,
            dtype=torch.float32,
        )
    )
    theta = torch.nn.Parameter(theta)
    theta_initial = theta.detach().clone()
    learning_rate = 0.18
    beta1 = 0.9
    beta2 = 0.999
    adam_eps = 1.0e-8
    regularization_lambda = float(regularization_lambda)
    optimizer = torch.optim.Adam(
        [theta],
        lr=learning_rate,
        betas=(beta1, beta2),
        eps=adam_eps,
    )

    source_window = _build_source_window(ny, nz, device)
    base_layers = _build_base_pml_layers(
        nx=nx,
        ny=ny,
        nz=nz,
        pml_thickness=pml_thickness,
        dt=dt,
        spacing=spacing,
        device=device,
        implementation=implementation,
    )

    def _compose_eps(eps_design: torch.Tensor) -> torch.Tensor:
        eps_full = torch.full((nx, ny, nz), eps_bg, device=device, dtype=torch.float32)
        eps_full[design_slices] = eps_design
        return eps_full

    with torch.no_grad():
        eps_initial_design = eps_min + (eps_max - eps_min) * torch.sigmoid(theta_initial)
        eps_initial_eval = _compose_eps(eps_initial_design)
        eps_initial_mid = eps_initial_eval[:, :, nz // 2].detach().cpu().numpy()

    eps_frames: list[np.ndarray] = [eps_initial_mid.copy()]
    objective_history: list[float] = []
    signal_history: list[float] = []

    for iteration in range(n_iters):
        optimizer.zero_grad(set_to_none=True)
        sigmoid_theta = torch.sigmoid(theta)
        eps_design = eps_min + (eps_max - eps_min) * sigmoid_theta

        eps = _compose_eps(eps_design)

        forward = _simulate_forward(
            eps=eps,
            sigma_e=sigma_e,
            base_pml_layers=base_layers,
            spacing=spacing,
            dt=dt,
            source_x=source_x,
            source_window=source_window,
            target_x=target_x,
            focus_window=focus_window,
            suppression_window=suppression_window,
            suppression_weight=suppression_weight,
            source_amplitude=source_amplitude,
            n_steps=n_steps,
            objective_start=objective_start,
            record_frames=False,
            frame_stride=frame_stride,
            implementation=implementation,
            checkpoint_segments=checkpoint_segments,
        )

        signal_tensor = forward["signal_tensor"]
        reg = regularization_lambda * torch.mean(
            (eps_design - eps_bg) * (eps_design - eps_bg)
        )
        objective = -signal_weight * signal_tensor + reg
        objective.backward()
        optimizer.step()

        signal = float(signal_tensor.detach().item())

        objective_history.append(float(objective.detach().item()))
        signal_history.append(signal)

        with torch.no_grad():
            eps_after = _compose_eps(
                eps_min + (eps_max - eps_min) * torch.sigmoid(theta.detach())
            )
            eps_mid = eps_after[:, :, nz // 2].detach().cpu().numpy()
        eps_frames.append(eps_mid.copy())

        print(
            f"[iter {iteration + 1:03d}/{n_iters:03d}] "
            f"loss={float(objective.detach().item()):.6e} signal={signal:.6e}"
        )

    with torch.no_grad():
        eps_initial_design = eps_min + (eps_max - eps_min) * torch.sigmoid(theta_initial)
        eps_final_design = eps_min + (eps_max - eps_min) * torch.sigmoid(theta.detach())
        eps_initial_eval = _compose_eps(eps_initial_design)
        eps_final_eval = _compose_eps(eps_final_design)
        eps_initial_mid = eps_initial_eval[:, :, nz // 2].detach().cpu().numpy()
        eps_final_mid = eps_final_eval[:, :, nz // 2].detach().cpu().numpy()

        initial_forward = _simulate_forward(
            eps=eps_initial_eval,
            sigma_e=sigma_e,
            base_pml_layers=base_layers,
            spacing=spacing,
            dt=dt,
            source_x=source_x,
            source_window=source_window,
            target_x=target_x,
            focus_window=focus_window,
            suppression_window=suppression_window,
            suppression_weight=suppression_weight,
            source_amplitude=source_amplitude,
            n_steps=n_steps,
            objective_start=objective_start,
            record_frames=True,
            frame_stride=frame_stride,
            implementation=implementation,
            checkpoint_segments=0,
        )
        final_forward = _simulate_forward(
            eps=eps_final_eval,
            sigma_e=sigma_e,
            base_pml_layers=base_layers,
            spacing=spacing,
            dt=dt,
            source_x=source_x,
            source_window=source_window,
            target_x=target_x,
            focus_window=focus_window,
            suppression_window=suppression_window,
            suppression_weight=suppression_weight,
            source_amplitude=source_amplitude,
            n_steps=n_steps,
            objective_start=objective_start,
            record_frames=True,
            frame_stride=frame_stride,
            implementation=implementation,
            checkpoint_segments=0,
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    design_mask = np.zeros((nx, ny), dtype=np.float32)
    design_mask[x0:x1, y0:y1] = 1.0

    eps_gif_frames: list[np.ndarray] = []
    for idx, eps_mid in enumerate(eps_frames):
        eps_gif_frames.append(
            _render_field_frame(
                eps_mid,
                design_mask,
                title=f"Permittivity z-mid (iter {idx})",
                cmap="viridis",
                vmin=eps_min,
                vmax=eps_max,
                symmetric=False,
                contour_color="white",
            )
        )

    imageio.mimsave(output_dir / "material_evolution.gif", eps_gif_frames, fps=6, loop=0)

    initial_ey_raw = initial_forward["ey_frames"]
    final_ey_raw = final_forward["ey_frames"]
    ey_vlim = max(
        float(max(np.max(np.abs(frame)) for frame in initial_ey_raw)),
        float(max(np.max(np.abs(frame)) for frame in final_ey_raw)),
        1.0e-7,
    )

    initial_ey_frames = [
        _render_field_frame(
            frame,
            design_mask,
            title=f"Initial Ey z-mid (sample {idx})",
            cmap="RdBu_r",
            vmin=-ey_vlim,
            vmax=ey_vlim,
        )
        for idx, frame in enumerate(initial_ey_raw)
    ]
    final_ey_frames = [
        _render_field_frame(
            frame,
            design_mask,
            title=f"Optimized Ey z-mid (sample {idx})",
            cmap="RdBu_r",
            vmin=-ey_vlim,
            vmax=ey_vlim,
        )
        for idx, frame in enumerate(final_ey_raw)
    ]
    comparison_ey_frames = [
        _render_wave_comparison_frame(
            initial_slice=init_frame,
            optimized_slice=opt_frame,
            design_slice=design_mask,
            title=f"Ey Comparison (sample {idx})",
            vlim=ey_vlim,
        )
        for idx, (init_frame, opt_frame) in enumerate(zip(initial_ey_raw, final_ey_raw))
    ]

    imageio.mimsave(output_dir / "ey_initial.gif", initial_ey_frames, fps=10, loop=0)
    imageio.mimsave(output_dir / "ey_optimized.gif", final_ey_frames, fps=10, loop=0)
    imageio.mimsave(output_dir / "ey_before_after.gif", comparison_ey_frames, fps=10, loop=0)

    imageio.imwrite(output_dir / "ey_initial_first.png", initial_ey_frames[0])
    imageio.imwrite(output_dir / "ey_initial_last.png", initial_ey_frames[-1])
    imageio.imwrite(output_dir / "ey_optimized_first.png", final_ey_frames[0])
    imageio.imwrite(output_dir / "ey_optimized_last.png", final_ey_frames[-1])

    _save_material_panel(
        eps_initial_mid,
        eps_final_mid,
        output_dir / "material_before_after.png",
    )
    _save_objective_plot(
        objective_history,
        signal_history,
        output_dir / "objective_history.png",
    )

    ey_initial = initial_forward["electric"][1, :, :, nz // 2].detach().cpu().numpy()
    ey_final = final_forward["electric"][1, :, :, nz // 2].detach().cpu().numpy()
    hz_initial = initial_forward["magnetic"][2, :, :, nz // 2].detach().cpu().numpy()
    hz_final = final_forward["magnetic"][2, :, :, nz // 2].detach().cpu().numpy()
    _save_forward_comparison(
        ey_initial,
        ey_final,
        hz_initial,
        hz_final,
        design_mask,
        output_dir / "forward_comparison.png",
    )

    peak_gpu_memory_mb = None
    if device.type == "cuda":
        peak_gpu_memory_mb = float(
            torch.cuda.max_memory_allocated(device) / (1024.0 * 1024.0)
        )

    metadata = {
        "device": str(device),
        "grid": [nx, ny, nz],
        "pml_thickness": int(pml_thickness),
        "n_steps": int(n_steps),
        "n_iters": int(n_iters),
        "implementation": implementation,
        "checkpoint_segments": int(checkpoint_segments),
        "peak_gpu_memory_mb": peak_gpu_memory_mb,
        "source_x": int(source_x),
        "target_plane_x": int(target_x),
        "source_x_override": None if source_x_override is None else int(source_x_override),
        "target_x_override": None if target_x_override is None else int(target_x_override),
        "objective_start_frac": float(objective_start_frac),
        "source_amplitude": float(source_amplitude),
        "target_window_center_y": float(0.5 * (ny - 1) + target_y_shift_frac * ny),
        "target_window_center_z": float(0.5 * (nz - 1) + target_z_shift_frac * nz),
        "target_y_shift_frac": float(target_y_shift_frac),
        "target_z_shift_frac": float(target_z_shift_frac),
        "suppression_weight": float(suppression_weight),
        "signal_weight": float(signal_weight),
        "eps_max": float(eps_max),
        "regularization_lambda": float(regularization_lambda),
        "objective_start_step": int(objective_start),
        "initial_signal": float(signal_history[0]),
        "final_signal": float(signal_history[-1]),
        "initial_loss": float(objective_history[0]),
        "final_loss": float(objective_history[-1]),
    }
    (output_dir / "run_metadata.json").write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Autograd-based material optimization example using EM and PML "
            "forward functionals."
        )
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("examples/electromagnetics/output_adjoint_material_focus"),
    )
    parser.add_argument("--n", type=int, default=24, help="Grid size along each axis")
    parser.add_argument("--pml-thickness", type=int, default=4)
    parser.add_argument("--n-steps", type=int, default=48)
    parser.add_argument("--n-iters", type=int, default=20)
    parser.add_argument("--frame-stride", type=int, default=2)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--suppression-weight", type=float, default=0.55)
    parser.add_argument("--signal-weight", type=float, default=1.0)
    parser.add_argument("--eps-max", type=float, default=10.0)
    parser.add_argument("--regularization-lambda", type=float, default=1.0e-7)
    parser.add_argument("--source-x", type=int, default=None)
    parser.add_argument("--target-x", type=int, default=None)
    parser.add_argument("--objective-start-frac", type=float, default=0.55)
    parser.add_argument("--target-y-shift-frac", type=float, default=0.22)
    parser.add_argument("--target-z-shift-frac", type=float, default=0.0)
    parser.add_argument("--source-amplitude", type=float, default=0.45)
    parser.add_argument(
        "--checkpoint-segments",
        type=int,
        default=8,
        help=(
            "Number of temporal segments for gradient checkpointing. "
            "Set to 0 to disable checkpointing."
        ),
    )
    args = parser.parse_args()

    metadata = run_adjoint_optimization(
        output_dir=args.output_dir,
        n=args.n,
        pml_thickness=args.pml_thickness,
        n_steps=args.n_steps,
        n_iters=args.n_iters,
        frame_stride=args.frame_stride,
        seed=args.seed,
        suppression_weight=args.suppression_weight,
        signal_weight=args.signal_weight,
        eps_max=args.eps_max,
        regularization_lambda=args.regularization_lambda,
        source_x_override=args.source_x,
        target_x_override=args.target_x,
        objective_start_frac=args.objective_start_frac,
        target_y_shift_frac=args.target_y_shift_frac,
        target_z_shift_frac=args.target_z_shift_frac,
        source_amplitude=args.source_amplitude,
        checkpoint_segments=args.checkpoint_segments,
    )

    print("Optimization complete.")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
