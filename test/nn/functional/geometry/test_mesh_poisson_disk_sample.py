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

import numpy as np
import pytest
import torch

from physicsnemo.nn.functional import mesh_poisson_disk_sample
from physicsnemo.nn.functional.geometry import MeshPoissonDiskSample
from test.conftest import requires_module


# Build a deterministic watertight mesh for sampling tests.
def _build_case(device: str, subdivisions: int = 2):
    from physicsnemo.mesh.primitives.procedural.lumpy_sphere import (
        load as load_lumpy_sphere,
    )

    mesh = load_lumpy_sphere(subdivisions=subdivisions, device=device)
    mesh_vertices = mesh.points.to(torch.float32).contiguous()
    mesh_indices_2d = mesh.cells.to(torch.int32).contiguous()
    return mesh_vertices, mesh_indices_2d


# Sort point rows lexicographically for order-invariant comparisons.
def _sorted_points(points: torch.Tensor) -> torch.Tensor:
    if points.numel() == 0:
        return points
    points_np = points.detach().cpu().numpy()
    sort_idx = np.lexsort((points_np[:, 2], points_np[:, 1], points_np[:, 0]))
    sorted_np = points_np[sort_idx]
    return torch.from_numpy(sorted_np).to(device=points.device, dtype=points.dtype)


# Compute minimum non-diagonal pairwise distance.
def _minimum_pairwise_distance(points: torch.Tensor) -> float:
    if points.shape[0] < 2:
        return float("inf")
    distance_matrix = torch.cdist(points, points)
    distance_matrix.fill_diagonal_(float("inf"))
    return float(distance_matrix.min().item())


# Build call arguments for parameterized error-path tests.
def _case_bad_mesh_vertices_shape(
    mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor
):
    bad_vertices = torch.zeros(4, 2, device=mesh_vertices.device, dtype=torch.float32)
    return (bad_vertices, mesh_indices), {"implementation": "warp"}


def _case_bad_mesh_indices_shape(
    mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor
):
    bad_indices = torch.zeros(4, 4, device=mesh_vertices.device, dtype=torch.int32)
    return (mesh_vertices, bad_indices), {"implementation": "warp"}


def _case_bad_mesh_indices_dtype(
    mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor
):
    return (mesh_vertices, mesh_indices.to(torch.float32)), {"implementation": "warp"}


def _case_bad_mesh_indices_bounds(
    mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor
):
    bad_indices = mesh_indices.clone()
    bad_indices[0, 0] = mesh_vertices.shape[0]
    return (mesh_vertices, bad_indices), {"implementation": "warp"}


def _case_bad_min_distance(mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor):
    return (mesh_vertices, mesh_indices), {
        "min_distance": 0.0,
        "implementation": "warp",
    }


def _case_bad_batch_size(mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor):
    return (mesh_vertices, mesh_indices), {"batch_size": 0, "implementation": "warp"}


def _case_bad_max_points(mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor):
    return (mesh_vertices, mesh_indices), {"max_points": 0, "implementation": "warp"}


def _case_bad_max_iterations(mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor):
    return (mesh_vertices, mesh_indices), {
        "max_iterations": 0,
        "implementation": "warp",
    }


def _case_bad_open3d_init_factor(
    mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor
):
    return (mesh_vertices, mesh_indices), {
        "open3d_init_factor": 0,
        "implementation": "warp",
    }


def _case_bad_per_vertex_radius_shape(
    mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor
):
    return (mesh_vertices, mesh_indices), {
        "per_vertex_radius": torch.ones(5, device=mesh_vertices.device),
        "implementation": "warp",
    }


def _case_bad_per_vertex_radius_dtype(
    mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor
):
    return (mesh_vertices, mesh_indices), {
        "per_vertex_radius": torch.ones(
            mesh_vertices.shape[0],
            device=mesh_vertices.device,
            dtype=torch.int32,
        ),
        "implementation": "warp",
    }


def _case_bad_per_vertex_radius_values(
    mesh_vertices: torch.Tensor,
    mesh_indices: torch.Tensor,
):
    return (mesh_vertices, mesh_indices), {
        "per_vertex_radius": torch.zeros(
            mesh_vertices.shape[0],
            device=mesh_vertices.device,
            dtype=torch.float32,
        ),
        "implementation": "warp",
    }


def _case_bad_hash_grid_resolution_length(
    mesh_vertices: torch.Tensor,
    mesh_indices: torch.Tensor,
):
    return (mesh_vertices, mesh_indices), {
        "hash_grid_resolution": (64, 64),
        "implementation": "warp",
    }


def _case_bad_hash_grid_resolution_values(
    mesh_vertices: torch.Tensor,
    mesh_indices: torch.Tensor,
):
    return (mesh_vertices, mesh_indices), {
        "hash_grid_resolution": (64, 0, 64),
        "implementation": "warp",
    }


def _case_bad_mode(mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor):
    return (mesh_vertices, mesh_indices), {
        "mode": "not_a_mode",
        "implementation": "warp",
    }


def _case_bad_target_num_points(
    mesh_vertices: torch.Tensor, mesh_indices: torch.Tensor
):
    return (mesh_vertices, mesh_indices), {
        "mode": "weighted_sample_elimination",
        "target_num_points": 0,
        "implementation": "warp",
    }


_ERROR_CASE_BUILDERS = {
    "bad_mesh_vertices_shape": _case_bad_mesh_vertices_shape,
    "bad_mesh_indices_shape": _case_bad_mesh_indices_shape,
    "bad_mesh_indices_dtype": _case_bad_mesh_indices_dtype,
    "bad_mesh_indices_bounds": _case_bad_mesh_indices_bounds,
    "bad_min_distance": _case_bad_min_distance,
    "bad_batch_size": _case_bad_batch_size,
    "bad_max_points": _case_bad_max_points,
    "bad_max_iterations": _case_bad_max_iterations,
    "bad_open3d_init_factor": _case_bad_open3d_init_factor,
    "bad_per_vertex_radius_shape": _case_bad_per_vertex_radius_shape,
    "bad_per_vertex_radius_dtype": _case_bad_per_vertex_radius_dtype,
    "bad_per_vertex_radius_values": _case_bad_per_vertex_radius_values,
    "bad_hash_grid_resolution_length": _case_bad_hash_grid_resolution_length,
    "bad_hash_grid_resolution_values": _case_bad_hash_grid_resolution_values,
    "bad_mode": _case_bad_mode,
    "bad_target_num_points": _case_bad_target_num_points,
}


_ERROR_CASE_EXPECTATIONS = {
    "bad_mesh_vertices_shape": (ValueError, r"shape \(n_vertices, 3\)"),
    "bad_mesh_indices_shape": (ValueError, r"shape \(n_faces, 3\)"),
    "bad_mesh_indices_dtype": (TypeError, "integer dtype"),
    "bad_mesh_indices_bounds": (ValueError, "0 <= index < n_vertices"),
    "bad_min_distance": (ValueError, "strictly positive"),
    "bad_batch_size": (ValueError, "strictly positive"),
    "bad_max_points": (ValueError, "strictly positive"),
    "bad_max_iterations": (ValueError, "strictly positive"),
    "bad_open3d_init_factor": (
        ValueError,
        "open3d_init_factor must be strictly positive",
    ),
    "bad_per_vertex_radius_shape": (ValueError, "per_vertex_radius must have shape"),
    "bad_per_vertex_radius_dtype": (TypeError, "floating dtype"),
    "bad_per_vertex_radius_values": (ValueError, "strictly positive"),
    "bad_hash_grid_resolution_length": (ValueError, "exactly 3 values"),
    "bad_hash_grid_resolution_values": (ValueError, "strictly positive"),
    "bad_mode": (ValueError, "mode must be one of"),
    "bad_target_num_points": (
        ValueError,
        "target_num_points must be strictly positive",
    ),
}


# Validate warp implementation behavior across supported sampling modes.
@requires_module("warp")
@pytest.mark.parametrize(
    "mode_case",
    (
        "dart_throwing",
        "adaptive_radius",
        "weighted_sample_elimination",
    ),
)
def test_mesh_poisson_disk_sample_warp(device: str, mode_case: str):
    subdivisions = (
        3 if mode_case in {"adaptive_radius", "weighted_sample_elimination"} else 2
    )
    mesh_vertices, mesh_indices_2d = _build_case(
        device=device, subdivisions=subdivisions
    )

    kwargs = {
        "batch_size": 4096,
        "max_points": 2048,
        "max_iterations": 10,
        "verbose": False,
        "random_seed": 1234,
        "hash_grid_resolution": 64,
        "implementation": "warp",
    }

    if mode_case == "dart_throwing":
        kwargs["min_distance"] = 0.08
        output = mesh_poisson_disk_sample(mesh_vertices, mesh_indices_2d, **kwargs)

        assert output.ndim == 2
        assert output.shape[1] == 3
        assert output.dtype == torch.float32
        assert 0 < output.shape[0] <= kwargs["max_points"]
        assert _minimum_pairwise_distance(output) >= 0.9 * kwargs["min_distance"]
        return

    if mode_case == "adaptive_radius":
        z = mesh_vertices[:, 2]
        z_norm = (z - z.min()) / (z.max() - z.min()).clamp_min(1.0e-6)
        kwargs["min_distance"] = 0.06
        kwargs["per_vertex_radius"] = 0.06 + 0.05 * z_norm
        kwargs["max_iterations"] = 8
        kwargs["hash_grid_resolution"] = (64, 64, 64)
        output = mesh_poisson_disk_sample(mesh_vertices, mesh_indices_2d, **kwargs)

        assert output.ndim == 2
        assert output.shape[1] == 3
        assert output.dtype == torch.float32
        assert 0 < output.shape[0] <= kwargs["max_points"]
        assert _minimum_pairwise_distance(output) >= 0.05
        return

    kwargs["mode"] = "weighted_sample_elimination"
    kwargs["min_distance"] = 0.02
    kwargs["target_num_points"] = 512
    kwargs["open3d_init_factor"] = 5
    output = mesh_poisson_disk_sample(mesh_vertices, mesh_indices_2d, **kwargs)

    assert output.shape == (kwargs["target_num_points"], 3)
    assert output.dtype == torch.float32
    assert torch.isfinite(output).all()

    bbox_min = mesh_vertices.min(dim=0).values - 1.0e-5
    bbox_max = mesh_vertices.max(dim=0).values + 1.0e-5
    assert bool((output >= bbox_min).all())
    assert bool((output <= bbox_max).all())
    assert float(output.std(dim=0).min().item()) > 1.0e-4
    assert torch.unique(output, dim=0).shape[0] >= int(
        0.95 * kwargs["target_num_points"]
    )
    assert _minimum_pairwise_distance(output) > 0.0


# Validate equivalent outputs for flattened and (n_faces, 3) index layouts.
@requires_module("warp")
@pytest.mark.parametrize(
    "mode,extra_kwargs",
    (
        ("dart_throwing", {"min_distance": 0.08, "max_iterations": 8}),
        (
            "weighted_sample_elimination",
            {
                "min_distance": 0.02,
                "target_num_points": 512,
                "open3d_init_factor": 5,
                "max_iterations": 8,
            },
        ),
    ),
)
def test_mesh_poisson_disk_sample_index_layout_compatibility(
    device: str,
    mode: str,
    extra_kwargs: dict[str, object],
):
    mesh_vertices, mesh_indices_2d = _build_case(device=device, subdivisions=3)
    kwargs = {
        "batch_size": 4096,
        "max_points": 2048,
        "random_seed": 2026,
        "mode": mode,
        "implementation": "warp",
        **extra_kwargs,
    }

    output_faces = mesh_poisson_disk_sample(mesh_vertices, mesh_indices_2d, **kwargs)
    output_flat = mesh_poisson_disk_sample(
        mesh_vertices,
        mesh_indices_2d.reshape(-1),
        **kwargs,
    )

    if device == "cpu":
        torch.testing.assert_close(
            _sorted_points(output_faces), _sorted_points(output_flat)
        )
        return

    # GPU launches are not strictly deterministic due parallel conflict resolution.
    count_delta = abs(output_faces.shape[0] - output_flat.shape[0])
    allowed_delta = int(0.15 * max(output_faces.shape[0], output_flat.shape[0])) + 4
    assert count_delta <= allowed_delta
    torch.testing.assert_close(
        output_faces.mean(dim=0),
        output_flat.mean(dim=0),
        atol=5e-2,
        rtol=5e-2,
    )


# Validate weighted mode ignores per-vertex radius with a user warning.
@requires_module("warp")
@pytest.mark.parametrize("mode", ("weighted_sample_elimination",))
def test_mesh_poisson_disk_sample_weighted_per_vertex_radius_warning(
    device: str,
    mode: str,
):
    mesh_vertices, mesh_indices_2d = _build_case(device=device, subdivisions=3)
    per_vertex_radius = torch.full(
        (mesh_vertices.shape[0],),
        0.04,
        device=mesh_vertices.device,
        dtype=torch.float32,
    )

    with pytest.warns(UserWarning, match="per_vertex_radius is ignored"):
        output = mesh_poisson_disk_sample(
            mesh_vertices,
            mesh_indices_2d,
            min_distance=0.02,
            per_vertex_radius=per_vertex_radius,
            target_num_points=256,
            max_points=1024,
            mode=mode,
            implementation="warp",
        )
    assert output.shape[0] == 256


# Validate input/error handling paths.
@requires_module("warp")
@pytest.mark.parametrize(
    "error_case",
    (
        "bad_mesh_vertices_shape",
        "bad_mesh_indices_shape",
        "bad_mesh_indices_dtype",
        "bad_mesh_indices_bounds",
        "bad_min_distance",
        "bad_batch_size",
        "bad_max_points",
        "bad_max_iterations",
        "bad_open3d_init_factor",
        "bad_per_vertex_radius_shape",
        "bad_per_vertex_radius_dtype",
        "bad_per_vertex_radius_values",
        "bad_hash_grid_resolution_length",
        "bad_hash_grid_resolution_values",
        "bad_mode",
        "bad_target_num_points",
    ),
)
def test_mesh_poisson_disk_sample_error_handling(device: str, error_case: str):
    mesh_vertices, mesh_indices_2d = _build_case(device=device, subdivisions=2)

    call_builder = _ERROR_CASE_BUILDERS[error_case]
    expected_exception, expected_match = _ERROR_CASE_EXPECTATIONS[error_case]
    args, kwargs = call_builder(mesh_vertices, mesh_indices_2d)

    with pytest.raises(expected_exception, match=expected_match):
        mesh_poisson_disk_sample(*args, **kwargs)


# Validate benchmark input generation contract for this FunctionSpec.
@requires_module("warp")
def test_mesh_poisson_disk_sample_make_inputs_forward(device: str):
    cases = list(MeshPoissonDiskSample.make_inputs_forward(device=device))
    assert len(cases) == len(MeshPoissonDiskSample._BENCHMARK_CASES)

    labels = [case[0] for case in cases]
    assert labels == [case[0] for case in MeshPoissonDiskSample._BENCHMARK_CASES]

    label, args, kwargs = cases[0]
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    output = MeshPoissonDiskSample.dispatch(*args, implementation="warp", **kwargs)
    assert output.ndim == 2
    assert output.shape[1] == 3
    assert output.dtype == torch.float32
