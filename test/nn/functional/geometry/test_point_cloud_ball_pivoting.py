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

from __future__ import annotations

import math

import pytest
import torch

from physicsnemo.nn.functional import point_cloud_ball_pivoting
from physicsnemo.nn.functional.geometry import PointCloudBallPivoting
from test.conftest import requires_module


# Build a deterministic oriented point cloud on a perturbed unit sphere.
def _build_case(
    device: str,
    *,
    num_points: int = 2048,
    seed: int = 2026,
) -> tuple[torch.Tensor, torch.Tensor]:
    torch_device = torch.device(device)
    generator = torch.Generator(device=torch_device)
    generator.manual_seed(seed)

    u = torch.rand(
        (num_points,), generator=generator, device=torch_device, dtype=torch.float32
    )
    v = torch.rand(
        (num_points,), generator=generator, device=torch_device, dtype=torch.float32
    )

    theta = 2.0 * math.pi * u
    z = 2.0 * v - 1.0
    radial = torch.sqrt(torch.clamp(1.0 - z * z, min=0.0))

    x = radial * torch.cos(theta)
    y = radial * torch.sin(theta)
    points = torch.stack((x, y, z), dim=1)
    points = points + 0.04 * torch.stack(
        (
            torch.sin(2.0 * theta) * radial,
            torch.cos(3.0 * theta) * radial,
            0.5 * torch.sin(theta),
        ),
        dim=1,
    )
    normals = torch.nn.functional.normalize(points, dim=1)
    return points.contiguous(), normals.contiguous()


# Build call arguments for parameterized error-path tests.
def _case_bad_points_shape(points: torch.Tensor, normals: torch.Tensor):
    bad_points = torch.zeros((4, 2), device=points.device, dtype=torch.float32)
    return (bad_points, normals), {"radii": (0.1,), "implementation": "warp"}


def _case_bad_normals_shape(points: torch.Tensor, normals: torch.Tensor):
    bad_normals = torch.zeros((4, 2), device=points.device, dtype=torch.float32)
    return (points, bad_normals), {"radii": (0.1,), "implementation": "warp"}


def _case_mismatched_rows(points: torch.Tensor, normals: torch.Tensor):
    return (points, normals[:-1]), {"radii": (0.1,), "implementation": "warp"}


def _case_too_few_points(points: torch.Tensor, normals: torch.Tensor):
    tiny_points = points[:2].contiguous()
    tiny_normals = normals[:2].contiguous()
    return (tiny_points, tiny_normals), {"radii": (0.1,), "implementation": "warp"}


def _case_empty_radii(points: torch.Tensor, normals: torch.Tensor):
    return (points, normals), {"radii": (), "implementation": "warp"}


def _case_bad_radius(points: torch.Tensor, normals: torch.Tensor):
    return (points, normals), {"radii": (0.0,), "implementation": "warp"}


def _case_bad_max_neighbors(points: torch.Tensor, normals: torch.Tensor):
    return (points, normals), {
        "radii": (0.1,),
        "max_neighbors": 2,
        "implementation": "warp",
    }


def _case_bad_max_triangles(points: torch.Tensor, normals: torch.Tensor):
    return (points, normals), {
        "radii": (0.1,),
        "max_triangles": 0,
        "implementation": "warp",
    }


def _case_bad_front_mode(points: torch.Tensor, normals: torch.Tensor):
    return (points, normals), {
        "radii": (0.1,),
        "front_mode": "unknown",
        "implementation": "warp",
    }


def _case_bad_front_batch_size(points: torch.Tensor, normals: torch.Tensor):
    return (points, normals), {
        "radii": (0.1,),
        "front_mode": "batched",
        "front_batch_size": 0,
        "implementation": "warp",
    }


_ERROR_CASE_BUILDERS = {
    "bad_points_shape": _case_bad_points_shape,
    "bad_normals_shape": _case_bad_normals_shape,
    "mismatched_rows": _case_mismatched_rows,
    "too_few_points": _case_too_few_points,
    "empty_radii": _case_empty_radii,
    "bad_radius": _case_bad_radius,
    "bad_max_neighbors": _case_bad_max_neighbors,
    "bad_max_triangles": _case_bad_max_triangles,
    "bad_front_mode": _case_bad_front_mode,
    "bad_front_batch_size": _case_bad_front_batch_size,
}


_ERROR_CASE_EXPECTATIONS = {
    "bad_points_shape": (ValueError, r"points must have shape \(n_points, 3\)"),
    "bad_normals_shape": (ValueError, r"normals must have shape \(n_points, 3\)"),
    "mismatched_rows": (ValueError, "same number of rows"),
    "too_few_points": (ValueError, "at least three points"),
    "empty_radii": (ValueError, "radii must contain at least one value"),
    "bad_radius": (ValueError, "strictly positive finite"),
    "bad_max_neighbors": (ValueError, "max_neighbors must be >= 3"),
    "bad_max_triangles": (ValueError, "strictly positive"),
    "bad_front_mode": (ValueError, "front_mode must be one of"),
    "bad_front_batch_size": (ValueError, "front_batch_size must be strictly positive"),
}


# Validate core warp output properties and triangle index validity.
@requires_module("warp")
def test_point_cloud_ball_pivoting_warp(device: str):
    points, normals = _build_case(device=device, num_points=2400, seed=1234)

    vertices, faces = point_cloud_ball_pivoting(
        points,
        normals,
        radii=(0.08, 0.11, 0.14),
        max_neighbors=128,
        implementation="warp",
    )

    assert vertices.shape == points.shape
    assert vertices.dtype == torch.float32
    assert faces.ndim == 2
    assert faces.shape[1] == 3
    assert faces.dtype == torch.int32
    assert faces.shape[0] > 0

    assert int(faces.min().item()) >= 0
    assert int(faces.max().item()) < points.shape[0]


# Validate batched front mode and sanity against serial output.
@requires_module("warp")
def test_point_cloud_ball_pivoting_batched_mode(device: str):
    points, normals = _build_case(device=device, num_points=2000, seed=4321)

    _, faces_serial = point_cloud_ball_pivoting(
        points,
        normals,
        radii=(0.08, 0.11, 0.14),
        max_neighbors=128,
        front_mode="serial",
        implementation="warp",
    )
    _, faces_batched = point_cloud_ball_pivoting(
        points,
        normals,
        radii=(0.08, 0.11, 0.14),
        max_neighbors=128,
        front_mode="batched",
        front_batch_size=24,
        implementation="warp",
    )

    assert faces_batched.ndim == 2
    assert faces_batched.shape[1] == 3
    assert faces_batched.dtype == torch.int32
    assert faces_batched.shape[0] > 0
    assert int(faces_batched.min().item()) >= 0
    assert int(faces_batched.max().item()) < points.shape[0]

    # Batched front processing can change growth order, but should stay in-range.
    count_delta = abs(faces_serial.shape[0] - faces_batched.shape[0])
    allowed_delta = int(0.30 * max(faces_serial.shape[0], faces_batched.shape[0])) + 8
    assert count_delta <= allowed_delta


# Validate max_triangles cap is respected.
@requires_module("warp")
def test_point_cloud_ball_pivoting_max_triangles_cap(device: str):
    points, normals = _build_case(device=device, num_points=1800, seed=2027)

    _, faces = point_cloud_ball_pivoting(
        points,
        normals,
        radii=(0.09, 0.12),
        max_neighbors=96,
        max_triangles=120,
        implementation="warp",
    )

    assert faces.shape[0] <= 120


# Validate benchmark input generation contract for this FunctionSpec.
@requires_module("warp")
def test_point_cloud_ball_pivoting_make_inputs_forward(device: str):
    cases = list(PointCloudBallPivoting.make_inputs_forward(device=device))
    assert len(cases) == len(PointCloudBallPivoting._BENCHMARK_CASES)

    labels = [case[0] for case in cases]
    assert labels == [case[0] for case in PointCloudBallPivoting._BENCHMARK_CASES]

    label, args, kwargs = cases[0]
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    vertices, faces = PointCloudBallPivoting.dispatch(
        *args,
        implementation="warp",
        **kwargs,
    )
    assert vertices.ndim == 2
    assert vertices.shape[1] == 3
    assert faces.ndim == 2
    assert faces.shape[1] == 3


# Validate input/error handling paths.
@requires_module("warp")
@pytest.mark.parametrize(
    "error_case",
    (
        "bad_points_shape",
        "bad_normals_shape",
        "mismatched_rows",
        "too_few_points",
        "empty_radii",
        "bad_radius",
        "bad_max_neighbors",
        "bad_max_triangles",
        "bad_front_mode",
        "bad_front_batch_size",
    ),
)
def test_point_cloud_ball_pivoting_error_handling(device: str, error_case: str):
    points, normals = _build_case(device=device, num_points=256, seed=7)
    call_builder = _ERROR_CASE_BUILDERS[error_case]
    expected_exception, expected_match = _ERROR_CASE_EXPECTATIONS[error_case]
    args, kwargs = call_builder(points, normals)

    with pytest.raises(expected_exception, match=expected_match):
        point_cloud_ball_pivoting(*args, **kwargs)
