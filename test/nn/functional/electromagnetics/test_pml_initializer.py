# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
import torch

from physicsnemo.nn.functional import pml_initializer
from physicsnemo.nn.functional.electromagnetics import PMLInitializer
from test.conftest import requires_module
from test.nn.functional._parity_utils import clone_case


# Build deterministic PML-initializer inputs.
def _build_case(
    device: str,
    direction: tuple[float, float, float],
    pml_shape: tuple[int, int, int],
    thickness: int,
    seed: int = 711,
):
    torch_device = torch.device(device)
    generator = torch.Generator(device=torch_device)
    generator.manual_seed(seed)

    pml_layer = torch.randn(
        36,
        pml_shape[0],
        pml_shape[1],
        pml_shape[2],
        generator=generator,
        device=torch_device,
        dtype=torch.float32,
    )

    args = (
        pml_layer,
        direction,
        thickness,
        0.5,
    )
    kwargs = {
        "kappa": 1.2,
        "a": 1.0e-8,
        "inplace": False,
    }
    return args, kwargs


def test_pml_initializer_torch(device: str):
    args, kwargs = _build_case(
        device=device,
        direction=(1.0, 0.0, 0.0),
        pml_shape=(8, 12, 10),
        thickness=8,
        seed=711,
    )
    output = pml_initializer(*args, implementation="torch", **kwargs)
    reference = PMLInitializer.dispatch(*args, implementation="torch", **kwargs)
    PMLInitializer.compare_forward(output, reference)


@requires_module("warp")
def test_pml_initializer_warp(device: str):
    args, kwargs = _build_case(
        device=device,
        direction=(1.0, 0.0, 0.0),
        pml_shape=(8, 12, 10),
        thickness=8,
        seed=712,
    )
    output = pml_initializer(*args, implementation="warp", **kwargs)
    assert output.shape == args[0].shape


def test_pml_initializer_make_inputs_forward(device: str):
    label, args, kwargs = next(iter(PMLInitializer.make_inputs_forward(device)))
    assert isinstance(label, str)
    assert isinstance(args, tuple)
    assert isinstance(kwargs, dict)

    output = PMLInitializer.dispatch(*args, implementation="torch", **kwargs)
    assert output.shape[0] == 36


@requires_module("warp")
def test_pml_initializer_backend_forward_parity(device: str):
    args_torch, kwargs_torch = _build_case(
        device=device,
        direction=(0.0, 1.0, 0.0),
        pml_shape=(12, 6, 10),
        thickness=6,
        seed=713,
    )
    args_warp, kwargs_warp = clone_case(args_torch, kwargs_torch)

    out_torch = PMLInitializer.dispatch(
        *args_torch,
        implementation="torch",
        **kwargs_torch,
    )
    out_warp = PMLInitializer.dispatch(
        *args_warp,
        implementation="warp",
        **kwargs_warp,
    )
    PMLInitializer.compare_forward(out_warp, out_torch)


def test_pml_initializer_compare_forward_contract(device: str):
    _, args, kwargs = next(iter(PMLInitializer.make_inputs_forward(device)))
    output = PMLInitializer.dispatch(*args, implementation="torch", **kwargs)
    reference = output.detach().clone()
    PMLInitializer.compare_forward(output, reference)


def test_pml_initializer_error_handling(device: str):
    args, kwargs = _build_case(
        device=device,
        direction=(1.0, 0.0, 0.0),
        pml_shape=(8, 12, 10),
        thickness=8,
        seed=714,
    )

    with pytest.raises(ValueError, match="direction must have exactly one non-zero"):
        PMLInitializer.dispatch(
            args[0],
            (1.0, 1.0, 0.0),
            args[2],
            args[3],
            implementation="torch",
            **kwargs,
        )

    with pytest.raises(ValueError, match="thickness must match pml_layer extent"):
        PMLInitializer.dispatch(
            args[0],
            args[1],
            args[2] - 1,
            args[3],
            implementation="torch",
            **kwargs,
        )


def test_pml_initializer_inplace_contract(device: str):
    args, kwargs = _build_case(
        device=device,
        direction=(0.0, 0.0, -1.0),
        pml_shape=(12, 10, 6),
        thickness=6,
        seed=715,
    )

    pml_original = args[0]
    pml_before = pml_original.clone()

    out_of_place = PMLInitializer.dispatch(
        *args,
        implementation="torch",
        **kwargs,
    )
    assert out_of_place.data_ptr() != pml_original.data_ptr()
    torch.testing.assert_close(pml_original, pml_before)

    inplace_kwargs = dict(kwargs)
    inplace_kwargs["inplace"] = True
    in_place = PMLInitializer.dispatch(
        *args,
        implementation="torch",
        **inplace_kwargs,
    )
    assert in_place.data_ptr() == pml_original.data_ptr()
