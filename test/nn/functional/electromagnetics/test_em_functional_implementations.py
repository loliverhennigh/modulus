# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from physicsnemo.nn.functional.electromagnetics import (
    ElectricFieldUpdate,
    MagneticFieldUpdate,
    PMLElectricFieldUpdate,
    PMLInitializer,
    PMLMagneticFieldUpdate,
    PMLPhiEUpdate,
    PMLPhiHUpdate,
)


@pytest.mark.parametrize(
    "spec_cls",
    [
        ElectricFieldUpdate,
        MagneticFieldUpdate,
        PMLInitializer,
        PMLPhiEUpdate,
        PMLPhiHUpdate,
        PMLElectricFieldUpdate,
        PMLMagneticFieldUpdate,
    ],
)
def test_em_functional_has_torch_and_warp_registration(spec_cls):
    impls = set(spec_cls.implementations())
    assert "torch" in impls
    assert "warp" in impls
