# SPDX-FileCopyrightText: Copyright (c) 2023 - 2026 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Backward has two kernels by design:
# - fields-only gradients (E/H)
# - full gradients (E/H plus material fields)
from .backward_fields import _magnetic_field_update_backward_kernel_fields
from .backward_full import _magnetic_field_update_backward_kernel_full
from .forward_mu_field_scalar import _magnetic_field_update_kernel_mu_field_scalar
from .forward_mu_field_sigma_field import (
    _magnetic_field_update_kernel_mu_field_sigma_field,
)
from .forward_scalar_scalar import _magnetic_field_update_kernel_scalar_scalar
from .forward_scalar_sigma_field import _magnetic_field_update_kernel_scalar_sigma_field

__all__ = [
    "_magnetic_field_update_kernel_scalar_scalar",
    "_magnetic_field_update_kernel_scalar_sigma_field",
    "_magnetic_field_update_kernel_mu_field_scalar",
    "_magnetic_field_update_kernel_mu_field_sigma_field",
    "_magnetic_field_update_backward_kernel_fields",
    "_magnetic_field_update_backward_kernel_full",
]
