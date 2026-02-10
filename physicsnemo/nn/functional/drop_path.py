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

import importlib
from typing import Any

import torch
from torch import Tensor

from physicsnemo.core.function_spec import FunctionSpec
from physicsnemo.core.version_check import check_version_spec

WARP_AVAILABLE = check_version_spec("warp", "0.6.0", hard_fail=False)

if WARP_AVAILABLE:
    wp = importlib.import_module("warp")
    wp.config.quiet = True
    wp.init()

    @wp.func
    def _apply_mask(x: wp.float16, mask: float):
        return x * wp.float16(mask)

    @wp.func
    def _apply_mask(x: wp.float32, mask: float):
        return x * mask

    @wp.func
    def _apply_mask(x: wp.float64, mask: float):
        return x * wp.float64(mask)

    @wp.kernel
    def _drop_path_any(
        x: wp.array(dtype=Any),
        out: wp.array(dtype=Any),
        inner: int,
        keep_prob: float,
        scale_by_keep: int,
        seed: int,
    ):
        tid = wp.tid()
        batch = tid // inner
        rng = wp.rand_init(seed, batch)
        r = wp.randf(rng)
        mask = 0.0
        if r < keep_prob:
            mask = 1.0
            if scale_by_keep:
                mask = mask / keep_prob
        out[tid] = _apply_mask(x[tid], mask)

    wp.overload(
        _drop_path_any,
        [
            wp.array(dtype=wp.float16),
            wp.array(dtype=wp.float16),
            int,
            float,
            int,
            int,
        ],
    )
    wp.overload(
        _drop_path_any,
        [
            wp.array(dtype=wp.float32),
            wp.array(dtype=wp.float32),
            int,
            float,
            int,
            int,
        ],
    )
    wp.overload(
        _drop_path_any,
        [
            wp.array(dtype=wp.float64),
            wp.array(dtype=wp.float64),
            int,
            float,
            int,
            int,
        ],
    )

    @torch.library.custom_op("physicsnemo::drop_path_warp", mutates_args=())
    def drop_path_impl(
        x: torch.Tensor,
        keep_prob: float,
        scale_by_keep: bool,
        seed: int,
    ) -> torch.Tensor:

        x_contig = x.contiguous()
        output = torch.empty_like(x_contig)
        inner = x_contig.numel() // x_contig.shape[0]

        wp_device, wp_stream = FunctionSpec.warp_launch_context(x_contig)

        with wp.ScopedStream(wp_stream):
            wp.launch(
                _drop_path_any,
                dim=x_contig.numel(),
                inputs=[
                    wp.from_torch(x_contig.view(-1)),
                    wp.from_torch(output.view(-1)),
                    int(inner),
                    float(keep_prob),
                    int(scale_by_keep),
                    int(seed),
                ],
                device=wp_device,
                stream=wp_stream,
            )

        return output

    @drop_path_impl.register_fake
    def _(
        x: torch.Tensor,
        keep_prob: float,
        scale_by_keep: bool,
        seed: int,
    ) -> torch.Tensor:
        return torch.empty_like(x)

    def drop_path_warp(
        x: Tensor,
        drop_prob: float = 0.0,
        training: bool = False,
        scale_by_keep: bool = True,
    ) -> Tensor:
        if drop_prob == 0.0 or not training:
            return x

        keep_prob = 1 - drop_prob

        if x.dtype == torch.bfloat16:
            shape = (x.shape[0],) + (1,) * (x.ndim - 1)
            random_tensor = x.new_empty(shape).bernoulli_(keep_prob)
            if keep_prob > 0.0 and scale_by_keep:
                random_tensor.div_(keep_prob)
            return x * random_tensor

        seed = int(torch.randint(0, 2**31 - 1, (1,), device="cpu").item())
        return drop_path_impl(x, keep_prob, scale_by_keep, seed)
else:

    def drop_path_warp(
        x: Tensor,
        drop_prob: float = 0.0,
        training: bool = False,
        scale_by_keep: bool = True,
    ) -> Tensor:
        raise ImportError(
            "warp is not installed, can not be used as an implementation for drop_path"
        )


class DropPath(FunctionSpec):
    """Drop paths (stochastic depth) per sample.

    Cut & paste from timm master. Drop paths (Stochastic Depth) per sample (when
    applied in main path of residual blocks). This is the same as the
    DropConnect implementation used for EfficientNet and related networks, but
    the original name is misleading as "Drop Connect" is a different form of
    dropout. See: https://github.com/tensorflow/tpu/issues/494#issuecomment-532968956
    for discussion.

    Parameters
    ----------
    x : torch.Tensor
        Input tensor.
    drop_prob : float, optional
        Drop probability, by default 0.0.
    training : bool, optional
        Whether stochastic depth is enabled, by default False.
    scale_by_keep : bool, optional
        Scale by keep probability, by default True.
    implementation : {"torch"} or None
        Implementation to use. When ``None``, dispatch selects the available
        implementation.

    Notes
    -----
    The layer and argument names use "drop path" rather than mixing DropConnect
    or "survival rate" to align with common usage.
    """

    @FunctionSpec.register(name="warp", required_imports=("warp>=0.6.0",), rank=0)
    def warp_forward(
        x: Tensor,
        drop_prob: float = 0.0,
        training: bool = False,
        scale_by_keep: bool = True,
    ) -> Tensor:
        return drop_path_warp(
            x,
            drop_prob=drop_prob,
            training=training,
            scale_by_keep=scale_by_keep,
        )

    @FunctionSpec.register(name="torch", rank=1, baseline=True)
    def torch_forward(
        x: Tensor,
        drop_prob: float = 0.0,
        training: bool = False,
        scale_by_keep: bool = True,
    ) -> Tensor:
        if drop_prob == 0.0 or not training:
            return x
        keep_prob = 1 - drop_prob
        shape = (x.shape[0],) + (1,) * (x.ndim - 1)
        random_tensor = x.new_empty(shape).bernoulli_(keep_prob)
        if keep_prob > 0.0 and scale_by_keep:
            random_tensor.div_(keep_prob)
        return x * random_tensor

    @classmethod
    def make_inputs(cls, device: torch.device | str = "cpu"):
        device = torch.device(device)
        cases = [
            ("small", 8, 64),
            ("medium", 16, 256),
            ("large", 32, 1024),
        ]
        for label, batch, features in cases:
            x = torch.randn(batch, features, device=device)
            yield (
                f"{label}-batch{batch}-features{features}-drop0p1-train",
                (x,),
                {"drop_prob": 0.1, "training": True, "scale_by_keep": True},
            )


drop_path = DropPath.make_function("drop_path")


__all__ = [
    "DropPath",
    "drop_path",
]
