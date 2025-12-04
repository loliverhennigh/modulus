"""Finite difference functional backed by Warp custom ops."""

from __future__ import annotations

from typing import Any, Iterable, Sequence, Tuple

import torch
from torch import Tensor

try:
    import warp as wp
    wp.init()
except ImportError as err:  # pragma: no cover - ImportError only raised in misconfigured envs.
    raise ImportError(
        "Warp is required for the finite difference functional. Install it from https://github.com/NVIDIA/warp"
    ) from err

from physicsnemo.core.function import Function

wp.config.quiet = True


def _normalize_spacing(spacing: Sequence[float] | float, dims: int) -> Tuple[float, ...]:
    if isinstance(spacing, Iterable) and not isinstance(spacing, (str, bytes)):
        spacing_values = tuple(float(s) for s in spacing)
    else:
        spacing_values = (float(spacing),)

    if len(spacing_values) == 1 and dims > 1:
        spacing_values = spacing_values * dims

    if len(spacing_values) != dims:
        raise ValueError(
            f"Spacing must provide {dims} value(s) for the spatial dimensions, got {spacing_values}"
        )

    if any(s <= 0.0 for s in spacing_values):
        raise ValueError(f"Spacing values must be positive, got {spacing_values}")

    return spacing_values


def _prepare_input(values: Tensor, has_batch: bool) -> tuple[Tensor, bool]:
    if has_batch:
        return values, False
    return values.unsqueeze(0), True


def _check_spatial_shape(spatial_shape: Tuple[int, ...]) -> None:
    for size in spatial_shape:
        if size < 3:
            raise ValueError(
                "Finite differences require at least three points per spatial dimension; "
                f"got shape {spatial_shape}."
            )


def _get_wp_context(tensor: Tensor) -> tuple[wp.stream, str | None]:
    if tensor.device.type == "cuda":
        stream = wp.stream_from_torch(torch.cuda.current_stream(tensor.device))
        return stream, None
    return None, "cpu"


def _launch_kernel(kernel, dim: int, inputs: list, stream: wp.stream, device: str | None) -> None:
    wp.launch(
        kernel,
        dim=dim,
        inputs=inputs,
        stream=stream,
        device=device,
    )


@wp.func
def _wrap_index(idx: int, size: int):
    result = idx % size
    if result < 0:
        result += size
    return result


@wp.kernel
def _finite_difference_1d_kernel(
    values: wp.array(dtype=wp.float32, ndim=2),
    gradients: wp.array(dtype=wp.float32, ndim=2),
    batch: int,
    dim0: int,
    spacing0: float,
):
    tid = wp.tid()
    total = batch * dim0
    if tid >= total:
        return

    b = tid // dim0
    i = tid - b * dim0

    ip = _wrap_index(i + 1, dim0)
    im = _wrap_index(i - 1, dim0)

    grad = (values[b, ip] - values[b, im]) / (2.0 * spacing0)

    gradients[b, i] = grad


@wp.kernel
def _finite_difference_2d_kernel(
    values: wp.array(dtype=wp.float32, ndim=3),
    gradients: wp.array(dtype=wp.vec2f, ndim=3),
    batch: int,
    dim0: int,
    dim1: int,
    spacing0: float,
    spacing1: float,
):
    tid = wp.tid()
    total = batch * dim0 * dim1
    if tid >= total:
        return

    plane = dim0 * dim1
    b = tid // plane
    rem = tid - b * plane
    i = rem // dim1
    j = rem - i * dim1

    # Axis 0 derivative
    ip = _wrap_index(i + 1, dim0)
    im = _wrap_index(i - 1, dim0)
    grad0 = (values[b, ip, j] - values[b, im, j]) / (2.0 * spacing0)

    # Axis 1 derivative
    jp = _wrap_index(j + 1, dim1)
    jm = _wrap_index(j - 1, dim1)
    grad1 = (values[b, i, jp] - values[b, i, jm]) / (2.0 * spacing1)

    gradients[b, i, j] = wp.vec2f(grad0, grad1)


@wp.kernel
def _finite_difference_3d_kernel(
    values: wp.array(dtype=wp.float32, ndim=4),
    gradients: wp.array(dtype=wp.vec3f, ndim=4),
    batch: int,
    dim0: int,
    dim1: int,
    dim2: int,
    spacing0: float,
    spacing1: float,
    spacing2: float,
):
    tid = wp.tid()
    total = batch * dim0 * dim1 * dim2
    if tid >= total:
        return

    plane = dim1 * dim2
    volume = dim0 * plane
    b = tid // volume
    rem = tid - b * volume
    i = rem // plane
    rem = rem - i * plane
    j = rem // dim2
    k = rem - j * dim2

    # Axis 0 derivative
    ip = _wrap_index(i + 1, dim0)
    im = _wrap_index(i - 1, dim0)
    grad0 = (values[b, ip, j, k] - values[b, im, j, k]) / (2.0 * spacing0)

    # Axis 1 derivative
    jp = _wrap_index(j + 1, dim1)
    jm = _wrap_index(j - 1, dim1)
    grad1 = (values[b, i, jp, k] - values[b, i, jm, k]) / (2.0 * spacing1)

    # Axis 2 derivative
    kp = _wrap_index(k + 1, dim2)
    km = _wrap_index(k - 1, dim2)
    grad2 = (values[b, i, j, kp] - values[b, i, j, km]) / (2.0 * spacing2)

    gradients[b, i, j, k] = wp.vec3f(grad0, grad1, grad2)


def _run_finite_difference(
    values: Tensor,
    spacing: Tuple[float, ...],
    has_batch: bool,
) -> Tensor:
    if not values.is_floating_point():
        raise TypeError("Finite differences require floating point tensors")

    dims = values.dim() - (1 if has_batch else 0)
    if dims not in (1, 2, 3):
        raise ValueError(
            f"Finite differences support 1D, 2D, or 3D inputs, got tensor with {values.dim()} dims"
        )

    spacing_tuple = _normalize_spacing(spacing, dims)

    values32 = values.to(torch.float32).contiguous()
    prepared, added_batch = _prepare_input(values32, has_batch)
    batch = prepared.shape[0]
    spatial_shape = tuple(int(s) for s in prepared.shape[1:])
    _check_spatial_shape(spatial_shape)

    if dims == 1:
        grad_shape = (batch, *spatial_shape)
        grad_dtype = wp.float32
    elif dims == 2:
        grad_shape = (batch, *spatial_shape, dims)
        grad_dtype = wp.vec2f
    else:
        grad_shape = (batch, *spatial_shape, dims)
        grad_dtype = wp.vec3f

    gradients = torch.empty(
        grad_shape,
        device=values.device,
        dtype=torch.float32,
    )

    stream, wp_device = _get_wp_context(prepared)

    with wp.ScopedStream(stream):
        # wp_values = wp.from_torch(prepared, dtype=wp.float32, return_ctype=True)
        wp_values = wp.from_torch(prepared, dtype=wp.float32)
        # wp_grads = wp.from_torch(gradients, dtype=wp.float32, return_ctype=True)
        wp_grads = wp.from_torch(gradients, dtype=grad_dtype)

        if dims == 1:
            inputs = [wp_values, wp_grads, batch, spatial_shape[0], spacing_tuple[0]]
            _launch_kernel(
                _finite_difference_1d_kernel,
                batch * spatial_shape[0],
                inputs,
                stream,
                wp_device,
            )
        elif dims == 2:
            inputs = [
                wp_values,
                wp_grads,
                batch,
                spatial_shape[0],
                spatial_shape[1],
                spacing_tuple[0],
                spacing_tuple[1],
            ]
            _launch_kernel(
                _finite_difference_2d_kernel,
                batch * spatial_shape[0] * spatial_shape[1],
                inputs,
                stream,
                wp_device,
            )
        else:
            inputs = [
                wp_values,
                wp_grads,
                batch,
                spatial_shape[0],
                spatial_shape[1],
                spatial_shape[2],
                spacing_tuple[0],
                spacing_tuple[1],
                spacing_tuple[2],
            ]
            _launch_kernel(
                _finite_difference_3d_kernel,
                batch * spatial_shape[0] * spatial_shape[1] * spatial_shape[2],
                inputs,
                stream,
                wp_device,
            )

    if dims == 1:
        gradients = gradients.unsqueeze(1)
    else:
        gradients = gradients.movedim(-1, 1)

    if added_batch:
        gradients = gradients.squeeze(0)

    if values.dtype != torch.float32:
        gradients = gradients.to(values.dtype)

    return gradients


@torch.library.custom_op("physicsnemo::finite_difference_nd", mutates_args=())
def finite_difference_op(
    values: Tensor,
    spacing: Sequence[float],
    has_batch: bool = True,
) -> Tensor:
    return _run_finite_difference(values, spacing, has_batch)


@finite_difference_op.register_fake
def _finite_difference_fake(
    values: Tensor,
    spacing: Sequence[float],
    has_batch: bool = True,
) -> Tensor:
    dims = values.dim() - (1 if has_batch else 0)
    if dims not in (1, 2, 3):
        raise RuntimeError(
            "Finite differences support only 1D, 2D, or 3D inputs in fake tensor mode"
        )

    spatial_shape = values.shape[1:] if has_batch else values.shape
    if has_batch:
        shape = (values.shape[0], dims, *spatial_shape)
    else:
        shape = (dims, *spatial_shape)

    return values.new_empty(shape)


class FiniteDifference(Function):
    """Autograd wrapper around the custom finite difference op."""

    @staticmethod
    def forward(ctx, values: Tensor, spacing: Sequence[float], has_batch: bool = True):
        output = torch.ops.physicsnemo.finite_difference_nd(values, spacing, has_batch)
        ctx.mark_non_differentiable(output)
        return output

    @staticmethod
    def backward(ctx, *grad_outputs):  # pragma: no cover - no autograd yet
        return None, None, None

    @classmethod
    def make_inputs(
        cls,
    ) -> Iterable[tuple[str, dict[str, Any], tuple[Tensor, Sequence[float], bool]]]:
        configs = {
            "1D": ((1, 1024*8), (1.0,)),
            "2D": ((1, 256, 256), (1.0, 1.2)),
            "3D": ((1, 64, 64, 64), (0.8, 1.0, 1.2)),
        }
        device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
        for label, (shape, spacing) in configs.items():
            values = torch.randn(shape, device=device, dtype=torch.float32)
            yield label, {}, (values, spacing, True)

    @classmethod
    def reference_impl(
        cls, values: Tensor, spacing: Sequence[float], has_batch: bool = True
    ) -> Tensor:
        dims = values.dim() - (1 if has_batch else 0)
        tensor = values if has_batch else values.unsqueeze(0)
        grads = []
        for axis in range(1, dims + 1):
            forward = torch.roll(tensor, shifts=-1, dims=axis)
            backward = torch.roll(tensor, shifts=1, dims=axis)
            grads.append((forward - backward) / (2.0 * spacing[axis - 1]))
        stacked = torch.stack(grads, dim=1)
        return stacked if has_batch else stacked.squeeze(0)

    @classmethod
    def check(cls, actual: Tensor, expected: Tensor) -> None:
        torch.testing.assert_close(actual, expected, rtol=1e-4, atol=1e-4)


__all__ = ["FiniteDifference"]
