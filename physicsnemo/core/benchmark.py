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

"""Common benchmarking hooks shared by autograd functions and modules."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Tuple

import torch

_DEFAULT_RESULTS_DIR = Path("benchmarks/results")


class BenchmarkMixin:
    """Defines the hooks needed by the benchmark and validation helpers."""

    @classmethod
    def make_inputs(cls) -> Iterable[Tuple[str, dict[str, Any], Tuple[Any, ...]]]:
        """Yield (label, init_kwargs, run_args) tuples for benchmarking."""

        raise NotImplementedError(
            f"{cls.__name__}.make_inputs must be implemented by subclasses"
        )

    @classmethod
    def reference_impl(cls, *args, **kwargs):
        """Compute a reference output for correctness checks."""

        raise NotImplementedError(
            f"{cls.__name__}.reference_impl must be implemented by subclasses"
        )

    @classmethod
    def check(cls, *args, **kwargs) -> None:
        """Validate outputs against the reference implementation."""

        raise NotImplementedError(
            f"{cls.__name__}.check must be implemented by subclasses"
        )

    @classmethod
    def benchmark(
        cls,
        *,
        repeats: int = 10,
        warmup: int = 1,
    ) -> dict:
        """Collect benchmark data for downstream consumers.

        Parameters
        ----------
        repeats
            Number of timed iterations recorded per benchmark case.
        warmup
            Runs discarded before timings are collected. Warmups help amortize
            one-time setup costs.

        Returns
        -------
        dict
            Structured benchmark payload that can be consumed by CI, ASV, or
            plotting utilities.
        """

        if repeats <= 0:
            raise ValueError("repeats must be positive")
        if warmup < 0:
            raise ValueError("warmup must be non-negative")

        cases = []
        devices = set()
        dtypes = set()
        for index, case in enumerate(cls.make_inputs()):
            label, init_kwargs, run_args = cls._normalize_case(case)
            inputs = cls._tuple_inputs(run_args)
            tensor = cls._first_tensor(inputs)
            if tensor is None:
                case_device = torch.device("cpu")
                case_dtype = "unknown"
            else:
                case_device = tensor.device
                case_dtype = str(tensor.dtype).replace("torch.", "")

            instance = cls._instantiate_case(init_kwargs)
            cls._maybe_move_instance(instance, case_device)

            current_runner = cls._resolve_current_runner(instance)
            reference_runner = cls._resolve_reference_runner(instance)

            current_timings = cls._time_callable(
                current_runner, inputs, repeats, warmup, case_device
            )
            reference_timings = cls._time_callable(
                reference_runner, inputs, repeats, warmup, case_device
            )
            case_record = {
                "label": label,
                "index": index,
                "metadata": cls._benchmark_case_metadata(inputs),
                "device": str(case_device),
                "dtype": case_dtype,
                "init_kwargs": dict(init_kwargs),
                "current": {
                    "timings_ms": current_timings,
                    "statistics": cls._summarize_timings(current_timings),
                },
                "reference": {
                    "timings_ms": reference_timings,
                    "statistics": cls._summarize_timings(reference_timings),
                },
            }
            cases.append(case_record)
            devices.add(str(case_device))
            dtypes.add(case_dtype)

        payload = {
            "name": cls.__name__,
            "qualified_name": f"{cls.__module__}.{cls.__name__}",
            "generated_at": time.time(),
            "options": {
                "repeats": repeats,
                "warmup": warmup,
            },
            "devices": sorted(devices),
            "dtypes": sorted(dtypes),
            "cases": cases,
        }

        return payload

    @staticmethod
    def save_benchmark(payload: dict, directory: str | Path = _DEFAULT_RESULTS_DIR) -> Path:
        """Persist a benchmark payload to disk."""

        output_dir = Path(directory)
        output_dir.mkdir(parents=True, exist_ok=True)
        destination = output_dir / f"{payload['qualified_name']}.json"
        destination.write_text(json.dumps(payload, indent=2))
        return destination

    @classmethod
    def plot_benchmarks(
        cls,
        payload: dict | None = None,
        *,
        save: str | Path | None = None,
    ):
        """Render a bar chart comparing current vs reference timings."""

        if payload is None:
            payload = cls.benchmark()

        labels = [case["label"] for case in payload.get("cases", [])]
        if not labels:
            raise ValueError("No benchmark cases available to plot")

        current = [case["current"]["statistics"]["mean_ms"] for case in payload["cases"]]
        reference = [
            case["reference"]["statistics"]["mean_ms"] for case in payload["cases"]
        ]

        import matplotlib.pyplot as plt  # Imported lazily to avoid hard dependency

        x = range(len(labels))
        width = 0.35
        fig, ax = plt.subplots(figsize=(max(6, len(labels) * 1.5), 4))
        ax.bar([xi - width / 2 for xi in x], current, width, color="#2ca02c", label="Current")
        ax.bar([xi + width / 2 for xi in x], reference, width, color="#888888", label="Reference")
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, rotation=30, ha="right")
        ax.set_ylabel("Time (ms)")
        ax.set_title(f"{cls.__name__} Benchmark")
        ax.legend()
        fig.tight_layout()

        if save is not None:
            fig.savefig(save, bbox_inches="tight")
        else:
            plt.show()

        return fig

    @classmethod
    def _benchmark_forward(cls, *args, **kwargs):
        raise NotImplementedError(
            f"{cls.__name__} must implement _benchmark_forward for benchmarking"
        )

    @classmethod
    def _benchmark_case_label(cls, index: int, inputs: Tuple[Any, ...]) -> str:
        """Return a human readable label for a benchmark case."""

        return f"case_{index}"

    @classmethod
    def _benchmark_case_metadata(cls, inputs: Tuple[Any, ...]) -> dict:
        """Summarize the benchmark inputs for downstream reporting."""

        return {
            "arguments": [cls._summarize_value(value) for value in inputs],
        }

    @classmethod
    def _summarize_value(cls, value: Any) -> Any:
        if isinstance(value, torch.Tensor):
            return {
                "type": "tensor",
                "shape": list(value.shape),
                "dtype": str(value.dtype).replace("torch.", ""),
                "device": str(value.device),
                "requires_grad": bool(value.requires_grad),
            }
        if isinstance(value, (int, float, bool, str)):
            return value
        if isinstance(value, (list, tuple)):
            if all(isinstance(elem, (int, float, bool, str)) for elem in value):
                return list(value)
            return {
                "type": value.__class__.__name__,
                "length": len(value),
            }
        if isinstance(value, dict):
            summary = {}
            for key, val in value.items():
                summary[str(key)] = cls._summarize_value(val)
            return summary
        return {
            "type": value.__class__.__name__,
            "repr": repr(value),
        }

    @staticmethod
    def _tuple_inputs(raw_inputs: Any) -> Tuple[Any, ...]:
        if isinstance(raw_inputs, tuple):
            return raw_inputs
        if isinstance(raw_inputs, list):
            return tuple(raw_inputs)
        return (raw_inputs,)

    @classmethod
    def _first_tensor(cls, inputs: Tuple[Any, ...]) -> torch.Tensor | None:
        for value in inputs:
            tensor = cls._extract_tensor(value)
            if tensor is not None:
                return tensor
        return None

    @classmethod
    def _extract_tensor(cls, value: Any) -> torch.Tensor | None:
        if isinstance(value, torch.Tensor):
            return value
        if isinstance(value, (list, tuple)):
            for elem in value:
                tensor = cls._extract_tensor(elem)
                if tensor is not None:
                    return tensor
            return None
        if isinstance(value, dict):
            for elem in value.values():
                tensor = cls._extract_tensor(elem)
                if tensor is not None:
                    return tensor
        return None

    @classmethod
    def _time_callable(
        cls,
        runner: Callable[..., Any],
        args: Tuple[Any, ...],
        repeats: int,
        warmup: int,
        device: torch.device,
    ) -> list[float]:
        use_cuda = device.type == "cuda"
        if use_cuda:
            torch.cuda.synchronize(device)
        for _ in range(warmup):
            runner(*args)
            if use_cuda:
                torch.cuda.synchronize(device)

        timings: list[float] = []
        if use_cuda:
            start_event = torch.cuda.Event(enable_timing=True)
            end_event = torch.cuda.Event(enable_timing=True)
            for _ in range(repeats):
                torch.cuda.synchronize(device)
                start_event.record()
                runner(*args)
                end_event.record()
                torch.cuda.synchronize(device)
                timings.append(start_event.elapsed_time(end_event))
        else:
            for _ in range(repeats):
                t0 = time.perf_counter()
                runner(*args)
                timings.append((time.perf_counter() - t0) * 1e3)
        return timings

    @classmethod
    def _normalize_case(
        cls, case: Tuple[Any, ...]
    ) -> Tuple[str, dict[str, Any], Tuple[Any, ...]]:
        if len(case) != 3:
            raise ValueError(
                "Benchmark cases must yield (label, init_kwargs, run_args) tuples"
            )
        label, init_kwargs, run_args = case
        if not isinstance(label, str):
            raise TypeError("Benchmark case label must be a string")
        if init_kwargs is None:
            init_kwargs = {}
        if not isinstance(init_kwargs, dict):
            raise TypeError("Benchmark case init_kwargs must be a dict")
        return label, init_kwargs, run_args

    @classmethod
    def _instantiate_case(cls, init_kwargs: dict[str, Any]):
        if not init_kwargs and not issubclass(cls, torch.nn.Module):
            return None
        if issubclass(cls, torch.nn.Module):
            instance = cls(**init_kwargs)
            instance.eval()
            return instance
        return None

    @classmethod
    def _maybe_move_instance(cls, instance, device: torch.device) -> None:
        if instance is None:
            return
        if isinstance(instance, torch.nn.Module):
            try:
                instance.to(device)
            except Exception:  # pragma: no cover - best effort move
                pass

    @classmethod
    def _resolve_current_runner(cls, instance) -> Callable[..., Any]:
        if instance is not None:
            return instance
        return cls._benchmark_forward

    @classmethod
    def _resolve_reference_runner(cls, instance) -> Callable[..., Any]:
        if hasattr(cls, "reference_impl"):
            return cls.reference_impl
        raise NotImplementedError(
            f"{cls.__name__} must define reference_impl for benchmarking"
        )

    @staticmethod
    def _summarize_timings(timings: list[float]) -> dict:
        if not timings:
            return {
                "mean_ms": 0.0,
                "median_ms": 0.0,
                "min_ms": 0.0,
                "max_ms": 0.0,
            }
        sorted_times = sorted(timings)
        count = len(sorted_times)
        total = sum(sorted_times)
        if count % 2:
            median = sorted_times[count // 2]
        else:
            median = 0.5 * (
                sorted_times[count // 2 - 1] + sorted_times[count // 2]
            )
        return {
            "mean_ms": total / count,
            "median_ms": median,
            "min_ms": sorted_times[0],
            "max_ms": sorted_times[-1],
        }


__all__ = ["BenchmarkMixin"]
