# PhysicsNeMo NN Benchmarks

This directory contains ASV benchmarks for `physicsnemo.nn`.

For functionals, the benchmark flow is intentionally simple:

1. Implement or update the functional `FunctionSpec`.
2. Add representative `make_inputs_forward(device=...)` cases.
3. Add `make_inputs_backward(device=...)` when backward benchmarking is needed.
4. Register the `FunctionSpec` in `benchmarks/physicsnemo/nn/functional/registry.py`.
5. Run ASV and regenerate plots.

## Functional porting checklist

When bringing in a new functional from another repo (for example PumpkinPulse):

1. Add a `FunctionSpec` with explicit torch and warp implementations.
2. Add parity tests for forward and backward behavior.
3. Implement `make_inputs_forward` (and `make_inputs_backward` if applicable).
4. Add the functional to `benchmarks/physicsnemo/nn/functional/registry.py`.
5. Generate and include benchmark plots in docs.
6. Add a docs page entry under `docs/api/nn/functionals/`.

## Where to read more

- Functional benchmark rules and expectations:
  - `CODING_STANDARDS/FUNCTIONAL_APIS.md`
- `FunctionSpec` behavior and required hooks:
  - `physicsnemo/core/function_spec.py`

## Where to edit

- Benchmark registry (which functionals are benchmarked):
  - `benchmarks/physicsnemo/nn/functional/registry.py`
- ASV benchmark runner for functionals:
  - `benchmarks/physicsnemo/nn/functional/benchmark_functionals.py`
- Plot generation:
  - `benchmarks/physicsnemo/nn/functional/plot_functional_benchmarks.py`

## Example functionals to copy

- `physicsnemo/nn/functional/interpolation/grid_to_point_interpolation/grid_to_point_interpolation.py`
- `physicsnemo/nn/functional/neighbors/radius_search/radius_search.py`
- `physicsnemo/nn/functional/neighbors/knn/knn.py`

## Common commands

Run benchmarks (repo root):

```bash
./benchmarks/run_benchmarks.sh
```

Run only selected functionals while iterating:

```bash
PHYSICSNEMO_ASV_FUNCTIONALS=GridToPointInterpolation,RadiusSearch ./benchmarks/run_benchmarks.sh
```

Run only selected benchmark phases:

```bash
PHYSICSNEMO_ASV_PHASES=forward ./benchmarks/run_benchmarks.sh
PHYSICSNEMO_ASV_PHASES=forward,backward ./benchmarks/run_benchmarks.sh
```

Plots are written under:

- `docs/nn/functional/<category>/<functional_name>/benchmark.png` (forward)
- `docs/nn/functional/<category>/<functional_name>/benchmark_backward.png` (backward)
