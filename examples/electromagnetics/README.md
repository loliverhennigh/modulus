# Electromagnetics Functional Demo

This folder contains an end-to-end FDTD-style demo that exercises the
PhysicsNeMo electromagnetics functionals:

- `electric_field_update`
- `magnetic_field_update`
- `pml_initializer`
- `pml_phi_e_update`
- `pml_phi_h_update`
- `pml_electric_field_update`
- `pml_magnetic_field_update`

The scenario is a pulsed planar-wave source propagating through a 3D domain and
scattering from a lumpy dielectric sphere with PML boundaries.

## Run

From repo root:

```bash
PYTHONPATH=. SCIPY_ARRAY_API=1 python examples/electromagnetics/planar_wave_lumpy_sphere.py \
  --output-dir examples/electromagnetics/output/planar_wave_lumpy_sphere \
  --n 72 \
  --steps 180 \
  --frame-stride 3
```

The demo uses the **default FunctionSpec dispatch** for each functional.

## Outputs

The script writes:

- `material_map.png`
- `energy_timeseries.png`
- `final_fields.png`
- `ey_z_mid.gif`
- `hz_z_mid.gif`
- `ey_frame_first.png`, `ey_frame_last.png`
- `hz_frame_first.png`, `hz_frame_last.png`
- `run_summary.json`
