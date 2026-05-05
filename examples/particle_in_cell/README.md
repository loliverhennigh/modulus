# Particle-In-Cell Examples

## Hot Plasma Ball Expansion

`hot_plasma_ball_expansion.py` runs a periodic-box PIC simulation using the
PhysicsNemo functionals:

- `gather_fields_to_particles`
- `particle_push_boris`
- `deposit_current_charge_conserving`
- `magnetic_field_update`
- `electric_field_update`

It initializes a quasi-neutral electron/proton sphere in a 200 um periodic box.
By default, the electron mass is scaled by `100x` to increase skin depth, and
the density is chosen near the highest value that still satisfies a requested
skin-depth resolution target.

### Run

```bash
python examples/particle_in_cell/hot_plasma_ball_expansion.py \
  --device auto \
  --implementation auto \
  --grid-n 128 \
  --steps 120 \
  --vtk-stride 12 \
  --frame-stride 2 \
  --output-dir examples/particle_in_cell/output_hot_plasma_ball
```

### Outputs

- `run_summary.json`: setup and conservation summary
- `diagnostics.csv`: per-step diagnostics
- `diagnostics.png`: energy and charge-error curves
- `hot_plasma_ball.gif`: z-midplane animation (`Ey`, `Hz`, `rho`)
- `animation_frames/frame_*.png`: per-frame images used for GIF
- `vtk_fields/fields_step_*.vtk`: electric, magnetic, current, charge density
- `vtk_particles/particles_step_*.vtk`: particle positions/velocity/species

### Notes

- `--radius-um 50` corresponds to a 100 um diameter sphere.
- To write all particles to VTK, set `--vtk-particle-max -1`.
- If runtime is too high, reduce `--grid-n`, `--steps`, or increase
  `--vtk-stride` and/or `--frame-stride`.
- To disable animation output: `--skip-animation`.
