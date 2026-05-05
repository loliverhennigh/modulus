# Particle-In-Cell Functionals Roadmap

This package tracks the PIC functionals needed to reproduce WarpX-like
macroscopic EM + particle workflows in PhysicsNemo.

## Core Particle Loop

- [x] `particle_push_boris` (torch + warp)
- [x] `gather_fields_to_particles` (Yee-aware interpolation)
- [x] `deposit_current_charge_conserving` (order-1 segmented deposition)
- [ ] `deposit_charge_density`

## Species / Source Control

- [ ] `species_activation_toggle`
- [ ] `ionize_species_to_two_products`

## Materials / Geometry Coupling

- [ ] `material_id_from_implicit_geometry`
- [ ] `material_property_lookup_update`

## Diagnostics / Runtime

- [ ] `field_smoothing_filter`
- [ ] `field_probe_diagnostic`
- [ ] `species_density_snapshot_diagnostic`
- [ ] `nt_integral_species`
- [ ] `checkpoint_restart_state`
