# Finite-Volume Euler Example

This example runs a second-order MUSCL-Hancock finite-volume solve of the
compressible Euler equations on unstructured simplicial meshes. It includes a
2D triangular forward-facing-step case and a 3D extruded tetrahedral version of
the same geometry.

The example uses `physicsnemo.mesh.Mesh` for the static simplicial mesh and the
existing `mesh_green_gauss_gradient` functional for cell-centered gradient
reconstruction. Euler-specific pieces, including primitive/conservative
conversion, boundary states, CFL selection, and the fused MUSCL-Hancock
limiter/reconstruction/Rusanov update are kept in `euler_finite_volume.py`.

The timestep runtime keeps only the Mesh geometry, cell-neighbor ids, and local
boundary tags. Face centroids, normals, areas, cell volumes, and limiter
stencils are computed from that compact cell-local connectivity inside the
operators.

Run with:

```bash
python euler_solver.py
```

The default config writes density plots and final tensors under Hydra's
`outputs_finite_volume_euler/` run directory. It also writes PNG frames and
animated GIFs when `plot_every` and `make_animation` are enabled. Set
`solver.implementation` to `torch` or `warp` to choose the backend for the same
Euler finite-volume algorithm.
