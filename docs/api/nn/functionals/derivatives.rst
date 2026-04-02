Derivative Functionals
======================

.. autofunction:: physicsnemo.nn.functional.uniform_grid_gradient

.. autofunction:: physicsnemo.nn.functional.rectilinear_grid_gradient

.. autofunction:: physicsnemo.nn.functional.mesh_lsq_gradient

.. autofunction:: physicsnemo.nn.functional.mesh_green_gauss_gradient

.. autofunction:: physicsnemo.nn.functional.spectral_grid_gradient

.. autofunction:: physicsnemo.nn.functional.meshless_fd_stencil_points

.. autofunction:: physicsnemo.nn.functional.meshless_fd_derivatives

Meshless Finite-Difference Workflow
-----------------------------------

The meshless finite-difference API is split into two composable steps:

1. Generate canonical stencil locations around each query point with
   :func:`physicsnemo.nn.functional.meshless_fd_stencil_points`.
2. Evaluate your model at those stencil points, then recover first/second
   derivatives with
   :func:`physicsnemo.nn.functional.meshless_fd_derivatives`.
