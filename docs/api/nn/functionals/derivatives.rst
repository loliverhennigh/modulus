Derivative Functionals
======================

.. autofunction:: physicsnemo.nn.functional.uniform_grid_gradient

.. autofunction:: physicsnemo.nn.functional.rectilinear_grid_gradient

.. autofunction:: physicsnemo.nn.functional.mesh_lsq_gradient

.. autofunction:: physicsnemo.nn.functional.mesh_green_gauss_gradient

.. autofunction:: physicsnemo.nn.functional.spectral_grid_gradient

.. autofunction:: physicsnemo.nn.functional.meshless_fd_derivatives

Meshless Finite-Difference Workflow
-----------------------------------

The meshless finite-difference derivative functional expects model values
sampled on a canonical local ``{-1, 0, 1}`` stencil around each query point.
Given these stencil values, it returns first-order or second-order derivatives
(including optional mixed second derivatives).
