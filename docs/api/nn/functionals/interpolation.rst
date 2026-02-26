Interpolation Functionals
=========================

By default, ``grid_to_point_interpolation`` and
``point_to_grid_interpolation`` dispatch to the Warp backend when available.
The deprecated ``interpolation`` alias preserves torch-default behavior for
backward compatibility.

.. autofunction:: physicsnemo.nn.functional.grid_to_point_interpolation
.. autofunction:: physicsnemo.nn.functional.point_to_grid_interpolation

.. rubric:: Benchmarks (ASV)

.. rubric:: Forward

.. figure:: /nn/functional/interpolation/grid_to_point_interpolation/benchmark_forward.png
   :alt: Grid-to-point interpolation forward benchmark comparison
   :width: 100%

.. rubric:: Backward

.. figure:: /nn/functional/interpolation/grid_to_point_interpolation/benchmark_backward.png
   :alt: Grid-to-point interpolation backward benchmark comparison
   :width: 100%
