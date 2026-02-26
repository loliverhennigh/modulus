Interpolation Functionals
=========================

By default, ``grid_to_point_interpolation`` and
``point_to_grid_interpolation`` dispatch to the Warp backend when available.
The deprecated ``interpolation`` alias preserves torch-default behavior for
backward compatibility.

Grid-To-Point Interpolation
---------------------------

.. autofunction:: physicsnemo.nn.functional.grid_to_point_interpolation

.. rubric:: Benchmarks (ASV)

.. rubric:: Forward

.. figure:: /nn/functional/interpolation/grid_to_point_interpolation/benchmark_forward.png
   :alt: Grid-to-point interpolation forward benchmark comparison
   :width: 100%

.. rubric:: Backward

.. figure:: /nn/functional/interpolation/grid_to_point_interpolation/benchmark_backward.png
   :alt: Grid-to-point interpolation backward benchmark comparison
   :width: 100%

Point-To-Grid Interpolation
---------------------------

.. autofunction:: physicsnemo.nn.functional.point_to_grid_interpolation

.. rubric:: Benchmarks (ASV)

.. rubric:: Forward

.. figure:: /nn/functional/interpolation/point_to_grid_interpolation/benchmark_forward.png
   :alt: Point-to-grid interpolation forward benchmark comparison
   :width: 100%

.. rubric:: Backward

.. figure:: /nn/functional/interpolation/point_to_grid_interpolation/benchmark_backward.png
   :alt: Point-to-grid interpolation backward benchmark comparison
   :width: 100%
