Interpolation Functionals
=========================

Grid-To-Point Interpolation
---------------------------

.. note::
   By default, ``grid_to_point_interpolation`` dispatches to the Warp backend.
   The deprecated ``interpolation`` alias preserves torch-default behavior for
   backward compatibility.

.. autofunction:: physicsnemo.nn.functional.grid_to_point_interpolation

.. rubric:: Visualization

This animation shows query points colored by interpolated values over the same
fixed structured grid field.

.. figure:: /nn/functional/interpolation/grid_to_point_interpolation/grid_to_point_queries.gif
   :alt: Grid-to-point interpolation query animation
   :width: 85%

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

.. rubric:: Visualization

The image below shows scattered input point values (left) and the resulting
rasterized grid field (right). The animation shows convergence as more points
are accumulated.

.. figure:: /nn/functional/interpolation/point_to_grid_interpolation/point_to_grid_overview.png
   :alt: Point-to-grid interpolation visualization overview
   :width: 100%

.. figure:: /nn/functional/interpolation/point_to_grid_interpolation/point_to_grid_convergence.gif
   :alt: Point-to-grid interpolation convergence animation
   :width: 75%

.. rubric:: Benchmarks (ASV)

.. rubric:: Forward

.. figure:: /nn/functional/interpolation/point_to_grid_interpolation/benchmark_forward.png
   :alt: Point-to-grid interpolation forward benchmark comparison
   :width: 100%

.. rubric:: Backward

.. figure:: /nn/functional/interpolation/point_to_grid_interpolation/benchmark_backward.png
   :alt: Point-to-grid interpolation backward benchmark comparison
   :width: 100%
