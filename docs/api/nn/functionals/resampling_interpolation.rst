Interpolation Functionals
=========================

Grid-To-Point Interpolation
---------------------------

.. note::
   By default, ``grid_to_point_interpolation`` dispatches to the Warp backend
   when available. The deprecated ``interpolation`` alias preserves the
   historical torch-default behavior for backward compatibility.

.. autofunction:: physicsnemo.nn.functional.grid_to_point_interpolation

.. rubric:: Visualization

.. figure:: /nn/functional/interpolation/grid_to_point_interpolation/grid_to_point_queries.gif
   :alt: Grid-to-point interpolation query animation
   :width: 85%

Point-To-Grid Interpolation
---------------------------

.. autofunction:: physicsnemo.nn.functional.point_to_grid_interpolation

.. rubric:: Visualization

.. figure:: /nn/functional/interpolation/point_to_grid_interpolation/point_to_grid_convergence.gif
   :alt: Point-to-grid interpolation convergence animation
   :width: 75%

Legacy Interpolation Alias
--------------------------

.. autofunction:: physicsnemo.nn.functional.interpolation

.. rubric:: Benchmarks (ASV)

.. figure:: /img/nn/functional/interpolation/benchmark.png
   :alt: Interpolation benchmark comparison
   :width: 100%
