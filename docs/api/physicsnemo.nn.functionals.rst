PhysicsNeMo Functionals
=======================

PhysicsNeMo functionals mirror the ``torch.nn.functional`` style: stateless
operations that are easy to compose and benchmark. All functionals are built on
``FunctionSpec`` so multiple backend implementations can share the same public
API while still supporting validation and benchmarking.

.. toctree::
   :maxdepth: 2
   :caption: PhysicsNeMo Functionals
   :name: PhysicsNeMo Functionals

   nn/functionals/neighbors
   nn/functionals/geometry
   nn/functionals/fourier_spectral
   nn/functionals/resampling_interpolation
