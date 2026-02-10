PhysicsNeMo Functionals
=======================

PhysicsNeMo functionals mirror the ``torch.nn.functional`` style: stateless
operations that are easy to compose in model code and training loops. Many
functionals are optimized for NVIDIA GPU computing and are designed to use accelerated
implementations when available. Some functionals provide multiple
implementations with preferred usage settings; if a preferred implementation is
unavailable, dispatch falls back to another supported option and emits a
fallback warning.

.. toctree::
   :maxdepth: 2
   :caption: PhysicsNeMo Functionals
   :name: PhysicsNeMo Functionals

   nn/functionals/neighbors
   nn/functionals/geometry
   nn/functionals/fourier_spectral
   nn/functionals/resampling_interpolation
