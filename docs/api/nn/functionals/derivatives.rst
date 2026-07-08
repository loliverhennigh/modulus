Derivative Functionals
======================

The figures compare representative inputs (left) with functional outputs
(right). The mesh examples use the PyTorch implementation on the same
deterministic irregular triangular mesh.

.. contents:: On this page
   :local:
   :depth: 1

Gradient
--------

.. autofunction:: physicsnemo.nn.functional.uniform_grid_gradient

.. figure:: /img/nn/functional/derivatives/uniform_grid_gradient.png
   :alt: Uniform grid gradient example
   :width: 100%

.. autofunction:: physicsnemo.nn.functional.rectilinear_grid_gradient

.. figure:: /img/nn/functional/derivatives/rectilinear_grid_gradient.png
   :alt: Rectilinear grid gradient example
   :width: 100%

.. autofunction:: physicsnemo.nn.functional.mesh_lsq_gradient

.. figure:: /img/nn/functional/derivatives/mesh_lsq_gradient.png
   :alt: Mesh LSQ gradient example
   :width: 100%

.. autofunction:: physicsnemo.nn.functional.mesh_green_gauss_gradient

.. figure:: /img/nn/functional/derivatives/mesh_green_gauss_gradient.png
   :alt: Mesh Green-Gauss gradient example
   :width: 100%

.. autofunction:: physicsnemo.nn.functional.spectral_grid_gradient

.. figure:: /img/nn/functional/derivatives/spectral_grid_gradient.png
   :alt: Spectral grid gradient example
   :width: 100%

Mesh Divergence
---------------

.. autofunction:: physicsnemo.nn.functional.mesh_lsq_divergence

.. figure:: /img/nn/functional/derivatives/mesh_lsq_divergence.png
   :alt: LSQ mesh divergence of a vector field on an irregular triangular mesh
   :width: 100%

.. autofunction:: physicsnemo.nn.functional.mesh_cotan_divergence

.. figure:: /img/nn/functional/derivatives/mesh_cotan_divergence.png
   :alt: Cotangent DEC divergence of a vector field on an irregular triangular mesh
   :width: 100%

Mesh Curl
---------

.. autofunction:: physicsnemo.nn.functional.mesh_lsq_curl

.. figure:: /img/nn/functional/derivatives/mesh_lsq_curl.png
   :alt: LSQ curl of a vortex field on an irregular triangular mesh
   :width: 100%

Mesh Laplacian
--------------

.. autofunction:: physicsnemo.nn.functional.mesh_lsq_laplacian

.. figure:: /img/nn/functional/derivatives/mesh_lsq_laplacian.png
   :alt: Double-LSQ Laplacian of a scalar field on an irregular triangular mesh
   :width: 100%

.. autofunction:: physicsnemo.nn.functional.mesh_cotan_laplacian

.. figure:: /img/nn/functional/derivatives/mesh_cotan_laplacian.png
   :alt: Cotangent Laplacian of a scalar field on an irregular triangular mesh
   :width: 100%

Other Derivatives
-----------------

.. autofunction:: physicsnemo.nn.functional.meshless_fd_derivatives
