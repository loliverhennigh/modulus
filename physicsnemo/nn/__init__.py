"""Neural network building blocks for PhysicsNeMo."""

from physicsnemo.core import Module
# from physicsnemo.nn.module import FiniteDifferenceNd
from physicsnemo.nn.module.finite_difference import FiniteDifferenceNd

__all__ = ["Module", "FiniteDifferenceNd"]
