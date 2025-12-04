# SPDX-FileCopyrightText: Copyright (c) 2023 - 2025 NVIDIA CORPORATION & AFFILIATES.
# SPDX-FileCopyrightText: All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Utilities for version compatibility checking.

This is used to provide a uniform and consistent way to check for missing
packages, when not all packages are required for the base physicsnemo
install.  Additionally, for some packages (it's not mandatory to do this),
we have a registry of packages -> install tip that is used
to provide a helpful error message.
"""

import functools
from importlib import metadata
from typing import Optional

from packaging.specifiers import SpecifierSet
from packaging.version import Version

install_cmds = {
    "cupy": "pip install cupy-cuda13",
    "cuml": "pip install cuml-cu13",
    "scipy": "pip install scipy",
}

extra_info = {
    "cupy": "For more details about installing cupy, see https://docs.cupy.dev/en/stable/install.html/.",
    "cuml": "For more details about installing cuml, see https://docs.rapids.ai/install/.",
    "scipy": "For more details about installing scipy, see https://www.scipy.org/install/.",
}


def check_min_version(
    distribution_name: str,
    min_version: str,
    *,
    hard_fail: bool = True,
) -> bool:
    """Backwards-compatible helper that checks ``package >= min_version``."""

    return ensure_available(
        distribution_name,
        spec=f">={min_version}",
        hard_fail=hard_fail,
    )


@functools.lru_cache(maxsize=None)
def get_installed_version(distribution_name: str) -> Optional[str]:
    """
    Return the installed version for a given distribution without importing it.
    Uses importlib.metadata to avoid heavy import-time side effects.
    Cached for repeated lookups.
    """
    try:
        return metadata.version(distribution_name)
    except metadata.PackageNotFoundError:
        return None


def check_version_spec(
    distribution_name: str,
    spec: str,
    *,
    error_msg: Optional[str] = None,
    hard_fail: bool = True,
) -> bool:
    """
    Check whether the installed distribution satisfies a PEP 440 version specifier.

    Args:
        distribution_name: Distribution (package) name as installed by pip
        spec: PEP 440 version specifier (e.g., '>=2.4,<2.6')
        error_msg: Optional custom error message
        hard_fail: Whether to raise an ImportError if the version requirement is not met
    Returns:
        True if version requirement is met; False if not and hard_fail=False

    Raises:
        ImportError: If package is not installed or requirement not satisfied (and hard_fail=True)
    """
    installed = get_installed_version(distribution_name)
    if installed is None:
        if hard_fail:
            raise ImportError(
                f"Package '{distribution_name}' is required but not installed."
            )
        else:
            return False

    ok = Version(installed) in SpecifierSet(spec)
    if not ok:
        msg = (
            error_msg
            or f"{distribution_name} {spec} is required, but found {installed}"
        )
        if hard_fail:
            raise ImportError(msg)
        return False

    return True


def require_version_spec(package_name: str, spec: str = ">=0.0.0"):
    """
    Decorator variant that accepts a full PEP 440 specifier instead of a single minimum version.

    Args:
        package_name: Name of the package to check
        spec: PEP 440 version specifier (e.g., '>=2.4,<2.6')

    Returns:
        Decorator function that checks version requirement before execution

    Example:
        @require_version("torch", "2.3")
        def my_function():
            # This function will only execute if torch >= 2.3
            pass
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            check_version_spec(package_name, spec, hard_fail=True)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def ensure_available(
    distribution_name: str,
    spec: str = ">=0.0.0",
    *,
    install_hint: Optional[str] = None,
    extra_message: Optional[str] = None,
    hard_fail: bool = True,
) -> bool:
    """
    Ensure a distribution is installed and satisfies the given specifier.
    If not satisfied:
      - When hard_fail=True, raises ImportError with an actionable message
      - When hard_fail=False, returns False

    Args:
        distribution_name: Distribution (package) name as installed by pip
        spec: PEP 440 specifier (e.g., '>=24.0.0', '>=2.4,<2.6')
        install_hint: Optional string suggesting how to install (e.g., "pip install cupy-cuda12x")
        extra_message: Optional extra context to append to the error
        hard_fail: Whether to raise on failure
    """
    try:
        return check_version_spec(distribution_name, spec, hard_fail=True)
    except ImportError as e:
        if not hard_fail:
            return False
        msg_parts = [str(e)]
        # If not provided, and we have a hint above, use it:
        if install_hint is None and distribution_name in install_cmds:
            install_hint = install_cmds[distribution_name]
        # If not provided, and we have extra info above, use it:
        if extra_message is None and distribution_name in extra_message:
            extra_message = extra_info[distribution_name]

        if install_hint:
            msg_parts.append(f"Install hint: {install_hint}")
        if extra_message:
            msg_parts.append(extra_message)
        raise ImportError(" | ".join(msg_parts))
