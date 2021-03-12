"""Setup."""


import os
import re
import sys
from typing import List, Tuple

from setuptools import setup  # type: ignore[import]

HERE = os.path.abspath(os.path.dirname(__file__))
OLDEST_PY_VERSION: Tuple[int, int] = (3, 6)
PY_VERSION: Tuple[int, int] = (3, 8)
NAME = "file_catalog"
REQUIREMENTS_PATH = os.path.join(HERE, "requirements.txt")
REQUIREMENTS = open(REQUIREMENTS_PATH).read().splitlines()


# Check Python Version -----------------------------------------------------------------
if sys.version_info < OLDEST_PY_VERSION:
    print(
        f"ERROR: {NAME} requires at least Python {OLDEST_PY_VERSION[0]}.{OLDEST_PY_VERSION[1]}+ to run "
        f"( {sys.version_info} < {OLDEST_PY_VERSION} )"
    )
    sys.exit(1)


# Helper Utilities ---------------------------------------------------------------------


def _get_version() -> str:
    with open(os.path.join(HERE, NAME, "__init__.py")) as init_f:
        for line in init_f.readlines():
            if "__version__" in line:
                # grab "X.Y.Z" from "__version__ = 'X.Y.Z'" (quote-style insensitive)
                return line.replace('"', "'").split("=")[-1].split("'")[1]
    raise Exception("cannot find __version__")


def _get_pypi_requirements() -> List[str]:
    return [m.replace("==", ">=") for m in REQUIREMENTS if "git+" not in m]


def _get_git_requirements() -> List[str]:
    def valid(req: str) -> bool:
        pat = r"^git\+https://github\.com/\w+/\w+@(v)?\d+\.\d+\.\d+#egg=\w+$"
        if not re.match(pat, req):
            raise Exception(
                f"from {REQUIREMENTS_PATH}: "
                f"pip-install git-package url is not in standardized format {pat} ({req})"
            )
        return True

    return [m.replace("git+", "") for m in REQUIREMENTS if "git+" in m and valid(m)]


def _python_lang_classifiers() -> List[str]:
    """NOTE: Will still work even after the '3.9 -> 4.0'-transition."""

    def int_10x_it(py_version: Tuple[int, int]) -> int:
        return int(f"{py_version[0]}{py_version[1]}")

    return [
        f"Programming Language :: Python :: {r/10}"
        for r in range(int_10x_it(OLDEST_PY_VERSION), int_10x_it(PY_VERSION) + 1)
    ]


def _development_status() -> str:
    # "Development Status :: 1 - Planning",
    # "Development Status :: 2 - Pre-Alpha",
    # "Development Status :: 3 - Alpha",
    # "Development Status :: 4 - Beta",
    # "Development Status :: 5 - Production/Stable",
    # "Development Status :: 6 - Mature",
    # "Development Status :: 7 - Inactive",
    version = _get_version()
    if version.startswith("0.0.0"):
        return "Development Status :: 2 - Pre-Alpha"
    elif version.startswith("0.0."):
        return "Development Status :: 3 - Alpha"
    elif version.startswith("0."):
        return "Development Status :: 4 - Beta"
    elif int(version.split(".")[0]) >= 1:
        return "Development Status :: 5 - Production/Stable"
    else:
        raise Exception(f"Could not figure Development Status for version: {version}")


# Setup --------------------------------------------------------------------------------

setup(
    name=NAME,
    version=_get_version(),
    description="File Catalog",
    long_description=open(os.path.join(HERE, "README.md")).read(),  # include new-lines
    long_description_content_type="text/markdown",
    url="https://github.com/WIPACrepo/file_catalog",
    license="MIT",
    classifiers=sorted(
        _python_lang_classifiers()
        + [_development_status()]
        + ["License :: OSI Approved :: MIT License"]
    ),
    keywords="file catalog",
    packages=[NAME, f"{NAME}.schema"],
    install_requires=_get_pypi_requirements(),
    dependency_links=_get_git_requirements(),
    package_data={NAME: ["data/www/*", "data/www_templates/*", "py.typed"]},
    entry_points={"console_scripts": ["file_catalog=file_catalog.__main__:main"]},
)
