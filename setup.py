"""Setup."""


import os
import re
import sys
from typing import List

from setuptools import setup  # type: ignore[import]

HERE = os.path.abspath(os.path.dirname(__file__))
PY_VERSION = (3, 8)
NAME = "file_catalog"
REQUIREMENTS_PATH = os.path.join(HERE, "requirements.txt")
REQUIREMENTS = open(REQUIREMENTS_PATH).read().splitlines()


# Check Python Version -----------------------------------------------------------------
if sys.version_info < PY_VERSION:
    print(
        f"ERROR: {NAME} requires at least Python {PY_VERSION[0]}.{PY_VERSION[1]}+ to run."
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


# Setup --------------------------------------------------------------------------------

setup(
    name=NAME,
    version=_get_version(),
    description="File Catalog",
    long_description=open(os.path.join(HERE, "README.md")).read(),  # include new-lines
    long_description_content_type="text/markdown",
    url="https://github.com/WIPACrepo/file_catalog",
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    keywords="file catalog",
    packages=[NAME, f"{NAME}.schema"],
    install_requires=_get_pypi_requirements(),
    dependency_links=_get_git_requirements(),
    package_data={NAME: ["data/www/*", "data/www_templates/*", "py.typed"]},
    entry_points={"console_scripts": ["file_catalog=file_catalog.__main__:main"]},
)
