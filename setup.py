#!/usr/bin/env python
"""Setup."""


import os
import subprocess

from setuptools import setup  # type: ignore[import]

subprocess.run(
    "pip install git+https://github.com/WIPACrepo/wipac-dev-tools.git".split(),
    check=True,
)
from wipac_dev_tools import SetupShop  # noqa: E402  # pylint: disable=C0413

shop = SetupShop(
    "file_catalog",
    os.path.abspath(os.path.dirname(__file__)),
    ((3, 6), (3, 8)),
    "File Catalog",
)

setup(
    url="https://github.com/WIPACrepo/file_catalog",
    package_data={shop.name: ["data/www/*", "data/www_templates/*", "py.typed"]},
    **shop.get_kwargs(subpackages=["schema"]),
    entry_points={"console_scripts": ["file_catalog=file_catalog.__main__:main"]},
)
