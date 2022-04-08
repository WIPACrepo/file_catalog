#!/usr/bin/env python
"""Setup."""


from setuptools import setup  # type: ignore[import]

setup(
    # TODO
    # package_data={shop.name: ["data/www/*", "data/www_templates/*", "py.typed"]},
    entry_points={"console_scripts": ["file_catalog=file_catalog.__main__:main"]},
)
