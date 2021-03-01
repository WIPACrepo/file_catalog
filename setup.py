"""Setup."""

# fmt:off
# pylint: skip-file

from os import path

from setuptools import setup  # type: ignore[import]

# local imports
import file_catalog

here = path.abspath(path.dirname(__file__))

long_description = open(path.join(here, 'README.md')).read()
install_requires = [
    m.strip().replace('==', '>=') for m in open(path.join(here, 'requirements.txt'))
]

setup(
    name='file_catalog',
    version=file_catalog.__version__,
    description='File catalog',
    long_description=long_description,
    url='https://github.com/WIPACrepo/file_catalog',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='file catalog',
    packages=['file_catalog'],
    install_requires=install_requires,
    package_data={
        'file_catalog': ['data/www/*', 'data/www_templates/*', 'py.typed'],
    },
    entry_points={
        'console_scripts':[
            'file_catalog=file_catalog.__main__:main',]
    }
)
