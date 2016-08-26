import sys
from setuptools import setup
from os import path

import file_catalog

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md')) as f:
    long_description = f.read()


install_requires = ['tornado>=4.2', 'pymongo>=3.3']
if sys.version_info < (3, 2):
    install_requires.extend(['futures'])

setup(
    name='file_catalog',
    version=file_catalog.__version__,
    description='File catalog',
    long_description=long_description,
    url='https://github.com/dsschult/file_catalog',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    keywords='file catalog',
    packages=['file_catalog'],
    install_requires=install_requires,
    package_data={
        'file_catalog':['data/www/*','data/www_templates/*'],
    },
    entry_points={
        'console_scripts':[
            'file_catalog=file_catalog.__main__:main',
    }
)