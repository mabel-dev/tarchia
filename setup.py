import os
import platform

from Cython.Build import cythonize
from setuptools import Extension
from setuptools import find_packages
from setuptools import setup

LIBRARY = "tarchia"

__author__ = "notset"
__version__ = "0.0.0"
with open(f"{LIBRARY}/__version__.py", mode="r") as v:
    vers = v.read()
# xec(vers)  # nosec


def is_mac():  # pragma: no cover
    return platform.system().lower() == "darwin"


def is_win():  # pragma: no cover
    os.name == "nt"


if is_mac():
    COMPILE_FLAGS = ["-O3"]
elif is_win():
    COMPILE_FLAGS = ["/O2"]
else:
    COMPILE_FLAGS = ["-O3", "-march=native"]


with open("README.md", "r") as rm:
    long_description = rm.read()

try:
    with open("requirements.txt", "r") as f:
        required = f.read().splitlines()
except:
    with open("tarchia.egg-info/requires.txt", "r") as f:
        required = f.read().splitlines()

extensions = []

setup_config = {
    "name": LIBRARY,
    "version": __version__,
    "description": "Tarchia - Metadata Catalog",
    "long_description": long_description,
    "long_description_content_type": "text/markdown",
    "maintainer": "@joocer",
    "author": __author__,
    "author_email": "justin.joyce@joocer.com",
    "packages": find_packages(include=[LIBRARY, f"{LIBRARY}.*"]),
    "python_requires": ">=3.9",
    "url": "https://github.com/mabel-dev/{LIBRARY}/",
    "install_requires": required,
    "ext_modules": cythonize(extensions),
    "package_data": {
        "": ["*.pyx", "*.pxd"],
    },
}

setup(**setup_config)
