#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
setup.py for jartic2geojson package
"""

from setuptools import setup, find_packages

setup(
    name="jartic2geojson",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.0.0",
        "geopandas>=0.9.0",
        "numpy>=1.19.0",
        "scipy>=1.6.0",
        "shapely>=1.7.0",
        "fiona>=1.8.0",
        "psutil>=5.8.0",
    ],
    entry_points={
        'console_scripts': [
            'jartic2geojson=jartic2geojson.cli:main',
            'repair_geometries=jartic2geojson.cli:repair_geometries_entry',
        ],
    },
)
