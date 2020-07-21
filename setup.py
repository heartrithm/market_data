#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="candles-sync",
    version="0.0.1",
    description="Library and CLI for syncing crypto exchange candles to influxdb",
    packages=find_packages(),
    install_requires=["requests"],
)
