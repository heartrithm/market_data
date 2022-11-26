from setuptools import find_packages, setup

setup(
    name="market_data",
    version="0.2.1",
    description="Library and CLI for syncing crypto exchange data (candles, etc) to influxdb",
    packages=find_packages(),
    install_requires=[
        "arrow",
        "boto3",
        "influxdb",
        "loguru",
        "ratelimit",
        "requests",
        "tardis-client",
        "tardis-dev",
    ],
)
