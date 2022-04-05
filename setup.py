from setuptools import setup, find_packages

setup(
    name="market_data",
    version="0.1.6",
    description="Library and CLI for syncing crypto exchange data (candles, etc) to influxdb",
    packages=find_packages(),
    install_requires=["arrow", "boto3", "influxdb", "loguru", "ratelimit", "requests", "tardis-client", "tardis-dev"],
)
