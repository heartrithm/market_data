import utils


def pytest_configure(config):
    utils._called_from_test = True
