import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        metavar="LIVY_URL",
        nargs="?",
        const="http://localhost:8998",
        help="Run integration tests against the specified Livy server URL "
        + "(default: http://localhost:8998)",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--integration") is not None:
        # --integration given in cli: do not skip slow tests
        return
    skip = pytest.mark.skip(reason="Add --integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def integration_url(request):
    return request.config.getoption("--integration")
