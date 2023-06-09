import pytest
import asyncio

##########################################################################
# Global pytest options
##########################################################################


def pytest_addoption(parser):
    parser.addoption(
        "--live", action="store_true", default=False, help="run live tests"
    )


@pytest.fixture()
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
