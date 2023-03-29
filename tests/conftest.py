import pytest
import asyncio

##########################################################################
# Global pytest options
##########################################################################


def pytest_addoption(parser):
    parser.addoption(
        "--live", action="store_true", default=False, help="run live tests"
    )


# Make sure that async tests are run in their own event loop
@pytest.fixture(scope="session")
def event_loop(request):
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
