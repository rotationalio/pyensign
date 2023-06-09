##########################################################################
# Global pytest options
##########################################################################


def pytest_addoption(parser):
    parser.addoption(
        "--live", action="store_true", default=False, help="run live tests"
    )
