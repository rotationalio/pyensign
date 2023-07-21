import pytest
import asyncio

##########################################################################
# Global pytest options
##########################################################################


def pytest_addoption(parser):
    parser.addoption(
        "--live", action="store_true", default=False, help="run live tests"
    )
    parser.addoption(
        "--creds",
        action="store",
        default="",
        help="path to credentials file for live tests",
    )
