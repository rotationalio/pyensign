import jwt
import json
import pytest

from pyensign.auth.tokens import expires_at, not_before


# Load the token fixtures
@pytest.fixture
def tokens():
    with open("tests/fixtures/tokens.json") as f:
        return json.load(f)


def test_expires_at(tokens):
    """
    Test parsing the expiration time from a JWT token.
    """
    assert expires_at(tokens["access_token"]) == 1680615330


def test_expires_at_invalid():
    with pytest.raises(jwt.exceptions.DecodeError):
        expires_at("invalid_token")


def test_not_before(tokens):
    """
    Test parsing the not before time from a JWT token.
    """
    assert not_before(tokens["refresh_token"]) == 1680614430


def test_not_before_invalid():
    with pytest.raises(jwt.exceptions.DecodeError):
        not_before("invalid_token")
