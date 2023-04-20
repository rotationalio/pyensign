import jwt


def expires_at(token):
    """
    Get the expiration unix timestamp of a JWT token.
    """
    return jwt.decode(token, options={"verify_signature": False})["exp"]


def not_before(token):
    """
    Get the not before unix timestamp of a JWT token.
    """
    return jwt.decode(token, options={"verify_signature": False})["nbf"]
