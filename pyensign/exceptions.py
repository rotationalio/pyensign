##########################################################################
## PyEnsign Exceptions
##########################################################################


class PyEnsignError(Exception):
    """
    Base class for PyEnsign exceptions
    """

    pass


class AuthenticationError(PyEnsignError):
    """
    Raised when PyEnsign fails to authenticate with the Auth server
    """

    pass


class EnsignError(PyEnsignError):
    """
    Raised when PyEnsign receives an error from the Ensign server
    """

    pass


class EnsignResponseType(PyEnsignError):
    """
    Raised when PyEnsign receives an unexpected message type from the Ensign server
    """

    pass
