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


class EnsignResponseType(EnsignError):
    """
    Raised when PyEnsign receives an unexpected message type from the Ensign server
    """

    pass


class EnsignTopicCreateError(EnsignError):
    """
    Raised when Ensign failed to create a topic
    """

    pass
