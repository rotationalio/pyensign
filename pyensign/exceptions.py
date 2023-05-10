import inspect
from grpc import RpcError
from functools import wraps

##########################################################################
## Decorators
##########################################################################

_except = (RpcError, AttributeError)


def _wrap_rpc(fn):
    @wraps(fn)
    def wrap(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except _except as e:
            _handle_client_error(e)

    return wrap


def _wrap_async_rpc(coro):
    @wraps(coro)
    async def wrap(*args, **kwargs):
        try:
            return await coro(*args, **kwargs)
        except _except as e:
            _handle_client_error(e)

    return wrap


def _wrap_generator(fn):
    @wraps(fn)
    def wrap(*args, **kwargs):
        try:
            for rep in fn(*args, **kwargs):
                yield rep
        except _except as e:
            _handle_client_error(e)

    return wrap


def _wrap_async_generator(coro):
    @wraps(coro)
    async def wrap(*args, **kwargs):
        try:
            async for rep in coro(*args, **kwargs):
                yield rep
        except _except as e:
            _handle_client_error(e)

    return wrap


def catch_rpc_error(coro):
    """
    Decorate coroutines to catch and handle gRPC client errors.
    """

    if inspect.isgeneratorfunction(coro):
        return _wrap_generator(coro)
    elif inspect.isasyncgenfunction(coro):
        return _wrap_async_generator(coro)
    elif inspect.iscoroutinefunction(coro):
        return _wrap_async_rpc(coro)
    else:
        return _wrap_rpc(coro)


##########################################################################
## Error Handling
##########################################################################


def _handle_client_error(e):
    """
    Provides error handling for errors raised by the gRPC client.
    """

    if isinstance(e, RpcError):
        raise EnsignRPCError("Received gRPC error from server: {}".format(e)) from e
    elif isinstance(e, AttributeError):
        raise EnsignAttributeError(
            "Error accessing field from response: {}".format(e)
        ) from e
    else:
        raise PyEnsignError("Unknown error: {}".format(e)) from e


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


class CacheMissError(PyEnsignError):
    """
    Raised when PyEnsign fails to find a cached value
    """

    pass


class EnsignError(PyEnsignError):
    """
    Raised when PyEnsign receives an error from the Ensign server
    """

    pass


class EnsignRPCError(EnsignError):
    """
    Raised when PyEnsign receives a gRPC error from the Ensign server
    """

    pass


class EnsignTypeError(EnsignError, TypeError):
    """
    Raised when PyEnsign receives an unexpected type in a response message
    """

    pass


class EnsignAttributeError(EnsignError, AttributeError):
    """
    Raised when PyEnsign encounters an error trying to access a field from a response message
    """

    pass


class EnsignTopicCreateError(EnsignError):
    """
    Raised when Ensign failed to create a topic
    """

    pass


class EnsignTopicNotFoundError(EnsignError):
    """
    Raised when a topic could not be retrieved from Ensign
    """

    pass
