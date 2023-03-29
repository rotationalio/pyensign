import inspect
from grpc import RpcError
from functools import wraps

##########################################################################
## Decorators
##########################################################################

_except = (RpcError, AttributeError)


def wrap_rpc(fn):
    @wraps(fn)
    def wrap(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except _except as e:
            handle_client_error(e)

    return wrap


def wrap_async_rpc(coro):
    @wraps(coro)
    async def wrap(*args, **kwargs):
        try:
            return await coro(*args, **kwargs)
        except _except as e:
            handle_client_error(e)

    return wrap


def wrap_generator(fn):
    @wraps(fn)
    def wrap(*args, **kwargs):
        try:
            for rep in fn(*args, **kwargs):
                yield rep
        except _except as e:
            handle_client_error(e)

    return wrap


def wrap_async_generator(coro):
    @wraps(coro)
    async def wrap(*args, **kwargs):
        try:
            async for rep in coro(*args, **kwargs):
                yield rep
        except _except as e:
            handle_client_error(e)

    return wrap


def catch_rpc_error(coro):
    """
    Decorate coroutines to catch and handle gRPC client errors.
    """

    if inspect.isgeneratorfunction(coro):
        return wrap_generator(coro)
    elif inspect.isasyncgenfunction(coro):
        return wrap_async_generator(coro)
    elif inspect.iscoroutinefunction(coro):
        return wrap_async_rpc(coro)
    else:
        return wrap_rpc(coro)


##########################################################################
## Error Handling
##########################################################################


def handle_client_error(e):
    """
    Provides error handling for errors raised by the gRPC client.
    """

    if isinstance(e, RpcError):
        raise EnsignRPCError(e)
    elif isinstance(e, AttributeError):
        raise EnsignAttributeError(e)
    else:
        raise PyEnsignError(e)


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


class EnsignRPCError(EnsignError):
    """
    Raised when PyEnsign receives a gRPC error from the Ensign server
    """

    pass


class EnsignResponseType(EnsignError):
    """
    Raised when PyEnsign receives an unexpected message type from the Ensign server
    """

    pass


class EnsignAttributeError(EnsignError):
    """
    Raised when PyEnsign encounters an error trying to access a field from a response message
    """

    pass


class EnsignTopicCreateError(EnsignError):
    """
    Raised when Ensign failed to create a topic
    """

    pass
