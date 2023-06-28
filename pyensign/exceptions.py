import inspect
from grpc import RpcError
from functools import wraps

##########################################################################
## Decorators
##########################################################################


def _wrap_rpc(fn):
    @wraps(fn)
    def wrap(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            _handle_client_error(e)

    return wrap


def _wrap_async_rpc(coro):
    @wraps(coro)
    async def wrap(*args, **kwargs):
        try:
            return await coro(*args, **kwargs)
        except Exception as e:
            _handle_client_error(e)

    return wrap


def _wrap_generator(fn):
    @wraps(fn)
    def wrap(*args, **kwargs):
        try:
            for rep in fn(*args, **kwargs):
                yield rep
        except Exception as e:
            _handle_client_error(e)

    return wrap


def _wrap_async_generator(coro):
    @wraps(coro)
    async def wrap(*args, **kwargs):
        try:
            async for rep in coro(*args, **kwargs):
                yield rep
        except Exception as e:
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
        code = e.code()
        raise EnsignRPCError(code.name, code.value, e.details()) from e
    elif isinstance(e, AttributeError):
        raise EnsignAttributeError(
            "error accessing field from Ensign response: {}".format(e)
        ) from e
    elif isinstance(e, EnsignTopicNotFoundError):
        raise UnknownTopicError(e.topic) from e
    elif isinstance(e, EnsignTypeError):
        raise EnsignTypeError("unexpected type in Ensign response: {}".format(e)) from e
    elif isinstance(e, EnsignInitError):
        raise EnsignInitError(
            "error processing request: client is not connected"
        ) from e
    elif isinstance(e, EnsignTimeoutError):
        raise EnsignTimeoutError(
            "timeout exceeded while connecting to Ensign: {}".format(e)
        ) from e
    elif isinstance(e, EnsignClientClosingError):
        raise EnsignClientClosingError(
            "error processing request: client is already closing"
        ) from e
    else:
        raise PyEnsignError("unknown error: {}".format(e)) from e


##########################################################################
## PyEnsign Exceptions
##########################################################################


class TopicNotFoundError(Exception):
    """
    Raised when a topic is not found
    """

    def __init__(self, topic):
        self.topic = topic

    def __str__(self):
        return "topic not found: {}".format(self.topic)

    pass


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


class UnknownTopicError(PyEnsignError, TopicNotFoundError):
    """
    Raised when PyEnsign fails to parse a topic
    """

    def __str__(self):
        return "unknown topic: {}, please specify the name or ID of a topic in your project".format(
            self.topic
        )

    pass


class AckError(PyEnsignError):
    """
    Raised when PyEnsign could not ack an event
    """

    pass


class NackError(PyEnsignError):
    """
    Raised when PyEnsign could not nack an event
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

    def __init__(self, name, code, details):
        self.name = name
        self.code = code
        self.details = details

    def __str__(self):
        return "received gRPC error from Ensign: {}: {} ({})".format(
            self.name, self.details, self.code
        )

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


class EnsignInvalidTopicError(EnsignError, ValueError):
    """
    Raised when a topic does not have all required fields
    """

    pass


class EnsignTopicNotFoundError(EnsignError, TopicNotFoundError):
    """
    Raised when a topic could not be retrieved from Ensign
    """

    def __init__(self, topic):
        self.topic = topic

    def __str__(self):
        return "topic not found: {}".format(self.topic)

    pass


class EnsignInitError(EnsignError):
    """
    Raised when the Ensign client is not initialized
    """

    pass


class EnsignTimeoutError(EnsignError):
    """
    Raised when the Ensign client times out waiting for a response from the server
    """

    pass


class EnsignClientClosingError(EnsignError):
    """
    Raised when the Ensign client is closing but a request is made
    """

    pass
