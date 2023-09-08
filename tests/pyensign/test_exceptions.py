import re

import pytest
from grpc import StatusCode
from grpc.aio import AioRpcError

from pyensign.exceptions import (
    EnsignRPCError,
    EnsignTopicNotFoundError,
    EnsignTypeError,
    EnsignInitError,
    EnsignClientClosingError,
    EnsignAttributeError,
    EnsignTimeoutError,
    PyEnsignError,
    UnknownTopicError,
)
from pyensign.exceptions import catch_rpc_error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception, expected, match",
    [
        (AttributeError("error"), EnsignAttributeError, "error"),
        (
            EnsignTopicNotFoundError("topic"),
            UnknownTopicError,
            "unknown topic: topic, please specify the name or ID of a topic in your project",
        ),
        (EnsignTypeError("error"), EnsignTypeError, "error"),
        (EnsignInitError("error"), EnsignInitError, "error"),
        (EnsignTimeoutError("error"), EnsignTimeoutError, "error"),
        (EnsignClientClosingError("error"), EnsignClientClosingError, "error"),
        (ZeroDivisionError("error"), PyEnsignError, "error"),
    ],
)
async def test_error_decorator(exception, expected, match):
    @catch_rpc_error
    def fn():
        raise exception

    with pytest.raises(expected, match=match) as exc_info:
        fn()
    assert len(exc_info.traceback) == 3

    @catch_rpc_error
    def fn_gen():
        yield 1
        raise exception

    with pytest.raises(expected, match=match) as exc_info:
        for _ in fn_gen():
            pass
    assert len(exc_info.traceback) == 3

    @catch_rpc_error
    async def coro():
        raise exception

    with pytest.raises(expected, match=match) as exc_info:
        await coro()
    assert len(exc_info.traceback) == 3

    @catch_rpc_error
    async def coro_gen():
        yield 1
        raise exception

    with pytest.raises(expected, match=match) as exc_info:
        async for _ in coro_gen():
            pass
    assert len(exc_info.traceback) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code, details",
    [
        (StatusCode.UNAUTHENTICATED, "unauthenticated"),
        (StatusCode.PERMISSION_DENIED, "permission denied"),
        (StatusCode.NOT_FOUND, "not found"),
    ],
)
async def test_rpc_meta(code, details):
    @catch_rpc_error
    def fn():
        raise AioRpcError(code, None, None, details)

    match = re.escape(EnsignRPCError(code.name, code.value, details).__str__())
    with pytest.raises(EnsignRPCError, match=match) as exc_info:
        fn()
    assert len(exc_info.traceback) == 3
