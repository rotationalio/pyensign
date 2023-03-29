import pytest
import asyncio
from grpc import RpcError

from pyensign.exceptions import EnsignRPCError, EnsignAttributeError
from pyensign.exceptions import catch_rpc_error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception, expected, match",
    [
        (RpcError("error"), EnsignRPCError, "error"),
        (AttributeError("error"), EnsignAttributeError, "error"),
    ],
)
async def test_error_decorator(exception, expected, match):
    @catch_rpc_error
    def fn():
        raise exception

    with pytest.raises(expected, match=match):
        fn()

    @catch_rpc_error
    def fn_gen():
        yield 1
        raise exception

    with pytest.raises(expected, match=match):
        for _ in fn_gen():
            pass

    @catch_rpc_error
    async def coro():
        raise exception

    with pytest.raises(expected, match=match):
        await coro()

    @catch_rpc_error
    async def coro_gen():
        yield 1
        raise exception

    with pytest.raises(expected, match=match):
        async for _ in coro_gen():
            pass
