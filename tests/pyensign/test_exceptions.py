import pytest
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
