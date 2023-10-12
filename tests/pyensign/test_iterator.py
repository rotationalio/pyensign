import grpc
import pytest
from asyncmock import AsyncMock

from pyensign.iterator import ResponseIterator


class TestResponseIterator:
    """
    Tests for the ResponseIterator class.
    """

    @pytest.mark.asyncio
    async def test_iter(self):
        """
        Should be able to asynchronously iterate over the class.
        """
        responses = ["response1", "response2", grpc.aio.EOF]
        stream = AsyncMock(spec=grpc.aio.StreamStreamCall)
        stream.read.side_effect = responses

        # Should iterate until the stream is closed
        i = 0
        async for response in ResponseIterator(stream):
            assert response == responses[i]
            i += 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exception",
        [
            (grpc.aio.AioRpcError(None, None, None)),
        ],
    )
    async def test_iter_exception(self, exception):
        """
        Iteration should stop on exceptions.
        """
        stream = AsyncMock(spec=grpc.aio.StreamStreamCall)
        stream.read.side_effect = exception
        async for _ in ResponseIterator(stream):
            assert False, "Should not yield on exception"
