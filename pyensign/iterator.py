import logging

import grpc


class ResponseIterator:
    """
    ResponseIterator is an asynchronous iterator that reads responses from a gRPC
    stream.
    """

    def __init__(self, stream):
        self.stream = stream
        pass

    async def consume(self):
        async for _ in self:
            pass

    async def __aiter__(self):
        while True:
            try:
                rep = await self.stream.read()
            except grpc.aio.AioRpcError as e:
                logging.warning(
                    f"gRPC error occurred while reading from the stream: {e}"
                )
                break
            except StopAsyncIteration:
                logging.debug("gRPC stream was closed")
                break
            # Handle unexpected end of stream
            if rep is grpc.aio.EOF:
                break
            yield rep
