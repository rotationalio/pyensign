import grpc
import asyncio

from pyensign.exceptions import EnsignTypeError


class RequestIterator:
    """
    RequestIterator is an asynchronous iterator that yields requests from an internal
    client stream after sending an initial handshaking request. This iterator can be
    used directly as the request iterator for the publish or subscribe RPCs.
    """

    def __init__(self, queue, init_request):
        self.queue = queue
        self.init_request = init_request

    async def __aiter__(self):
        # Send the initial request to the server
        yield self.init_request

        # Publish events from the client until closed
        # When this iterator is done, the gRPC stream will also be closed
        while True:
            req = await self.queue.read_request()
            if req is None:
                break
            yield req


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
            except grpc.aio.AioRpcError:
                break
            except asyncio.CancelledError:
                # If the channel is closed, gRPC cancels the task
                break
            # Handle unexpected end of stream
            if rep is grpc.aio.EOF:
                break
            yield rep


class PublishResponseIterator(ResponseIterator):
    """
    PublishResponseIterator is an asynchronous iterator that reads responses from a
    gRPC publish stream and executes user-defined callbacks for acks and nacks.
    """

    def __init__(self, stream, pending, on_ack=None, on_nack=None):
        """
        Parameters
        ----------
        stream : grpc.aio.StreamStreamCall
            The gRPC stream object which has been opened and is ready to be read from.
        pending : dict
            A dictionary of pending local ULIDs to events that have been published but
            not yet acked or nacked.
        """
        self.stream = stream
        self.pending = pending
        self.on_ack = on_ack
        self.on_nack = on_nack

    async def consume(self):
        """
        Consume all responses from the stream until closed.
        """
        async for rep in self:
            # Handle messages from the server
            rep_type = rep.WhichOneof("embed")
            if rep_type == "ack":
                event = self.pending.pop(rep.ack.id, None)
                if event:
                    event.mark_acked(rep.ack)
                    if self.on_ack:
                        await self.on_ack(rep.ack)
            elif rep_type == "nack":
                event = self.pending.pop(rep.nack.id, None)
                if event:
                    event.mark_nacked(rep.nack)
                    if self.on_nack:
                        await self.on_nack(rep.nack)
            elif rep_type == "close_stream":
                break
            else:
                raise EnsignTypeError(f"unexpected response type: {rep_type}")


class SubscribeResponseIterator(ResponseIterator):
    """
    SubscribeResponseIterator is an asynchronous iterator that reads responses from a
    gRPC subscribe stream and writes events to a client queue.
    """

    def __init__(self, stream, queue):
        self.stream = stream
        self.queue = queue

    async def consume(self):
        """
        Consume all responses from the stream until closed.
        """
        async for rep in self:
            rep_type = rep.WhichOneof("embed")
            if rep_type == "event":
                await self.queue.write_response(rep.event)
            elif rep_type == "close_stream":
                break
            else:
                await self.queue.write_response(
                    EnsignTypeError(f"unexpected response type: {rep_type}")
                )
                break
