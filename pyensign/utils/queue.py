import asyncio
from datetime import timedelta


class BidiQueue:
    """
    BidiQueue implements an in-memory bidirectional buffer for non-blocking
    communication between coroutines.
    """

    def __init__(self, max_queue_size=100):
        self._request_queue = asyncio.Queue(maxsize=max_queue_size)
        self._response_queue = asyncio.Queue(maxsize=max_queue_size)

    async def write_request(self, message):
        """
        Write a request to the queue, blocks if the queue is full.
        """
        await self._request_queue.put(message)

    async def read_request(self):
        """
        Read a request from the queue, blocks if the queue is empty.
        """
        return await self._request_queue.get()

    async def write_response(self, message):
        """
        Write a response to the queue, blocks if the queue is full.
        """
        await self._response_queue.put(message)

    async def read_response(self):
        """
        Read a response from the queue, blocks if the queue is empty.
        """
        return await self._response_queue.get()

    async def requests(self):
        """
        Return requests in the queue as an async iterator. The iterator is only
        exhausted when the queue is closed. If the queue is empty this method will
        block until there is a request in the queue.
        """
        while True:
            request = await self.read_request()
            if request is None:
                break
            yield request

    async def flush_requests(
        self, timeout=timedelta(seconds=2.0), tick=timedelta(milliseconds=100)
    ):
        """
        Block until the request queue is empty or the timeout is reached.
        """
        while not self._request_queue.empty():
            await asyncio.sleep(tick.total_seconds())
            if timeout is not None:
                timeout -= tick
                if timeout <= timedelta():
                    raise asyncio.TimeoutError(
                        "timeout exceeded before all requests were flushed"
                    )

    async def close(self):
        """
        Close the queue by sending a message to both the request and response queues.
        The main purpose of this is to unblock pending consumers. Otherwise, consumers
        will block indefinitely on read_request() or read_response(). This means that
        consumers should be prepared to handle None responses from reads.
        """
        await self._request_queue.put(None)
        await self._response_queue.put(None)
