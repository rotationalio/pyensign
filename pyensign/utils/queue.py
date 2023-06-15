import asyncio


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

    async def close(self):
        """
        Close the queue by sending a message to both the request and response queues.
        The main purpose of this is to unblock pending consumers. Otherwise, consumers
        will block indefinitely on read_request() or read_response(). This means that
        consumers should be prepared to handle None responses from reads.
        """
        await self._request_queue.put(None)
        await self._response_queue.put(None)
