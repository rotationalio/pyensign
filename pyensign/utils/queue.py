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
        self._done = asyncio.Event()
        self._closed = asyncio.Event()
        self._pending_request = None

    async def write_request(self, message):
        """
        Write a request to the queue, blocks if the queue is full. Consumers must call
        request_done() when they are done processing the request.
        """
        if self._closed.is_set():
            raise asyncio.InvalidStateError("cannot write request to a closed queue")
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

    def request_done(self):
        """
        Notify the queue that a request has been processed.
        """
        self._request_queue.task_done()

    async def _wait_until_done(self):
        """
        Wait for the queue to be in the done state.
        """
        await self._done.wait()

    async def _wait_until_closed(self):
        """
        Wait for the queue to be in the closed state.
        """
        await self._closed.wait()

    async def requests(self):
        """
        Return requests in the queue as an async iterator. The iterator is only
        exhausted when the queue goes into the done state. Consumers must call
        request_done() when they are done processing the request.
        """

        while True:
            # Yield the pending request if there is one
            if self._pending_request:
                req = self._pending_request
                self._pending_request = None
                yield req

            # Wait for a request from the queue or for the queue to be done
            done, pending = await asyncio.wait(
                [
                    asyncio.ensure_future(self._request_queue.get()),
                    asyncio.ensure_future(self._wait_until_done()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Cancel any pending tasks
            for task in pending:
                task.cancel()

            # Retrieve the request future result if available
            result = None
            for task in done:
                if task.result():
                    if result:
                        raise RuntimeError("expected only one future result")
                    result = task.result()

            if self._done.is_set():
                # If the queue is done we should not yield any more requests, so
                # the future may have to be saved for the next iteration
                if result:
                    self._pending_request = result
                break

            # If the queue is not done we should have a request to yield
            if not result:
                raise RuntimeError("expected a future result")

            yield result

    async def responses(self):
        """
        Return responses in the queue as an async iterator. The iterator is only
        exhausted when the queue is closed.
        """

        while not self._closed.is_set():
            # Wait for a response from the queue or for the queue to be closed
            done, pending = await asyncio.wait(
                [
                    asyncio.ensure_future(self._response_queue.get()),
                    asyncio.ensure_future(self._wait_until_closed()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Cancel any pending tasks
            for task in pending:
                task.cancel()

            # Retrieve the response future result if available
            for task in done:
                if task.result():
                    yield task.result()

    async def flush_requests(self, timeout=timedelta(seconds=2.0)):
        """
        Block until all requests have been processed or the timeout has expired.
        """

        await asyncio.wait_for(self._request_queue.join(), timeout.total_seconds())

    def discard_requests(self):
        """
        Discard all pending requests in the queue. This is usually called on shutdown.
        """

        if self._pending_request:
            self._request_queue.task_done()
            self._pending_request = None

        while not self._request_queue.empty():
            self._request_queue.get_nowait()
            self._request_queue.task_done()

    def ready(self):
        """
        Set the queue to the ready state making it readable. This should
        be set prior to writing any requests to the queue.
        """

        self._done.clear()

    def done_writing(self):
        """
        Set the queue to the done state to wake up consumers. This should be set when
        no more requests should be written to the queue.
        """

        self._done.set()

    async def close(self):
        """
        Close the queue, ensuring that all pending requests are processed. After
        close() is called, no more requests can be written to the queue.
        """

        self._closed.set()
        await self._request_queue.join()
