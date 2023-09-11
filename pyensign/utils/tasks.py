import asyncio


class WorkerPool:
    """
    WorkerPool manages a pool of async workers to limit the number of coroutines in the
    event loop.
    """

    def __init__(self, max_workers=10, max_queue_size=100):
        self.max_workers = max_workers
        self.workers = []
        self.queue = asyncio.Queue(maxsize=max_queue_size)

    async def schedule(self, coro, done_callback=None):
        """
        Schedule a coroutine to be executed by a worker.
        """
        await self.queue.put((coro, done_callback))
        if len(self.workers) < self.max_workers:
            worker = asyncio.create_task(self._worker())
            self.workers.append(worker)

    async def _worker(self):
        while True:
            try:
                coro, done_callback = self.queue.get_nowait()
                await coro
                if done_callback:
                    done_callback()
            except (asyncio.QueueEmpty, asyncio.CancelledError):
                # If there is nothing to be done or someone cancelled us, stop the
                # worker
                break
        self.workers.remove(asyncio.current_task())

    async def release(self):
        await asyncio.gather(*self.workers)
