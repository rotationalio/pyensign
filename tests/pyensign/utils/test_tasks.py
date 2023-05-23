import pytest

from pyensign.utils.tasks import WorkerPool


class TestWorkerPool:
    """
    Tests for the WorkerPool class.
    """

    @pytest.mark.asyncio
    async def test_tasks(self):
        counter = 0
        callbacks = 0
        pool = WorkerPool(max_workers=10, max_queue_size=100)

        async def increment():
            nonlocal counter
            counter += 1

        def callback():
            nonlocal callbacks
            callbacks += 1

        for _ in range(100):
            await pool.schedule(increment(), done_callback=callback)

        await pool.release()
        assert counter == 100
        assert callbacks == 100
