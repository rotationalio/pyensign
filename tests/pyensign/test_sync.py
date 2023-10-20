import asyncio

from pyensign.sync import sync_to_async, async_to_sync


async def test_sync_to_async():
    @sync_to_async
    def wrapped_fn():
        return "Hello World!"

    def unwrapped_fn():
        return "Hello World!"

    @sync_to_async
    def fn_with_args(a, b):
        return a + b

    @sync_to_async
    def fn_with_kwargs(a, b=2):
        return a + b

    assert await wrapped_fn() == "Hello World!"
    assert await sync_to_async(unwrapped_fn)() == "Hello World!"
    assert await fn_with_args(1, 2) == 3
    assert await fn_with_kwargs(1, b=2) == 3


def test_sync_to_async_run():
    @sync_to_async
    def wrapped_fn():
        return "Hello World!"

    assert asyncio.run(wrapped_fn()) == "Hello World!"


async def test_sync_gen_to_async():
    @sync_to_async
    def wrapped_gen():
        yield "Hello World!"

    def unwrapped_gen():
        yield "Hello World!"

    @sync_to_async
    def gen_with_args(i):
        for j in range(i):
            yield j

    @sync_to_async
    def gen_with_kwargs(i=1):
        for j in range(i):
            yield j

    async for res in wrapped_gen():
        assert res == "Hello World!"

    async for res in sync_to_async(unwrapped_gen)():
        assert res == "Hello World!"

    sum = 0
    async for res in gen_with_args(3):
        assert res in [0, 1, 2]
        sum += res
    assert sum == 3

    sum = 0
    async for res in gen_with_kwargs(i=3):
        assert res in [0, 1, 2]
        sum += res
    assert sum == 3


def test_sync_gen_to_async_run():
    @sync_to_async
    def wrapped_gen():
        yield "Hello World!"

    async def run():
        async for res in wrapped_gen():
            assert res == "Hello World!"

    asyncio.run(run())


def test_async_to_sync():
    @async_to_sync
    async def wrapped_fn():
        return "Hello World!"

    async def unwrapped_fn():
        return "Hello World!"

    @async_to_sync
    async def fn_with_args(a, b):
        return a + b

    @async_to_sync
    async def fn_with_kwargs(a, b=2):
        return a + b

    assert wrapped_fn() == "Hello World!"
    assert async_to_sync(unwrapped_fn)() == "Hello World!"
    assert fn_with_args(1, 2) == 3
    assert fn_with_kwargs(1, b=2) == 3


def test_async_gen_to_sync():
    @async_to_sync
    async def wrapped_gen():
        yield "Hello World!"

    async def unwrapped_gen():
        yield "Hello World!"

    @async_to_sync
    async def gen_with_args(i):
        for j in range(i):
            yield j

    @async_to_sync
    async def gen_with_kwargs(i=1):
        for j in range(i):
            yield j

    for res in wrapped_gen():
        assert res == "Hello World!"

    for res in async_to_sync(unwrapped_gen)():
        assert res == "Hello World!"

    sum = 0
    for res in gen_with_args(3):
        assert res in [0, 1, 2]
        sum += res
    assert sum == 3

    sum = 0
    for res in gen_with_kwargs(i=3):
        assert res in [0, 1, 2]
        sum += res
    assert sum == 3
