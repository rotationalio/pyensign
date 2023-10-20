import asyncio
import inspect


def sync_to_async(fn):
    """
    Function that wraps a synchronous function inside of an async function and returns
    the async function. Can be used as a function or a decorator.
    """

    if inspect.isgeneratorfunction(fn):
        return gen_to_async(fn)
    elif inspect.isfunction(fn):
        return fn_to_async(fn)
    else:
        raise TypeError("expected a function or generator function")


def async_to_sync(coro):
    """
    Function that wraps an async function inside of a synchronous function and returns
    the synchronous function. Can be used as a function or a decorator.
    """

    if inspect.isasyncgenfunction(coro):
        return async_gen_to_sync(coro)
    elif inspect.iscoroutinefunction(coro):
        return coro_to_sync(coro)
    else:
        raise TypeError("expected an async function or async generator function")


def coro_to_sync(coro):
    def wrap(*args, **kwargs):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            # If there is a running event loop, add the coroutine to the event loop
            if loop.is_running():
                return asyncio.run_coroutine_threadsafe(
                    coro(*args, **kwargs), loop
                ).result()

        return asyncio.run(coro(*args, **kwargs))

    return wrap


def async_gen_to_sync(gen):
    def run_in_current_loop(gen):
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            # If there is a running event loop, call the generator in the event loop
            if loop.is_running():
                return asyncio.run_coroutine_threadsafe(gen.__anext__(), loop).result()

        return asyncio.run(gen.__anext__())

    def wrap(*args, **kwargs):
        iter = gen(*args, **kwargs)

        def sync_next():
            try:
                return run_in_current_loop(iter)
            except StopIteration:
                raise StopAsyncIteration

        while True:
            try:
                yield sync_next()
            except StopAsyncIteration:
                break

    return wrap


def fn_to_async(fn):
    async def wrap(*args, **kwargs):
        loop = asyncio.get_event_loop()

        def wrap_fn():
            return fn(*args, **kwargs)

        # TODO: Figure out which thread to run the code in
        return await loop.run_in_executor(None, wrap_fn)

    return wrap


def gen_to_async(gen):
    async def wrap(*args, **kwargs):
        loop = asyncio.get_event_loop()
        iter = gen(*args, **kwargs)

        def sync_next():
            try:
                return next(iter)
            except StopIteration:
                raise StopAsyncIteration

        while True:
            try:
                # TODO: Figure out which thread to run the code in
                val = await loop.run_in_executor(None, sync_next)
                yield val
            except StopAsyncIteration:
                break

    return wrap
