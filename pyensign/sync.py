import asyncio
import inspect


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
                yield await loop.run_in_executor(None, sync_next)
            except StopAsyncIteration:
                break

    return wrap


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
