import asyncio


async def stop_task_gracefully(task: asyncio.Task | None, timeout: float = 5.0) -> None:
    """
    Waits for a task to end gracefully, cancels it if it doesn't end within
    the given timeout. Should typically be run after task shut down has been
    triggered elsewhere.

    Args:
        task (asyncio.Task | None): The task to wait for. If None, this 
            function does nothing. Defaults to None.
        timeout (float): The timeout in seconds. Defaults to 5.0 seconds.
    """
    if task and not task.done():
        try:
            await asyncio.wait_for(task, timeout=timeout)
        except asyncio.TimeoutError:
            task.cancel()
            await task
        finally:
            task = None


async def interruptable_wait(timeout: float = 1.0, stop_event: asyncio.Event | None = None) -> None:
    """
    Waits for a given amount of time with the option of interrupting the wait
    by a stop event being set.

    Args:
        timeout (float): The timeout in seconds. Defaults to 1.0 seconds.
        stop_event (asyncio.Event | None): The stop event to wait for. If not
            None, then the wait will be interrupted when the stop event is set.
            If None, the wait will not be interrupted. Defaults to None.
    """
    if stop_event is None:
        await asyncio.sleep(timeout)
    else:
        await asyncio.wait(
            [
                asyncio.create_task(stop_event.wait()),
                asyncio.create_task(asyncio.sleep(timeout)),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )