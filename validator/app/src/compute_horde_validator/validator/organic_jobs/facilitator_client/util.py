from django.conf import settings
from compute_horde_validator.validator.models import SystemEvent
import asyncio
import logging
from pydantic import BaseModel
from channels.layers import get_channel_layer
from .constants import LOCAL_MESSAGE_SEND_TIMEOUT, GRACEFULLY_STOP_TIMEOUT


default_logger = logging.getLogger(__name__)


async def cancel_and_await_task(task: asyncio.Task) -> None:
    """
    A helper function that cancels a task and awaits it.
    """
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def stop_task_gracefully(task: asyncio.Task | None, timeout: float = GRACEFULLY_STOP_TIMEOUT) -> None:
    """
    Waits for a task to end gracefully, cancels it if it doesn't end within
    the given timeout. Should typically be run after task shut down has been
    triggered elsewhere.

    Args:
        task (asyncio.Task | None): The task to wait for. If None, this 
            function does nothing. Defaults to None.
        timeout (float): The timeout in seconds. Defaults to GRACEFULLY_STOP_TIMEOUT seconds.
    """
    if task and not task.done():
        try:
            await asyncio.wait_for(task, timeout=timeout)
        except asyncio.TimeoutError:
            await cancel_and_await_task(task)


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
        sleep_task = asyncio.create_task(asyncio.sleep(timeout))
        interrupt_task = asyncio.create_task(stop_event.wait())
        await asyncio.wait(
            [sleep_task, interrupt_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Cancel the tasks to avoid task leaks
        await cancel_and_await_task(sleep_task)
        await cancel_and_await_task(interrupt_task)


async def safe_send_local_message(channel: str, message: BaseModel, logger: logging.Logger | None = None) -> None:
    """
    Sends a message via the default Django channel layer and includes a timeout
    to ensure that functions that send messages don't hang indefinitely.

    Any error is logged and then discarded.

    Args:
        channel (str): The channel over which to send the message.
        message (BaseModel): The message to send.
        logger (logging.Logger | None): The logger to use. Included to make
            it easier to trace the source of the error as this is a utility
            function that may be used by multiple components. If None, a 
            default logger will be used. Defaults to None.
    """
    try:
        await asyncio.wait_for(
            get_channel_layer().send(
                channel,
                message.model_dump(mode="json"),
            ),
            timeout=LOCAL_MESSAGE_SEND_TIMEOUT,
        )
    except Exception as exc:
        logger_to_use = logger if logger is not None else default_logger
        logger_to_use.error(
            "Error sending message (%s) over channel '%s' | %s: %s",
            message, channel, type(exc).__name__, exc,
        )


async def log_system_error_event(
    message: str,
    event_type: SystemEvent.EventType,
    event_subtype: SystemEvent.EventSubType,
    logger: logging.Logger | None = None) -> None:
    """
    Logs a system error event to the default logs and database.

    Args:
        message (str): The message to log and save.
        event_type (SystemEvent.EventType): The type of the system event.
        event_subtype (SystemEvent.EventSubType): The subtype of the system event.
        logger (logging.Logger | None): The logger to use. Included to make
            it easier to trace the source of the error as this is a utility
            function that may be used by multiple components. If None, a 
            default logger will be used. Defaults to None.
    """
    logger_to_use = logger if logger is not None else default_logger
    logger_to_use.error(message)
    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=event_type,
        subtype=event_subtype,
        long_description=message,
    )


async def interruptable_receive_local_message(channel: str, stop_event: asyncio.Event | None = None) -> dict | None:
    """
    Waits for a message on a specific local Django channel with the option of
    cancelling a blocking receive call by a stop event.

    Args:
        channel (str): The channel to receive the message from.
        stop_event (asyncio.Event | None): The stop event to wait for. If not
            None, then the receive will be interrupted when the stop event is set.
            If None, the receive will not be interrupted. Defaults to None.
    
    Returns:
        dict | None: The message received from the channel or None if the receive
            was interrupted.
    """
    if stop_event is None:
        return await get_channel_layer().receive(channel)
    else:
        receive_task = asyncio.create_task(get_channel_layer().receive(channel))
        interrupt_task = asyncio.create_task(stop_event.wait())
        await asyncio.wait(
            [receive_task, interrupt_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Cancel potentially unfinished task to prevent task leaks
        if receive_task.done():
            await cancel_and_await_task(interrupt_task)
            return await receive_task
        else:
            await cancel_and_await_task(receive_task)
            await cancel_and_await_task(interrupt_task)
            return None


        