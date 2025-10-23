from django.conf import settings
from compute_horde_validator.validator.models import SystemEvent
import asyncio
import logging
from pydantic import BaseModel
from channels.layers import get_channel_layer
from compute_horde.fv_protocol.validator_requests import (
    JobStatusUpdate,
    V0Heartbeat,
)
from compute_horde.fv_protocol.facilitator_requests import V0JobCheated
from .constants import LOCAL_MESSAGE_SEND_TIMEOUT


default_logger = logging.getLogger(__name__)


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
                {"payload": message.model_dump(mode="json")},
            ),
            timeout=LOCAL_MESSAGE_SEND_TIMEOUT,
        )
    except Exception as exc:
        logger_to_use = logger if logger is not None else default_logger
        logger_to_use.error(
            "Error sending message (%s) over channel '%s' | %s: %s",
            message, channel, type(exc).__name__, exc,
        )


async def log_sytem_error_event(
    message: str,
    type: SystemEvent.EventType,
    subtype: SystemEvent.EventSubType,
    logger: logging.Logger | None = None) -> None:
    """
    Logs a system error event to the default logs and database.

    Args:
        message (str): The message to log and save.
        type (SystemEvent.EventType): The type of the system event.
        subtype (SystemEvent.EventSubType): The subtype of the system event.
        logger (logging.Logger | None): The logger to use. Included to make
            it easier to trace the source of the error as this is a utility
            function that may be used by multiple components. If None, a 
            default logger will be used. Defaults to None.
    """
    logger_to_use = logger if logger is not None else default_logger
    logger_to_use.error(message)
    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=type,
        subtype=subtype,
        long_description=message,
    )


async def save_facilitator_event(message: BaseModel, long_description: str) -> None:
    """
    Saves a facilitator client error event. Autoselects the subtype based on the message type.

    Args:
        message (BaseModel): The message that caused the event.
        long_description (str): The long description of the event.
    """
    if isinstance(message, V0Heartbeat):
        supertype = SystemEvent.EventType.FACILITATOR_CLIENT_ERROR
        subtype = SystemEvent.EventSubType.HEARTBEAT_ERROR
    elif isinstance(message, JobStatusUpdate):
        supertype = SystemEvent.EventType.FACILITATOR_CLIENT_ERROR
        subtype = SystemEvent.EventSubType.JOB_STATUS_UPDATE_ERROR
    elif isinstance(message, V0JobCheated):
        supertype = SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
        subtype = SystemEvent.EventSubType.JOB_CHEATED
    else:
        supertype = SystemEvent.EventType.FACILITATOR_CLIENT_ERROR
        subtype = SystemEvent.EventSubType.GENERIC_ERROR

    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=supertype,
        subtype=subtype,
        long_description=long_description,
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
        task = asyncio.create_task(get_channel_layer().receive(channel))
        await asyncio.wait(
            [asyncio.create_task(stop_event.wait()), task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task.done():
            return await task
        return None


        