import asyncio
from collections import deque
from compute_horde.transport import AbstractTransport
import pydantic
from pydantic import BaseModel
from typing import Deque
import logging
from compute_horde.fv_protocol.facilitator_requests import (
    OrganicJobRequest,
    Response,
    V0JobCheated,
    V2JobRequest,
)
from compute_horde.fv_protocol.validator_requests import (
    JobStatusUpdate,
    V0Heartbeat,
)
from .util import stop_task_gracefully, interruptable_wait, safe_send_local_message, interruptable_receive_local_message, log_system_error_event, cancel_and_await_task
from .constants import (
    JOB_REQUEST_CHANNEL,
    JOB_STATUS_UPDATE_CHANNEL,
    HEARTBEAT_CHANNEL,
    CHEATED_JOB_REPORT_CHANNEL,
    POLL_INTERVAL,
    TRANSPORT_LAYER_MESSAGE_SEND_TIMEOUT,
)
from compute_horde_validator.validator.models import SystemEvent

logger = logging.getLogger(__name__)


class MessageTypeException(Exception):
    pass


class MessageWrapper(BaseModel):
    """A simple wrapper around a message that allows for retry counting"""
    content: BaseModel
    retry_count: int = 0
    max_retries: int = 3


class MessageManager:
    """
    Handles messaging between the facilitator and the validator components.

    Performs the following operations:
    - Manages queued messages to be sent to the facilitator via the transport
      layer
    - Listens for messages from the facilitator and pushes them into the 
      default Django channel layer
    - Subscribes to messages from the default Django channel layer and pushes
      them into the message queue to be sent to the facilitator.
    """
    MAX_MESSAGE_SEND_RETRIES = 3

    def __init__(self, transport_layer: AbstractTransport) -> None:
        """
        Args:
            transport_layer (AbstractTransport): The transport layer to send
                messages through.
        """
        self.transport_layer = transport_layer
        self._queue: Deque[MessageWrapper] = deque()
        self._queue_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._stop_event.set()  # Start stopped

        self._transport_layer_listener_task: asyncio.Task | None = None
        self._message_sender_task: asyncio.Task | None = None
        self._heartbeat_listener_task: asyncio.Task | None = None
        self._job_status_update_listener_task: asyncio.Task | None = None

    async def _enqueue_message(self, message: BaseModel) -> None:
        """
        Adds a message to the end of the queue
        """
        wrapped_msg = MessageWrapper(content=message, max_retries=self.MAX_MESSAGE_SEND_RETRIES)
        async with self._queue_lock:
            self._queue.append(wrapped_msg)
	        
    async def _get_next_message(self) -> MessageWrapper | None:
        """
        Gets the oldest message in the queue (FIFO principle).

        Returns:
            MessageWrapper | None: Either a queued message or None if the queue
                is empty.

        """
        async with self._queue_lock:
            try:
                return self._queue.popleft()
            except IndexError:  # Empty queue
                return None           
        
    async def _retry_message(self, message: MessageWrapper) -> None:
        """
        Inserts message into the front of the queue again to retry sending it.
        """
        async with self._queue_lock:
            if message.retry_count < message.max_retries:
                message.retry_count += 1
                self._queue.appendleft(message)
            else:
                await log_system_error_event(
                    message=f"Failed to send message ({message.content}) after {message.retry_count} retries",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.MESSAGE_SEND_ERROR,
                    logger=logger,
                )

    async def _get_queue_length(self) -> int:
        """Get the length of the queue while avoiding race conditions."""
        async with self._queue_lock:
            return len(self._queue)

    async def _process_incoming_transport_layer_message(self, message: str) -> None:
        """
        Parses an incoming message from the transport layer and takes the 
        appropriate action.

        Expects one of the following message types:
            - Response (sent by the facilitator to acknowledge messages)
            - OrganicJobRequest
            - V0JobCheated
        
        Logs an error message and raises a MessageTypeException if the message
        type is unknown.
        """
        try:
            response = Response.model_validate_json(message)
        except pydantic.ValidationError:
            pass
        else:
            if response.status != "success":
                logger.error("received error response from facilitator: %r", response)
            return

        try:
            job_request = pydantic.TypeAdapter(OrganicJobRequest).validate_json(message)
        except pydantic.ValidationError:
            pass
        else:
            await safe_send_local_message(
                channel=JOB_REQUEST_CHANNEL,
                message=job_request,
                logger=logger,
            )
            return

        try:
            cheated_job_report = pydantic.TypeAdapter(V0JobCheated).validate_json(message)
        except pydantic.ValidationError:
            pass
        else:
            await safe_send_local_message(
                channel=CHEATED_JOB_REPORT_CHANNEL,
                message=cheated_job_report,
                logger=logger,
            )
            return

        await log_system_error_event(
            message=f"unsupported or malformed message received from facilitator: {message}",
            event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
            event_subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
            logger=logger,
        )

    async def _listen_for_transport_layer_messages(self) -> None:
        """
        Listens for messages from the transport layer and adds them to the 
        appropriate Django channel.
        """
        try:
            while self.is_running():
                # If transport layer isn't connected, wait (and hope) for
                # ConnectionManager to re-establish the connection.
                if not self.transport_layer.is_connected():
                    await interruptable_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
                    continue
                    
                # Allow the receive to be interrupted in case the transport layer hangs
                receive_task = asyncio.create_task(self.transport_layer.receive())
                interrupt_task = asyncio.create_task(self._stop_event.wait())
                await asyncio.wait(
                    [receive_task, interrupt_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                # Cancel potentially unfinished tasks to prevent task leaks
                if receive_task.done():
                    await cancel_and_await_task(interrupt_task)
                    message = await receive_task
                    await self._process_incoming_transport_layer_message(message)
                else:
                    await cancel_and_await_task(receive_task)
                    await cancel_and_await_task(interrupt_task)
                
                # The transport_layer.receive method is blocking for the 
                # websockets transport layer but this might not be the case for
                # other transport layers. To avoid excessive polling of the 
                # transport layer, an additional cool-down wait is included here.
                await interruptable_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
        except asyncio.CancelledError:
            pass

    async def _try_to_send_next_message(self) -> None:
        """Attempt to send the next message in the queue with retries."""
        async with self._send_lock:
            msg = await self._get_next_message()
            if msg is None:
                return
            try:
                await asyncio.wait_for(
                    self.transport_layer.send(msg.content.model_dump_json()),
                    timeout=TRANSPORT_LAYER_MESSAGE_SEND_TIMEOUT,
                )
            except Exception as exc:
                logger.debug(
                    "Failed to send message (%s) with error (%s: %s) and attempting to retry",
                    msg.content, type(exc).__name__, exc
                )
                await self._retry_message(msg)
                # Brief wait to allow whatever problem prevented the message to be sent
                # to (hopefully) be fixed elsewhere
                await interruptable_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)

    async def _send_remaining_messages(self) -> None:
        """Attempt to send all remaining messages in the queue."""
        while True:
            if await self._get_queue_length() == 0:
                break
            await self._try_to_send_next_message()

    async def _send_messages(self) -> None:
        """
        Goes through message stored in the queue and attempts to send them
        through the transport layer.
        """
        try:
            while self.is_running():
                if await self._get_queue_length() == 0:
                    await interruptable_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
                    continue

                # If transport layer isn't connected, wait (and hope) for
                # ConnectionManager to re-establish the connection.
                if not self.transport_layer.is_connected():
                    await interruptable_wait(
                        timeout=POLL_INTERVAL,
                        stop_event=self._stop_event,
                    )
                    continue

                await self._try_to_send_next_message()
        except asyncio.CancelledError:
            pass
        finally:
            await self._send_remaining_messages()

    async def _process_incoming_local_message(self, msg: dict) -> None:
        """
        Validates a message from the default Django channel and adds it to the
        message queue.

        Args:
            msg (dict): The message to validate.
        """
        outgoing = None
        try:
            outgoing = JobStatusUpdate.model_validate(msg)
        except pydantic.ValidationError:
            pass
        try:
            outgoing = V0Heartbeat.model_validate(msg)
        except pydantic.ValidationError:
            pass
        if outgoing is None:
            raise MessageTypeException(f"Unknown message type: {msg}")
        
        await self._enqueue_message(outgoing)

    async def _listen_for_local_messages(self, channel: str) -> None:
        """
        Listen for messages on the default Django channel and place them into the message queue.
        """
        try:
            while self.is_running():
                msg_or_none = await interruptable_receive_local_message(channel, stop_event=self._stop_event)
                if msg_or_none is not None:
                    await self._process_incoming_local_message(msg_or_none)
        except asyncio.CancelledError:
            pass
    
    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    async def start(self) -> None:
        if self.is_running():
            return
            
        self._stop_event.clear()
        
        self._transport_layer_listener_task = asyncio.create_task(self._listen_for_transport_layer_messages())
        self._heartbeat_listener_task = asyncio.create_task(self._listen_for_local_messages(HEARTBEAT_CHANNEL))
        self._job_status_update_listener_task = asyncio.create_task(self._listen_for_local_messages(JOB_STATUS_UPDATE_CHANNEL))
        self._message_sender_task = asyncio.create_task(self._send_messages())
        
    async def stop(self) -> None:
        """
        Ends message manager. Attempts to send all remaining messages to clear
        the queue
        """
        if not self.is_running():
            return
        
        self._stop_event.set()

        # Stop listening for local messages first to prevent messages getting
        # stuck in the queue after the message sender task has been shut off 
        # --> messages may now get stuck in the Redis queue
        try:
            await stop_task_gracefully(self._job_status_update_listener_task)
        except Exception as exc:
            logger.error("Error stopping job status update listener task: %s: %s", type(exc).__name__, exc)

        try:
            await stop_task_gracefully(self._heartbeat_listener_task)
        except Exception as exc:
            logger.error("Error stopping heartbeat listener task: %s: %s", type(exc).__name__, exc)

        try:
            await stop_task_gracefully(self._message_sender_task)
        except Exception as exc:
            logger.error("Error stopping message sender task: %s: %s", type(exc).__name__, exc)

        try:
            await stop_task_gracefully(self._transport_layer_listener_task)
        except Exception as exc:
            logger.error("Error stopping transport layer listener task: %s: %s", type(exc).__name__, exc)

        # Attempt to clear the queue one final time. This may have already happened 
        # in the finally-block of the _send_messages method but if there are too many
        # messages this may have cancelled too soon.
        await self._send_remaining_messages()

        self._transport_layer_listener_task = None
        self._message_sender_task = None
        self._heartbeat_listener_task = None
        self._job_status_update_listener_task = None
