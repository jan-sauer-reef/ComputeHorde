import asyncio
from collections import deque
import pydantic
import time
from pydantic import BaseModel
from typing import Deque
import logging
from compute_horde.fv_protocol.facilitator_requests import (
    OrganicJobRequest,
    Response,
    V0JobCheated,
)
from compute_horde.fv_protocol.validator_requests import (
    JobStatusUpdate,
    V0Heartbeat,
)
from .util import stop_task_gracefully, interruptible_wait, safe_send_local_message, interruptible_receive_local_message, log_system_error_event, interruptible_receive_transport_layer_message, cancel_and_await_task
from .constants import (
    JOB_REQUEST_CHANNEL,
    JOB_STATUS_UPDATE_CHANNEL,
    HEARTBEAT_CHANNEL,
    CHEATED_JOB_REPORT_CHANNEL,
    POLL_INTERVAL,
    TRANSPORT_LAYER_MESSAGE_SEND_TIMEOUT,
)
from compute_horde_validator.validator.models import SystemEvent
from .exceptions import LocalChannelSendError
from .connection_manager import ConnectionManager
from .base import BaseComponent
from .metrics import (
    VALIDATOR_FC_COMPONENT_STATE,
    VALIDATOR_FC_MESSAGE_QUEUE_LENGTH,
    VALIDATOR_FC_MESSAGES_SENT,
    VALIDATOR_FC_MESSAGES_RECEIVED,
    VALIDATOR_FC_MESSAGE_SEND_FAILURES,
    VALIDATOR_FC_MESSAGE_SEND_DURATION,
)

logger = logging.getLogger(__name__)


class MessageWrapper(BaseModel):
    """A simple wrapper around a message that allows for retry counting"""
    content: BaseModel
    retry_count: int = 0
    max_retries: int = 3


class MessageTypeException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(f"Unknown message type: {message}")


class MessageRetryLimitExceeded(Exception):
    def __init__(self, message: MessageWrapper) -> None:
        super().__init__(f"Failed to send message ({message.content}) after {message.retry_count} retries")


class MessageManager(BaseComponent):
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

    def __init__(self, connection_manager: ConnectionManager) -> None:
        """
        Args:
            connection_manager (ConnectionManager): The connection manager to use
                to interact with the transport layer.
        """
        super().__init__()
        self.connection_manager = connection_manager
        self._queue: Deque[MessageWrapper] = deque()
        self._queue_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        
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
            VALIDATOR_FC_MESSAGE_QUEUE_LENGTH.set(len(self._queue))
	        
    async def _get_next_message(self) -> MessageWrapper | None:
        """
        Gets the oldest message in the queue (FIFO principle).

        Returns:
            MessageWrapper | None: Either a queued message or None if the queue
                is empty.

        """
        async with self._queue_lock:
            try:
                msg = self._queue.popleft()
                VALIDATOR_FC_MESSAGE_QUEUE_LENGTH.set(len(self._queue))
                return msg
            except IndexError:  # Empty queue
                VALIDATOR_FC_MESSAGE_QUEUE_LENGTH.set(0)
                return None           
        
    async def _retry_message(self, message: MessageWrapper) -> None:
        """
        Inserts message into the front of the queue again to retry sending it.

        Raises:
            MessageRetryLimitExceeded: If the message has reached the maximum number of retries.
        """
        async with self._queue_lock:
            if message.retry_count < message.max_retries:
                message.retry_count += 1
                self._queue.appendleft(message)
                VALIDATOR_FC_MESSAGE_QUEUE_LENGTH.set(len(self._queue))
            else:
                VALIDATOR_FC_MESSAGE_SEND_FAILURES.labels(message_type=type(message.content).__name__).inc()
                raise MessageRetryLimitExceeded(message)

    async def _get_queue_length(self) -> int:
        """Get the length of the queue while avoiding race conditions."""
        async with self._queue_lock:
            return len(self._queue)

    async def _process_incoming_transport_layer_message(self, message: str) -> BaseModel:
        """
        Parses an incoming message from the transport layer and takes the 
        appropriate action.

        Expects one of the following message types:
            - Response (sent by the facilitator to acknowledge messages)
            - OrganicJobRequest
            - V0JobCheated

        Args:
            message (str): The message to parse.

        Raises:
            LocalChannelSendError: If an error occurs while sending the message to the local channel.
            MessageTypeException: If the message type is unknown.

        Returns:
            BaseModel: The parsed message.
        """
        try:
            response = Response.model_validate_json(message)
        except pydantic.ValidationError:
            pass
        else:
            if response.status != "success":
                logger.error("received error response from facilitator: %r", response.model_dump_json())
            return response

        try:
            job_request = pydantic.TypeAdapter(OrganicJobRequest).validate_json(message)
        except pydantic.ValidationError:
            pass
        else:
            await safe_send_local_message(
                channel=JOB_REQUEST_CHANNEL,
                message=job_request,
            )
            return job_request

        try:
            cheated_job_report = pydantic.TypeAdapter(V0JobCheated).validate_json(message)
        except pydantic.ValidationError:
            pass
        else:
            await safe_send_local_message(
                channel=CHEATED_JOB_REPORT_CHANNEL,
                message=cheated_job_report,
            )
            return cheated_job_report

        raise MessageTypeException(message)

    async def _listen_for_transport_layer_messages(self) -> None:
        """
        Listens for messages from the transport layer and adds them to the 
        appropriate Django channel.
        """
        while self.is_running():
            try:
                # If transport layer isn't connected and authenticated, wait 
                # (and hope) for ConnectionManager to (re-)establish the connection.
                # This also ensures that the message manager doesn't accidentally 
                # grab the authentication message
                if not self.connection_manager.is_connected_and_authenticated():
                    await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
                    continue
                    
                message = await interruptible_receive_transport_layer_message(
                    connection_manager=self.connection_manager,
                    stop_event=self._stop_event,
                )
                if message is not None:
                    message =await self._process_incoming_transport_layer_message(message)
                
                VALIDATOR_FC_MESSAGES_RECEIVED.labels(message_type=type(message).__name__).inc()

                # The transport_layer.receive method is blocking for the 
                # websockets transport layer but this might not be the case for
                # other transport layers. To avoid excessive polling of the 
                # transport layer, an additional cool-down wait is included here.
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
            except asyncio.CancelledError:
                self._stop_event.set()
                VALIDATOR_FC_COMPONENT_STATE.labels(component=self.name).set(0)
                break
            except LocalChannelSendError as exc:
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.MESSAGE_SEND_ERROR,
                    logger=logger,
                )
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
            except MessageTypeException as exc:
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                    logger=logger,
                )
                VALIDATOR_FC_MESSAGES_RECEIVED.labels(message_type="unknown").inc()
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
            except Exception as exc:
                await log_system_error_event(
                    message=f"Error listening to incoming transport layer messages: {type(exc).__name__}: {exc}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                    logger=logger,
                )
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)

    async def _try_to_send_next_message(self) -> None:
        """
        Attempt to send the next message in the queue with retries.
        
        Raises:
            MessageRetryLimitExceeded: If the message has reached the maximum number of retries.
        """
        async with self._send_lock:
            msg = await self._get_next_message()
            if msg is None:
                return
            try:
                send_task = None
                start = time.monotonic()
                send_task = asyncio.create_task(self.connection_manager.send(msg.content.model_dump_json()))
                await asyncio.wait_for(send_task, timeout=TRANSPORT_LAYER_MESSAGE_SEND_TIMEOUT)
            except Exception as exc:
                if send_task is None:
                    # Asyncio could fail in creating the send task itself which would be no fault of the connection itself and shoukdn't count against the message retries
                    logger.error("Failed to create send task for message (%s) with error (%s: %s) and attempting to retry", msg.content, type(exc).__name__, exc)
                    msg.retry_count -= 1  # Subtract so that when _retry_message increments, it's net 0
                else:
                    await cancel_and_await_task(send_task)

                await self._retry_message(msg)  # Raises the MessageRetryLimitExceeded exception

                logger.debug(
                    "Failed to send message (%s) with error (%s: %s) and attempting to retry",
                    msg.content, type(exc).__name__, exc
                )

                # Brief wait to allow whatever problem prevented the message to be sent
                # to (hopefully) be fixed elsewhere
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
            else:
                VALIDATOR_FC_MESSAGE_SEND_DURATION.labels(message_type=type(msg.content).__name__).observe(time.monotonic() - start)
                VALIDATOR_FC_MESSAGES_SENT.labels(
                    message_type=type(msg.content).__name__,
                    retries=msg.retry_count,
                ).inc()

    async def _send_remaining_messages(self) -> None:
        """Attempt to send all remaining messages in the queue."""
        while True:
            if await self._get_queue_length() == 0:
                break
            try:
                await self._try_to_send_next_message()
            except MessageRetryLimitExceeded as exc:
                # This is a cleanup function -> errors can only be logged and accepted at this point
                logger.error(str(exc))

    async def _send_messages(self) -> None:
        """
        Goes through message stored in the queue and attempts to send them
        through the transport layer.
        """
        while self.is_running():
            try:    
                if await self._get_queue_length() == 0:
                    await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
                    continue

                # If transport layer isn't connected, wait (and hope) for
                # ConnectionManager to re-establish the connection.
                if not self.connection_manager.is_connected_and_authenticated():
                    await interruptible_wait(
                        timeout=POLL_INTERVAL,
                        stop_event=self._stop_event,
                    )
                    continue

                await self._try_to_send_next_message()
            except asyncio.CancelledError:
                self._stop_event.set()
                await self._send_remaining_messages()
                break
            except MessageRetryLimitExceeded as exc:
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.MESSAGE_SEND_ERROR,
                    logger=logger,
                )
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
            except Exception as exc:
                await log_system_error_event(
                    message=f"Error sending messages: {type(exc).__name__}: {exc}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                    logger=logger,
                )
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)

    async def _process_incoming_local_message(self, msg: dict) -> None:
        """
        Validates a message from the default Django channel and adds it to the
        message queue.

        Args:
            msg (dict): The message to validate.

        Raises:
            MessageTypeException: If the message type is unknown.
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
            raise MessageTypeException(msg)
        
        await self._enqueue_message(outgoing)

    async def _listen_for_local_messages(self, channel: str) -> None:
        """
        Listen for messages on the default Django channel and place them into the message queue.
        """
        while self.is_running():
            try:
                msg_or_none = await interruptible_receive_local_message(channel, stop_event=self._stop_event)
                if msg_or_none is not None:
                    await self._process_incoming_local_message(msg_or_none)
            except asyncio.CancelledError:
                self._stop_event.set()
                break
            except MessageTypeException as exc:
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                    logger=logger,
                )
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
            except Exception as exc:
                await log_system_error_event(
                    message=f"Error listening for local messages: {type(exc).__name__}: {exc}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                    logger=logger,
                )
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
    
    async def start(self) -> None:
        """Starts the message manager."""
        if self.is_running():
            return
            
        await super().start()

        self._transport_layer_listener_task = asyncio.create_task(self._listen_for_transport_layer_messages())
        self._heartbeat_listener_task = asyncio.create_task(self._listen_for_local_messages(HEARTBEAT_CHANNEL))
        self._job_status_update_listener_task = asyncio.create_task(self._listen_for_local_messages(JOB_STATUS_UPDATE_CHANNEL))
        self._message_sender_task = asyncio.create_task(self._send_messages())
        
    async def stop(self) -> None:
        """
        Ends message manager. Attempts to send all remaining messages to clear
        the queue.
        """
        if not self.is_running():
            return
        
        await super().stop()

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
            await stop_task_gracefully(self._transport_layer_listener_task)
        except Exception as exc:
            logger.error("Error stopping transport layer listener task: %s: %s", type(exc).__name__, exc)

        try:
            await stop_task_gracefully(self._message_sender_task)
        except Exception as exc:
            logger.error("Error stopping message sender task: %s: %s", type(exc).__name__, exc)

        # Attempt to clear the queue one final time. This may have already happened 
        # in the finally-block of the _send_messages method but if there are too many
        # messages this may have cancelled too soon.
        await self._send_remaining_messages()

        self._transport_layer_listener_task = None
        self._message_sender_task = None
        self._heartbeat_listener_task = None
        self._job_status_update_listener_task = None
