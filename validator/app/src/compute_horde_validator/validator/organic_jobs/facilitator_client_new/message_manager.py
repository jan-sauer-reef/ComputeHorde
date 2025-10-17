import asyncio
from collections import deque
from compute_horde.transport import AbstractTransport
import pydantic
from pydantic import BaseModel
from typing import Deque
import logging
from compute_horde.fv_protocol.facilitator_requests import (
    Error,
    OrganicJobRequest,
    Response,
    V0JobCheated,
    V2JobRequest,
)

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

    def __init__(
        self,
        transport_layer: AbstractTransport,
        local_channels: list[str] | None = None,
    ):
        """
        Args:
            transport_layer (AbstractTransport): The transport layer to send
                messages through.
            local_channels (list[str] | None): The local Django channels to 
                subscribe to for messages. All messages received from these
                channels will be added to the message queue to be sent to the
                facilitator.
        """
        self._transport_layer = transport_layer
        self._local_channels = local_channels or []
        self._queue: Deque[MessageWrapper] = deque()
        self._queue_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()

        self._transport_layer_listener_task: asyncio.Task | None = None
        self._message_sender_task: asyncio.Task | None = None
        self._local_listeners: list[asyncio.Task] = []

    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    async def start(self):
        if self.is_running():
            return
            
        self._stop_event.clear()

        self._transport_layer_listener_task = asyncio.create_task(self._listen_for_transport_layer_messages)
        self._message_sender_task = asyncio.create_task(self._send_messages)
        self._local_listeners = [
            asyncio.create_task(self._listen_for_local_messages(channel))
            for channel in self._local_channels
        ]
        

    async def _enqueue_message(self, message: BaseModel) -> None:
        """
        Adds a message to the end of the queue
        """
        wrapped_msg = MessageWrapper(content=message, max_retries=self.MAX_MESSAGE_SEND_RETRIES)
        async with self._queue_lock:
            self._queue.append(wrapped_msg)
	        
    async def _get_next_message(self) -> MessageWrapper | None:
        """
        Gets the oldest message in the queue (FIFO principle)
        """
        with self._queue_lock:
            if not self._queue:
                return None
            return self._queue.popleft()
        
    async def _retry_message(self, message: MessageWrapper) -> None:
        """
        Inserts message into the front of the queue again to retry sending it.
        """
        with self._queue_lock:
            if message.retry_count < message.max_retries:
                message.retry_count += 1
                self._queue.appendleft(message)
            else:
                logger.warning("Failed to send message after %s retries: %s", message.max_retries, message.content)

    async def _process_incoming_transport_layer_message(self, message: str) -> None:
        """
        Parses an incoming message from the transport layer and takes the 
        appropriate action.
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
            # TODO
            # Add job request to Redis channel 'job_requests' --> will be read by the
            # FacilitatorClient
            return

        try:
            return pydantic.TypeAdapter(V0JobCheated).validate_json(message)
        except pydantic.ValidationError:
            pass
        else:
            # TODO
            # Add cheated job report to Redis channel 'cheated_job_reports'
            # --> will be read by the FacilitatorClient
            return

        logger.error("Unknown message type: %s", message)
        raise MessageTypeException("Unknown message type: %s", message)

    async def _listen_for_transport_layer_messages(self):
        """
        Listens for messages from the transport layer and adds them to the message
        queue.

        Expects one of the following message types:
            - Response (sent by the facilitator to acknowledge messages)
            - OrganicJobRequest
            - V0JobCheated
        """
        try:
            while self.is_running():
                message = await self._transport_layer.receive()
                message = await self._process_incoming_transport_layer_message(message)
                
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Error listening for transport layer messages: %s: %s", type(exc).__name__, exc)
            raise

    async def _send_messages(self):
        """
        Main "run forever" loop that attempts to send all queued messages in
        their order. Will retry resending messages if they fail to send and
        can optionally raise errors or send system events indicating a message
        could not be sent. Will only attempt to send messages if the connection
        is active
        """
        while self._is_running:
            if len(self._queue) == 0:
                await asyncio.sleep(1)
                continue

            # To avoid wasting CPU cycles on persistent polling, the loop
            # sleeps briefly if the transport layer isn't connected. It would 
            # be nicer if the transport layer had some sort of internal 
            # mechanism that allows callers to poll once and wait, e.g.
            # 'wait_until_connected()' but this would need to be carefully
            # managed with regards to dropped connections.
            if not self._transport_layer.is_connected():
                await asyncio.sleep(1)
                continue
                
            await self._try_to_send_next_message()
    

    async def _listen_for_local_messages(self, channel: str):
        pass


    async def stop(self):
        """
        Ends message manager. Attempts to send all remaining messages to clear
        the queue
        """
        self._is_running = False
        while len(self._queue) > 0:
            await self._try_to_send_next_message()
    
    async def _try_to_send_next_message(self):
        async with self._send_lock:
            msg = await self.get_next_message()
            if msg is None:
                return
            try:
                await self._transport_layer.send(msg.content)
            except Exception:
                await self.retry_message(msg)	  