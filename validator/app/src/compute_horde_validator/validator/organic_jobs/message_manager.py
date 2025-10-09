import asyncio
from collections import deque
from compute_horde.transport import AbstractTransport
from pydantic import BaseModel
from typing import Deque
from threading import Event

class MessageWrapper(BaseModel):
    """A simple wrapper around a message that allows for retry counting"""
    content: BaseModel
    retry_count: int = 0
    max_retries: int = 3


class MessageManager:
    """
    Periodically checks whether any messages are queued and attempts to send
    these across a transport layer.

    The message manager will only attempt to send messages if the transport
    layer is connected. It intentionally does not attempt to establish a 
    connection and relies on other components to ensure connections are active.
    """
    def __init__(self, transport_layer: AbstractTransport, max_send_retries: int = 3):
        """
        Args:
            transport_layer (AbstractTransport): The transport layer to send
                messages through.
            max_send_retries (int): The maximum number of times to retry
                sending a message.
        """
        self._queue: Deque[MessageWrapper] = deque()
        self._queue_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._transport_layer = transport_layer
        self._max_send_retries = max_send_retries
        self._is_running = False

    async def enqueue_message(self, message: BaseModel) -> None:
        """
        Adds message to the end of the queue

        Args:
            message (BaseModel): The message to add to the queue.
        """
        wrapped_msg = MessageWrapper(content=message, max_retries=self._max_send_retries)
        async with self._queue_lock:
            self._queue.append(wrapped_msg)
	        
    async def get_next_message(self) -> MessageWrapper | None:
        """
        Gets the oldest message in the queue (FIFO principle)
        
        Returns:
            MessageWrapper | None: The oldest message in the queue or None if
                the queue is empty.
        """
        with self._queue_lock:
            if not self._queue:
                return None
            return self._queue.popleft()
        
    async def retry_message(self, message: MessageWrapper) -> None:
        """
        Inserts message into the front of the queue again to retry sending it.
        Keeps track of how often a message was retried for logging and
        optionally emits a SystemEvent (or exception?) if a message cannot be
        sent

        Args:
            message (MessageWrapper): The message to reinsert into the front of
                the queue.
        """
        with self._queue_lock:
            if message.retry_count < message.max_retries:
                message.retry_count += 1
                self._queue.appendleft(message)
            else:
                # TODO: Emit system event, print log message, and/or raise 
                #   error? Should the type of message impact the behavior?
                #   E.g. log message for non-critical job update that isn't
                #   sent but system event/error for jobs that can't be started
                #   by the miner/executor?
                raise NotImplementedError()
    
    async def start(self):
        """Sets up the message queue and starts the main loop"""
        self._is_running = True
        self._main_task = asyncio.create_task(self._main_loop)
        
    async def _main_loop(self):
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