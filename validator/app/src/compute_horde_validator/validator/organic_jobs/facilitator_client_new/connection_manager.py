import os
import asyncio

from compute_horde.transport.base import AbstractTransport, TransportConnectionError

class ConnectionManager:
    """
    Periodically checks that the connection across a transport layer is still
    active and reconnects
    """
    ADDITIONAL_HTTP_HEADERS = {
        "X-Validator-Runner-Version": os.environ.get("VALIDATOR_RUNNER_VERSION", "unknown"),
        "X-Validator-Version": os.environ.get("VALIDATOR_VERSION", "unknown"),
    }

    def __init__(self, transport_layer: AbstractTransport):
        """Sets up the connection management and starts the main loop"""
        self.transport_layer = transport_layer
        self._stop_event = asyncio.Event()
        self._main_task: asyncio.Task | None = None

    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    async def _connect_transport_layer(self):
        """Connect the transport layer"""
        # The websocket transport layer has its own exponential backoff logic
        # for retries, so if this fails then it's assumed it already retried
        # and failed.
        await self.transport_layer.start(additional_headers=self.ADDITIONAL_HTTP_HEADERS)

    async def _disconnect_transport_layer(self):
        """Clean up the transport layer connection"""
        self.transport_layer.stop()

    async def start(self):
        """Connect the transport layer and start the monitor loop"""
        self._stop_event.clear()
        await self._connect_transport_layer()
        self._main_task = asyncio.create_task(self._monitor_connection())

    async def _monitor_connection(self):
        """
        Check if the transport layer is connected and try to reconnect if it isn't
        """
        try:
            while self.is_running():
                if not self.transport_layer.is_connected():
                    await self._connect_transport_layer()
                # Use wait_for to allow interruption by stop event. TimeoutErrors are
                # expected under normal operation.
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=1.0)
                    break
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            # Log any unexpected errors but don't let them crash the monitor
            # In a real implementation, you might want to use a logger here
            print(f"Unexpected error in connection monitor: {e}")
            raise
                        
    async def stop(self):
        """Stops the connection manager"""
        if not self.is_running():
            return  # Already stopped
        
        # Signal the monitor loop to stop
        self._stop_event.set()
        
        # Cancel the main task if it exists
        if self._main_task and not self._main_task.done():
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass
        
        # Clean up transport layer
        await self._disconnect_transport_layer()
        
        self._main_task = None