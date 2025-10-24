import os
import asyncio
import bittensor_wallet
import pydantic
import httpx
import logging

from django.conf import settings

from compute_horde.transport import AbstractTransport, TransportConnectionError
from compute_horde.fv_protocol.facilitator_requests import Error, Response
from compute_horde.fv_protocol.validator_requests import V0AuthenticationRequest
from compute_horde_validator.validator.models import SystemEvent
from .util import stop_task_gracefully, interruptible_wait, cancel_and_await_task, log_system_error_event
from .constants import POLL_INTERVAL

logger = logging.getLogger(__name__)

class AuthenticationError(Exception):
    def __init__(self, reason: str, errors: list[Error]) -> None:
        self.reason = reason
        self.errors = errors


class ConnectionManager:
    """
    Periodically checks that the connection across a transport layer is still
    active and reconnects if it isn't.
    """
    AUTH_SEND_TIMEOUT = 10.0
    AUTH_RECEIVE_TIMEOUT = 10.0
    WEBHOOK_TIMEOUT = 10.0
    ADDITIONAL_HTTP_HEADERS = {
        "X-Validator-Runner-Version": os.environ.get("VALIDATOR_RUNNER_VERSION", "unknown"),
        "X-Validator-Version": os.environ.get("VALIDATOR_VERSION", "unknown"),
    }

    def __init__(
        self,
        keypair: bittensor_wallet.Keypair,
        transport_layer: AbstractTransport,
    ) -> None:
        """
        Args:
            keypair (bittensor_wallet.Keypair): The keypair to use for authentication.
            transport_layer (AbstractTransport): The transport layer to manage
                the connection for. It is expected that this transport layer 
                handles its own reconnection logic as it may be specific to the
                connection type.
        """
        self.transport_layer = transport_layer
        self.keypair = keypair
        self._stop_event = asyncio.Event()
        self._stop_event.set()  # Start stopped
        self._authentication_flag = asyncio.Event()
        self._cleanup_event = asyncio.Event()
        self._main_task: asyncio.Task | None = None
        self._http_client: httpx.AsyncClient | None = None
    
    async def _authenticate_connection(self) -> None:
        """Authenticates the connection with the facilitator."""
        # Set to False to ensure authentication doesn't become stale
        self._authentication_flag.clear()

        if not self.transport_layer.is_connected():
            raise AuthenticationError(
                "Transport layer must be connected before authentication is possible", []
            )

        try:
            await asyncio.wait_for(
                self.transport_layer.send(
                    V0AuthenticationRequest.from_keypair(self.keypair).model_dump_json()
                ),
                timeout=self.AUTH_SEND_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise AuthenticationError("authentication send timed out", [])

        try:
            raw_msg = await asyncio.wait_for(
                self.transport_layer.receive(),
                timeout=self.AUTH_RECEIVE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise AuthenticationError("authentication receive response timed out", [])

        try:
            response = Response.model_validate_json(raw_msg)
        except pydantic.ValidationError as exc:
            raise AuthenticationError(
                f"did not receive Response for V0AuthenticationRequest. Got ({raw_msg}) instead", []
            ) from exc
        if response.status != "success":
            raise AuthenticationError("auth request received failed response", response.errors)
        
        self._authentication_flag.set()

    async def _call_debug_connect_facilitator_webhook(self) -> None:
        if settings.DEBUG_CONNECT_FACILITATOR_WEBHOOK:
            if self._http_client is None:
                self._http_client = httpx.AsyncClient()
            try:
                await self._http_client.get(settings.DEBUG_CONNECT_FACILITATOR_WEBHOOK, timeout=self.WEBHOOK_TIMEOUT)
            except Exception:
                logger.info("when calling connect webhook:", exc_info=True)

    async def _connect_transport_layer(self) -> None:
        """Connect and authenticate the transport layer and call an optional debug webhook."""
        await self.transport_layer.start(additional_headers=self.ADDITIONAL_HTTP_HEADERS)
        await self._authenticate_connection()
        await self._call_debug_connect_facilitator_webhook()

    async def _cleanup_resources(self) -> None:
        """
        Clean up resources.
        """
        if self._cleanup_event.is_set():
            return
        
        self._cleanup_event.set()
        
        if self._http_client is not None:
            try:
                close_task = asyncio.create_task(self._http_client.aclose())
                await asyncio.wait_for(close_task, timeout=1.0)
            except asyncio.TimeoutError:
                await cancel_and_await_task(close_task)
            except Exception as exc:
                logger.error("Error closing HTTP client: %s: %s", type(exc).__name__, exc)
            finally:
                self._http_client = None

        try:
            # Long timeout to allow anything still being transferred to complete
            stop_task = asyncio.create_task(self.transport_layer.stop())
            await asyncio.wait_for(stop_task, timeout=10.0)
        except asyncio.TimeoutError:
            await cancel_and_await_task(stop_task)
        except Exception as exc:
            logger.error("Error disconnecting transport layer: %s: %s", type(exc).__name__, exc)

    async def _monitor_connection(self) -> None:
        """
        Check if the transport layer is connected and try to reconnect if it isn't
        """
        while self.is_running():
            try:
                if not self.transport_layer.is_connected():
                    await self._connect_transport_layer()
                # Reduce polling of transport layer
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
            except asyncio.CancelledError:
                self._stop_event.set()
                await self._cleanup_resources()
                break
            except TransportConnectionError as exc:
                await log_system_error_event(
                    message=f"Transport layer connection error: {type(exc).__name__}: {exc}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.TRANSPORT_CONNECTION_ERROR,
                    logger=logger,
                )
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
            except AuthenticationError as exc:
                await log_system_error_event(
                    message=f"Authentication error: {type(exc).__name__}: {exc}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.AUTHENTICATION_ERROR,
                    logger=logger,
                )
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)
            except Exception as exc:
                await log_system_error_event(
                    message=f"Unexpected error: {type(exc).__name__}: {exc}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                    logger=logger,
                )
                await interruptible_wait(timeout=POLL_INTERVAL, stop_event=self._stop_event)

    def is_running(self) -> bool:
        """Checks if the connection manager is running."""
        return not self._stop_event.is_set()

    def is_connected_and_authenticated(self) -> bool:
        """Checks if the connection is connected and authenticated."""
        return self.transport_layer.is_connected() and self._authentication_flag.is_set()

    async def receive(self) -> str:
        """Receives a message from the transport layer."""
        return await self.transport_layer.receive()

    async def send(self, message: str) -> None:
        """Sends a message to the transport layer."""
        await self.transport_layer.send(message)

    async def start(self) -> None:
        """Starts the connection manager main loop."""
        if self.is_running():
            return

        self._stop_event.clear()
        self._cleanup_event.clear()
        self._main_task = asyncio.create_task(self._monitor_connection())

    async def stop(self) -> None:
        """
        Stop the connection manager main loop and clean up resources.
        """
        if not self.is_running():
            return
        
        self._stop_event.set()
        
        try:
            # Long timeout to allow the transport layer to finish transmitting
            # any messages and close gracefully
            await stop_task_gracefully(task=self._main_task, timeout=15.0)
        except Exception as exc:
            logger.error("Error in connection manager main loop: %s: %s", type(exc).__name__, exc)

        # Attempt to run cleanup again in case the monitor loop couldn't be stopped gracefully
        await self._cleanup_resources()

        self._authentication_flag.clear()
        self._main_task = None