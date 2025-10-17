import os
import asyncio
import bittensor_wallet
import pydantic
import httpx
import logging
import tenacity

from django.conf import settings

from compute_horde.transport.base import AbstractTransport, TransportConnectionError
from compute_horde.fv_protocol.facilitator_requests import Error, Response
from compute_horde.fv_protocol.validator_requests import V0AuthenticationRequest

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
    POLL_INTERVAL = 1.0
    STOP_TIMEOUT = 5.0
    RECONNECT_TIMEOUT = 60
    AUTH_RETRIES = 3
    AUTH_SEND_TIMEOUT = 10.0
    AUTH_RECEIVE_TIMEOUT = 10.0
    ADDITIONAL_HTTP_HEADERS = {
        "X-Validator-Runner-Version": os.environ.get("VALIDATOR_RUNNER_VERSION", "unknown"),
        "X-Validator-Version": os.environ.get("VALIDATOR_VERSION", "unknown"),
    }

    def __init__(
        self,
        keypair: bittensor_wallet.Keypair,
        transport_layer: AbstractTransport,
    ):
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
        self._main_task: asyncio.Task | None = None
        self._http_client = httpx.AsyncClient() | None = None

    def is_running(self) -> bool:
        return (
            not self._stop_event.is_set() and
            self._main_task is not None and
            not self._main_task.done()
        )

    @tenacity.retry(
        stop=tenacity.stop_after_delay(RECONNECT_TIMEOUT),
        wait=tenacity.wait_incrementing(start=2, increment=2, max=10),
        retry=tenacity.retry_if_exception_type(TransportConnectionError),
        reraise=True,  # Otherwise we will get a generic RetryError in the trace
    )
    async def _connect_with_retry(self):
        await self.transport_layer.start(additional_headers=self.ADDITIONAL_HTTP_HEADERS)
    
    # Authentication could fail if messages are being dropped. A few retries should reduce this possibility
    @tenacity.retry(
        stop=tenacity.stop_after_attempt(AUTH_RETRIES),
        wait=tenacity.wait_incrementing(start=2, increment=2, max=10),
        retry=tenacity.retry_if_exception_type(AuthenticationError),
        reraise=True,  # Otherwise we will get a generic RetryError in the trace
    )
    async def _authenticate_connection(self):
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
            raw_msg = asyncio.wait_for(
                await self.transport_layer.receive(),
                timeout=self.AUTH_RECEIVE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise AuthenticationError("authentication receive response timed out", [])

        try:
            response = Response.model_validate_json(raw_msg)
        except pydantic.ValidationError as exc:
            raise AuthenticationError(
                "did not receive Response for V0AuthenticationRequest", []
            ) from exc
        if response.status != "success":
            raise AuthenticationError("auth request received failed response", response.errors)

    async def _call_debug_connect_facilitator_webhook(self):
        if settings.DEBUG_CONNECT_FACILITATOR_WEBHOOK:
            if self._http_client is None:
                self._http_client = httpx.AsyncClient()
            try:
                await self._http_client.get(settings.DEBUG_CONNECT_FACILITATOR_WEBHOOK)
            except Exception:
                logger.info("when calling connect webhook:", exc_info=True)

    async def _connect_transport_layer(self):
        try:
            await self._connect_with_retry()
        except TransportConnectionError as exc:
            logger.error("Error connecting to transport layer: %s: %s", type(exc).__name__, exc)
            raise

        try:
            await self._authenticate_connection()
        except AuthenticationError as exc:
            logger.error("Error authenticating connection: %s: %s", type(exc).__name__, exc)
            raise

        await self._call_debug_connect_facilitator_webhook()

    async def _disconnect_transport_layer(self):
        await self.transport_layer.stop()


    async def start(self):
        if self.is_running():
            return

        self._stop_event.clear()
        await self._connect_transport_layer()
        self._main_task = asyncio.create_task(self._monitor_connection())
        await self._main_task

    async def _monitor_connection(self):
        """
        Check if the transport layer is connected and try to reconnect if it isn't
        """
        try:
            while self.is_running():
                if not self.transport_layer.is_connected():
                    await self._connect_transport_layer()
                # Reduce polling of transport layer
                # Use wait_for to allow interruption by stop event
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.POLL_INTERVAL)
                except asyncio.TimeoutError:
                    # Expected under normal operation
                    pass
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Error monitoring connection: %s: %s", type(exc).__name__, exc)
            raise
                        
    async def stop(self):
        if not self.is_running():
            return
        
        self._stop_event.set()
        
        # Wait for the main loop to end gracefully, cancel it otherwise
        if self._main_task and not self._main_task.done():
            try:
                await asyncio.wait_for(self._main_task, timeout=self.STOP_TIMEOUT)
            except asyncio.TimeoutError:
                self._main_task.cancel()
                await self._main_task
        
        # Clean up transport layer
        await self._disconnect_transport_layer()
        
        self._main_task = None

        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None