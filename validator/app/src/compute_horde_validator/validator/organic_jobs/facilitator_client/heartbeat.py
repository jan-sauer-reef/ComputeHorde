import asyncio
import logging
from .util import stop_task_gracefully, interruptable_wait, safe_send_local_message
from .constants import HEARTBEAT_CHANNEL
from compute_horde.fv_protocol.validator_requests import V0Heartbeat
from .exceptions import LocalChannelSendError
from compute_horde_validator.validator.models import SystemEvent
from .util import log_system_error_event

logger = logging.getLogger(__name__)


class HeartbeatManager:
    """
    Periodically sends heartbeat messages to the Django default channel layer.
    """
    HEARTBEAT_INTERVAL = 60.0

    def __init__(self) -> None:
        self._stop_event = asyncio.Event()
        self._stop_event.set()  # Start stopped
        self._heartbeat_loop_task: asyncio.Task | None = None

    async def _heartbeat_loop(self) -> None:
        """
        Send a heartbeat message to the Django default channel layer in regular
        intervals.
        """
        while self.is_running():
            try:
                await safe_send_local_message(channel=HEARTBEAT_CHANNEL, message=V0Heartbeat())
                await interruptable_wait(timeout=self.HEARTBEAT_INTERVAL, stop_event=self._stop_event)
            except asyncio.CancelledError:
                pass
            except LocalChannelSendError as exc:
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.MESSAGE_SEND_ERROR,
                    logger=logger,
                )
            except Exception as exc:
                await log_system_error_event(
                    message=f"Error sending heartbeat message: {type(exc).__name__}: {exc}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                    logger=logger,
                )

    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    async def start(self) -> None:
        if self.is_running():
            return
        
        self._stop_event.clear()
        self._heartbeat_loop_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        if not self.is_running():
            return
        
        self._stop_event.set()

        try:
            await stop_task_gracefully(self._heartbeat_loop_task)
        except Exception as exc:
            logger.error("Error stopping heartbeat loop task: %s: %s", type(exc).__name__, exc)

        self._heartbeat_loop_task = None
