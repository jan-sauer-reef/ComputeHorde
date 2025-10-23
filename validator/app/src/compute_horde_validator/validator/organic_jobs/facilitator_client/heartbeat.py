import asyncio
import logging
from .util import stop_task_gracefully, interruptable_wait, safe_send_local_message
from .constants import HEARTBEAT_CHANNEL
from compute_horde.fv_protocol.validator_requests import V0Heartbeat

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
        try:
            while self.is_running():
                await safe_send_local_message(
                    channel=HEARTBEAT_CHANNEL,
                    message=V0Heartbeat(),
                    logger=logger,
                )
                await interruptable_wait(timeout=self.HEARTBEAT_INTERVAL, stop_event=self._stop_event)
        except asyncio.CancelledError:
            pass

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
