import asyncio
import pydantic
import logging
from compute_horde.fv_protocol.validator_requests import JobStatusUpdate
from django.conf import settings
from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest, V0JobCheated
from compute_horde_validator.validator.models import SystemEvent
from .constants import JOB_REQUEST_CHANNEL, CHEATED_JOB_REPORT_CHANNEL
from .util import interruptable_receive_local_message, log_sytem_error_event, stop_task_gracefully


logger = logging.getLogger(__name__)


class FacilitatorClient:
    """
    Handles job requests and cheated job reports sent from the facilitator.
    """

    def __init__(self) -> None:
        self._stop_event = asyncio.Event()
        self._job_request_listener_task: asyncio.Task | None = None
        self._cheated_job_report_listener_task: asyncio.Task | None = None

    async def _job_request_handler(self) -> None:
        """
        Listens for messages on the local job requests channel and forwards these to the job dispatcher.
        """
        try:
            while self.is_running():
                msg_or_none = await interruptable_receive_local_message(JOB_REQUEST_CHANNEL, stop_event=self._stop_event)
                if msg_or_none is not None:
                    try:
                        job_request = pydantic.TypeAdapter(OrganicJobRequest).validate_json(msg_or_none)
                    except pydantic.ValidationError:
                        await log_sytem_error_event(
                            message=f"Invalid job request received from facilitator: {msg_or_none}",
                            type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                            subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                            logger=logger,
                        )
                        continue
                    # TODO: Send to job dispatcher
        except asyncio.CancelledError:
            pass

    async def _cheated_job_report_handler(self) -> None:
        """
        Listens for messages on the local cheated job reports channel and processes them.
        """
        try:
            while self.is_running():
                msg_or_none = await interruptable_receive_local_message(CHEATED_JOB_REPORT_CHANNEL, stop_event=self._stop_event)
                if msg_or_none is not None:
                    try:
                        cheated_job_report = pydantic.TypeAdapter(V0JobCheated).validate_json(msg_or_none)
                    except pydantic.ValidationError:
                        await log_sytem_error_event(
                            message=f"Invalid cheated job report received from facilitator: {msg_or_none}",
                            type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                            subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                            logger=logger,
                        )
                    # TODO: Process cheated job report
        except asyncio.CancelledError:
            pass

    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    async def start(self) -> None:
        if self.is_running():
            return
            
        self._stop_event.clear()
        self._job_request_listener_task = asyncio.create_task(self._job_request_handler)
        self._cheated_job_report_listener_task = asyncio.create_task(self._cheated_job_report_handler)

    async def stop(self) -> None:
        if not self.is_running():
            return
        self._stop_event.set()
        
        try:
            await stop_task_gracefully(self._job_request_listener_task)
        except Exception as exc:
            logger.error("Error stopping job request listener task: %s: %s", type(exc).__name__, exc)

        try:
            await stop_task_gracefully(self._cheated_job_report_listener_task)
        except Exception as exc:
            logger.error("Error stopping cheated job report listener task: %s: %s", type(exc).__name__, exc)

        self._job_request_listener_task = None
        self._cheated_job_report_listener_task = None