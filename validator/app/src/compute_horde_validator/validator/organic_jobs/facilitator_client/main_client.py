import asyncio
import pydantic
import logging
from typing import Callable, Awaitable
from pydantic import BaseModel
from django.conf import settings
from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest, V0JobCheated
from compute_horde_validator.validator.models import SystemEvent
from celery import AsyncResult
from .constants import JOB_REQUEST_CHANNEL, CHEATED_JOB_REPORT_CHANNEL
from .util import interruptible_receive_local_message, log_system_error_event, stop_task_gracefully
from .jobs import job_request_task, process_miner_cheat_report
from .exceptions import LocalChannelReceiveError

logger = logging.getLogger(__name__)


class FacilitatorClient:
    """
    Handles job requests and cheated job reports sent from the facilitator.
    """

    def __init__(self) -> None:
        self._stop_event = asyncio.Event()
        self._stop_event.set()  # Start stopped
        self._job_request_listener_task: asyncio.Task | None = None
        self._cheated_job_report_listener_task: asyncio.Task | None = None

    async def _job_request_handler(self) -> None:
        """
        Listens for messages on the local job requests channel and forwards these to the job dispatcher.
        """
        while self.is_running():
            try:
                msg_or_none = await interruptible_receive_local_message(JOB_REQUEST_CHANNEL, stop_event=self._stop_event)
                if msg_or_none is not None:
                    job_request: OrganicJobRequest = OrganicJobRequest.model_validate(msg_or_none)
                    job_request_task.delay(job_request.model_dump_json())
            except asyncio.CancelledError:
                self._stop_event.set()
                break
            except LocalChannelReceiveError as exc:
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.MESSAGE_RECEIVE_ERROR,
                    logger=logger,
                )
            except pydantic.ValidationError:
                await log_system_error_event(
                    message=f"Invalid job request received from facilitator: {msg_or_none}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                    logger=logger,
                )
            except Exception as exc:
                await log_system_error_event(
                    message=f"Error handling job request:: {type(exc).__name__}: {exc}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                    logger=logger,
                )

    async def _cheated_job_report_handler(self) -> None:
        """
        Listens for messages on the local cheated job reports channel and processes them.
        """
        while self.is_running():
            try:
                msg_or_none = await interruptible_receive_local_message(CHEATED_JOB_REPORT_CHANNEL, stop_event=self._stop_event)
                if msg_or_none is not None:
                    cheated_job_report = V0JobCheated.model_validate(msg_or_none)
                    await process_miner_cheat_report(cheated_job_report)
            except asyncio.CancelledError:
                self._stop_event.set()
                break
            except LocalChannelReceiveError as exc:
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.MESSAGE_RECEIVE_ERROR,
                    logger=logger,
                )
            except pydantic.ValidationError:
                await log_system_error_event(
                    message=f"Invalid cheated job report received from facilitator: {msg_or_none}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                    logger=logger,
                )
            except Exception as exc:
                await log_system_error_event(
                    message=f"Error handling cheated job report:: {type(exc).__name__}: {exc}",
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
        self._job_request_listener_task = asyncio.create_task(self._job_request_handler())
        self._cheated_job_report_listener_task = asyncio.create_task(self._cheated_job_report_handler())

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