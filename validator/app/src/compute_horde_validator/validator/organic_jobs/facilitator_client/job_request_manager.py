import asyncio

import pydantic
import sentry_sdk
from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest, V0JobCheated

from compute_horde_validator.validator.models import SystemEvent

from .base import BaseComponent
from .constants import CHEATED_JOB_REPORT_CHANNEL, JOB_REQUEST_CHANNEL, WAIT_ON_ERROR_INTERVAL
from .exceptions import LocalChannelReceiveError
from .jobs_task import job_request_task, process_miner_cheat_report
from .metrics import VALIDATOR_FC_COMPONENT_STATE
from .util import (
    interruptible_receive_local_message,
    interruptible_wait,
    log_system_error_event,
    stop_task_gracefully,
)


class FacilitatorClientJobRequestManager(BaseComponent):
    """
    Handles job requests and cheated job reports sent from the facilitator.
    """

    def __init__(self) -> None:
        super().__init__()
        self._job_request_listener_task: asyncio.Task | None = None
        self._cheated_job_report_listener_task: asyncio.Task | None = None

    async def _job_request_handler(self) -> None:
        """
        Listens for messages on the local job requests channel and forwards these to the job dispatcher.
        """
        while self.is_running():
            try:
                msg_or_none = await interruptible_receive_local_message(
                    JOB_REQUEST_CHANNEL, stop_event=self._stop_event
                )
                if msg_or_none is not None:
                    job_request: OrganicJobRequest = OrganicJobRequest.model_validate(msg_or_none)
                    job_request_task.delay(job_request.model_dump_json())
            except asyncio.CancelledError:
                self._stop_event.set()
                VALIDATOR_FC_COMPONENT_STATE.labels(component=self.name).set(0)
                break
            except LocalChannelReceiveError as exc:
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.MESSAGE_RECEIVE_ERROR,
                    logger=self._logger,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except pydantic.ValidationError:
                await log_system_error_event(
                    message=f"Invalid job request received from facilitator: {msg_or_none}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                    logger=self._logger,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                await log_system_error_event(
                    message=f"Error handling job request: {type(exc).__name__}: {exc}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                    logger=self._logger,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )

    async def _cheated_job_report_handler(self) -> None:
        """
        Listens for messages on the local cheated job reports channel and processes them.
        """
        while self.is_running():
            try:
                msg_or_none = await interruptible_receive_local_message(
                    CHEATED_JOB_REPORT_CHANNEL, stop_event=self._stop_event
                )
                if msg_or_none is not None:
                    cheated_job_report = V0JobCheated.model_validate(msg_or_none)
                    await process_miner_cheat_report(cheated_job_report)
            except asyncio.CancelledError:
                self._stop_event.set()
                VALIDATOR_FC_COMPONENT_STATE.labels(component=self.name).set(0)
                break
            except LocalChannelReceiveError as exc:
                await log_system_error_event(
                    message=str(exc),
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.MESSAGE_RECEIVE_ERROR,
                    logger=self._logger,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except pydantic.ValidationError:
                await log_system_error_event(
                    message=f"Invalid cheated job report received from facilitator: {msg_or_none}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
                    logger=self._logger,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )
            except Exception as exc:
                sentry_sdk.capture_exception(exc)
                await log_system_error_event(
                    message=f"Error handling cheated job report: {type(exc).__name__}: {exc}",
                    event_type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                    event_subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                    logger=self._logger,
                )
                await interruptible_wait(
                    timeout=WAIT_ON_ERROR_INTERVAL, stop_event=self._stop_event
                )

    async def start(self) -> None:
        """Starts the main client."""
        if self.is_running():
            return

        await super().start()

        self._job_request_listener_task = asyncio.create_task(self._job_request_handler())
        self._cheated_job_report_listener_task = asyncio.create_task(
            self._cheated_job_report_handler()
        )

    async def stop(self) -> None:
        """Stops the main client."""
        if not self.is_running():
            return

        await super().stop()

        try:
            await stop_task_gracefully(self._job_request_listener_task)
            self._job_request_listener_task = None
        except Exception as exc:
            self._logger.error(
                "Error stopping job request listener task: %s: %s", type(exc).__name__, exc
            )

        try:
            await stop_task_gracefully(self._cheated_job_report_listener_task)
            self._cheated_job_report_listener_task = None
        except Exception as exc:
            self._logger.error(
                "Error stopping cheated job report listener task: %s: %s", type(exc).__name__, exc
            )
