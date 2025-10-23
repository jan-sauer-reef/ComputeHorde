from celery import Celery, Task
from compute_horde_validator.celery import app
import asyncio
import logging
import os
from collections import deque
from typing import Any, Literal

from asgiref.sync import async_to_sync
import bittensor_wallet
import httpx
import pydantic
import sentry_sdk
import tenacity
import websockets
from channels.layers import get_channel_layer
from compute_horde.fv_protocol.facilitator_requests import (
    Error,
    OrganicJobRequest,
    Response,
    V0JobCheated,
    V2JobRequest,
)
from compute_horde.fv_protocol.validator_requests import (
    HordeFailureDetails,
    JobFailureDetails,
    JobRejectionDetails,
    JobStatusMetadata,
    JobStatusUpdate,
    V0AuthenticationRequest,
    V0Heartbeat,
    V0MachineSpecsUpdate,
)
from compute_horde.job_errors import HordeError
from compute_horde.protocol_consts import (
    HordeFailureReason,
    JobFailureReason,
    JobParticipantType,
    JobRejectionReason,
    JobStage,
    JobStatus,
)
from compute_horde.protocol_messages import FailureContext
from compute_horde_core.signature import SignedRequest, verify_signature
from django.conf import settings
from pydantic import BaseModel

from compute_horde_validator.validator.allowance.types import NotEnoughAllowanceException
from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import (
    MinerBlacklist,
    OrganicJob,
    SystemEvent,
    ValidatorWhitelist,
)
from compute_horde_validator.validator.organic_jobs import blacklist
from compute_horde_validator.validator.organic_jobs.blacklist import report_miner_failed_job
from compute_horde_validator.validator.routing.default import routing
from compute_horde_validator.validator.routing.types import JobRoutingException
from compute_horde_validator.validator.tasks import (
    execute_organic_job_request_on_worker,
    slash_collateral_task,
)
from compute_horde_validator.validator.utils import MACHINE_SPEC_CHANNEL
from .constants import JOB_STATUS_UPDATE_CHANNEL, LOCAL_MESSAGE_SEND_TIMEOUT
from .util import safe_send_local_message

logger = logging.getLogger(__name__)


class JobRequestVerificationFailed(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class InvalidJobRequestFormat(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)

async def verify_request_or_fail(job_request: SignedRequest) -> None:
    """
    Check that the signer is in validator whitelist and that the signature is
    valid.
    """
    if job_request.signature is None:
        raise JobRequestVerificationFailed("Signature is empty")

    signature = job_request.signature
    signer = signature.signatory
    signed_payload = job_request.get_signed_payload()

    my_keypair = settings.BITTENSOR_WALLET().get_hotkey()
    if signer != my_keypair.ss58_address:
        whitelisted = await ValidatorWhitelist.objects.filter(hotkey=signer).aexists()
        if not whitelisted:
            raise JobRequestVerificationFailed(f"Signatory {signer} is not in validator whitelist")

    try:
        verify_signature(signed_payload, signature)
    except Exception as e:
        raise JobRequestVerificationFailed("Bad signature") from e


async def process_miner_cheat_report(cheated_job_request: V0JobCheated) -> None:
    """
    Process a cheated job report and blacklist the miner.
    """
    try:
        await verify_request_or_fail(cheated_job_request)
    except Exception as e:
        logger.warning(f"Failed to verify signed payload: {e} - will ignore")
        return
    job_uuid = cheated_job_request.job_uuid
    try:
        job = await OrganicJob.objects.prefetch_related("miner").aget(job_uuid=job_uuid)
    except OrganicJob.DoesNotExist:
        logger.error(f"Job {job_uuid} reported for cheating does not exist")
        return

    if job.cheated:
        logger.warning(f"Job {job_uuid} already marked as cheated - ignoring")
        return

    if job.status != OrganicJob.Status.COMPLETED:
        logger.info(f"Job {job_uuid} reported for cheating is not complete yet")
        return

    job.cheated = True
    await job.asave()

    blacklist_time = await aget_config("DYNAMIC_JOB_CHEATED_BLACKLIST_TIME_SECONDS")
    await blacklist.blacklist_miner(
        job, MinerBlacklist.BlacklistReason.JOB_CHEATED, blacklist_time
    )
    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
        subtype=SystemEvent.EventSubType.JOB_CHEATED,
        long_description="Job was reported as cheated",
        data={
            "job_uuid": str(job.job_uuid),
            "miner_hotkey": job.miner.hotkey,
        },
    )

    if not job.slashed:
        slash_collateral_task.delay(str(job.job_uuid))


class JobRequestTask(Task):
    """
    A custom task base class that defines a callback in case a task fails.

    Any task that uses this base class MUST have the job request as the first argument!
    """
    def on_failure(self, exc, task_id, args, kwargs, einfo):

        try:
            job_request: OrganicJobRequest = pydantic.TypeAdapter(OrganicJobRequest).validate_json(kwargs["job_request"] if kwargs else args[0])
            job_uuid = job_request.uuid
        except pydantic.ValidationError:
            job_uuid = "UNKNOWN"  # uuid can't be parsed if the job request was mangled

        if isinstance(exc, InvalidJobRequestFormat):
            message = self._make_job_rejected_message(
                job_uuid=job_uuid,
                message=exc.message,
                rejected_by=JobParticipantType.VALIDATOR,
                reason=JobRejectionReason.INVALID_REQUEST_FORMAT,
            )        
        elif isinstance(exc, JobRequestVerificationFailed):
            message = self._make_job_rejected_message(
                job_uuid=job_uuid,
                message=exc.message,
                rejected_by=JobParticipantType.VALIDATOR,
                reason=JobRejectionReason.INVALID_SIGNATURE,
            )
        
        elif isinstance(exc, NotEnoughAllowanceException):
            message = self._make_job_rejected_message(
                job_uuid=job_uuid,
                message="Job could not be routed to a miner",
                rejected_by=JobParticipantType.VALIDATOR,
                reason=JobRejectionReason.NO_MINER_FOR_JOB,
                context={"exception_type": type(exc).__qualname__},
            )
        else:
            exc = HordeError.wrap_unhandled(exc)
            message = self._make_horde_failed_message(
                job_uuid=job_uuid,
                reported_by=JobParticipantType.VALIDATOR,
                message=exc.message,
                reason=exc.reason,
                context=exc.context,
            )
        
        async_to_sync(safe_send_local_message)(
            channel=JOB_STATUS_UPDATE_CHANNEL,
            message=message,
            logger=logger,
        )

    def _make_job_rejected_message(
        self,
        job_uuid: str,
        message: str,
        rejected_by: JobParticipantType,
        reason: JobRejectionReason,
        context: FailureContext | None = None,
    ) -> JobStatusUpdate:
        return JobStatusUpdate(
            uuid=job_uuid,
            status=JobStatus.REJECTED,
            metadata=JobStatusMetadata(
                job_rejection_details=JobRejectionDetails(
                    rejected_by=rejected_by,
                    reason=reason,
                    message=message,
                    context=context,
                ),
            ),
        )
    
    def _make_horde_failed_message(
        self,
        job_uuid: str,
        message: str,
        reported_by: JobParticipantType,
        reason: HordeFailureReason,
        context: FailureContext | None = None,
    ) -> JobStatusUpdate:
        return JobStatusUpdate(
            uuid=job_uuid,
            status=JobStatus.HORDE_FAILED,
            metadata=JobStatusMetadata(
                horde_failure_details=HordeFailureDetails(
                    reported_by=reported_by,
                    reason=reason,
                    message=message,
                    context=context,
                ),
            ),
        )


@app.task(base=JobRequestTask)
def job_request_task(job_request: str) -> None:
    """
    Select an appropriate miner for the task and submit the task to it.

    Args:
        job_request (str): The job request as a JSON string.
    """
    try:
        job_request: OrganicJobRequest = pydantic.TypeAdapter(OrganicJobRequest).validate_json(job_request)
    except pydantic.ValidationError:
        raise InvalidJobRequestFormat(f"Invalid job request format: {job_request}")

    async_to_sync(verify_request_or_fail)(job_request)

    # Notify facilitator that the job request has been received
    async_to_sync(safe_send_local_message)(
        channel=JOB_STATUS_UPDATE_CHANNEL,
        message=JobStatusUpdate(uuid=job_request.uuid, status=JobStatus.RECEIVED),
        logger=logger,
    )

    # Select an appropriate miner for the task and submit the task to it
    job_route = async_to_sync(routing)().pick_miner_for_job_request(job_request)
    logger.info(f"Selected miner {job_route.miner.hotkey_ss58} for job {job_request.uuid}")
    job = async_to_sync(execute_organic_job_request_on_worker)(job_request, job_route)
    logger.info(
        f"Job {job_request.uuid} finished with status: {job.status} (comment={job.comment})"
    )

    if job.status == OrganicJob.Status.FAILED:
        async_to_sync(report_miner_failed_job)(job)