import asyncio
import datetime as dt
import logging
import time
import uuid
from datetime import timedelta
from functools import cached_property
from typing import Protocol, assert_never

import bittensor
from compute_horde.executor_class import EXECUTOR_CLASS
from compute_horde.protocol_consts import HordeFailureReason, JobParticipantType, JobRejectionReason
from compute_horde.protocol_messages import (
    GenericError,
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutionDoneRequest,
    V0ExecutorReadyRequest,
    V0HordeFailedRequest,
    V0InitialJobRequest,
    V0JobAcceptedReceiptRequest,
    V0JobFailedRequest,
    V0JobFinishedReceiptRequest,
    V0JobFinishedRequest,
    V0JobRequest,
    V0MachineSpecsRequest,
    V0StreamingJobReadyRequest,
    V0VolumesReadyRequest,
    ValidatorAuthForMiner,
    ValidatorToMinerMessage,
)
from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import JobStartedReceiptPayload, ReceiptPayload
from django.conf import settings
from django.db.models import DateTimeField, ExpressionWrapper, F
from django.utils import timezone
from pydantic import TypeAdapter

from compute_horde_miner.miner.executor_manager import current
from compute_horde_miner.miner.executor_manager.base import (
    AllExecutorsBusy,
    ExecutorUnavailable,
)
from compute_horde_miner.miner.miner_consumer.base_compute_horde_consumer import (
    BaseConsumer,
    log_errors_explicitly,
)
from compute_horde_miner.miner.miner_consumer.layer_utils import ValidatorInterfaceMixin
from compute_horde_miner.miner.models import (
    AcceptedJob,
    Validator,
    ValidatorBlacklist,
)
from compute_horde_miner.miner.receipts import current_store

logger = logging.getLogger(__name__)

AUTH_MESSAGE_MAX_AGE = 10

DONT_CHECK = "DONT_CHECK"


class _RequestWithBlobForSigning(Protocol):
    def blob_for_signing(self) -> str: ...


def get_miner_signature(msg: _RequestWithBlobForSigning) -> str:
    keypair = settings.BITTENSOR_WALLET().get_hotkey()
    return f"0x{keypair.sign(msg.blob_for_signing()).hex()}"


class MinerValidatorConsumer(BaseConsumer[ValidatorToMinerMessage], ValidatorInterfaceMixin):
    class NotInitialized(Exception):
        pass

    class MissingExecutorToken(Exception):
        pass

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        if settings.DEBUG_TURN_AUTHENTICATION_OFF or settings.IS_LOCAL_MINER:
            self.my_hotkey = DONT_CHECK
        else:
            self.my_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
        self.validator_authenticated = False
        self.msg_queue = []
        self.defer_saving_jobs = []
        self.defer_executor_ready = []
        self.pending_jobs: dict[str, AcceptedJob] = {}

        # Validator is populated only after the connection initialization succeeds
        self._maybe_validator: Validator | None = None

    @cached_property
    def validator(self) -> Validator:
        if not self._maybe_validator:
            raise MinerValidatorConsumer.NotInitialized("Missing validator")
        return self._maybe_validator

    @cached_property
    def validator_key(self) -> str:
        return str(self.scope["url_route"]["kwargs"]["validator_key"])

    @log_errors_explicitly
    async def connect(self):
        await super().connect()
        # TODO verify ssl cert

        try:
            self._maybe_validator = await Validator.objects.aget(public_key=self.validator_key)
        except Validator.DoesNotExist:
            await self.close_with_error_msg(f"Unknown validator: {self.validator_key}")
            return

        if not (self.validator.active or settings.DEBUG_TURN_AUTHENTICATION_OFF):
            await self.close_with_error_msg(f"Inactive validator: {self.validator_key}")
            return

        self.pending_jobs = await AcceptedJob.get_for_validator(self.validator)
        for job in self.pending_jobs.values():
            if job.status != AcceptedJob.Status.WAITING_FOR_PAYLOAD:
                # TODO: this actually works only for temporary connection issue between validator and miner;
                #       long running jobs would need to update job in regular periods, and actually would
                #       require complete refactor of communication scheme, so miner and validator can restart
                #       and still rebuild the connection and handle finished job execution... but right now
                #       losing connection between miner and executor is not recoverable, also restart of
                #       either validator or miner is unrecoverable, so when reading this take into account that
                #       this only handles this one particular case of broken connection between miner and validator
                if timezone.now() - job.updated_at > dt.timedelta(days=1):
                    job.status = AcceptedJob.Status.FAILED
                    # we don't want to block accepting connection - we defer it until after authorized
                    self.defer_saving_jobs.append(job)
                    logger.debug(f"Give up on job {job.job_uuid} after no status change for a day.")
                else:
                    await self.group_add(job.executor_token)
                continue
            if timezone.now() - job.updated_at > dt.timedelta(minutes=10):
                job.status = AcceptedJob.Status.FAILED
                # we don't want to block accepting connection - we defer it until after authorized
                self.defer_saving_jobs.append(job)
                logger.debug(
                    f"Give up on job {job.job_uuid} after not receiving payload after 10 minutes"
                )
            else:
                logger.debug(
                    f"Deferring executor ready msg for job {job.job_uuid}: {job.executor_token}"
                )
                await self.group_add(job.executor_token)
                # we don't send anything until we get authorization confirmation
                self.defer_executor_ready.append(job)

    async def close_with_error_msg(self, msg: str):
        await self.send(GenericError(details=msg).model_dump_json())
        logger.info(msg)
        await self.close(1000)

    def verify_auth_msg(self, msg: ValidatorAuthForMiner) -> tuple[bool, str]:
        if msg.timestamp < time.time() - AUTH_MESSAGE_MAX_AGE:
            return False, "msg too old"
        if not settings.IS_LOCAL_MINER and msg.miner_hotkey != self.my_hotkey:
            return False, f"wrong miner hotkey ({self.my_hotkey}!={msg.miner_hotkey})"
        if msg.validator_hotkey != self.validator_key:
            return (
                False,
                f"wrong validator hotkey ({self.validator_key}!={msg.validator_hotkey})",
            )

        keypair = bittensor.Keypair(ss58_address=self.validator_key)
        if keypair.verify(msg.blob_for_signing(), msg.signature):
            return True, ""

        return False, "Signature mismatches"

    def verify_receipt_payload(self, payload: ReceiptPayload, signature: str) -> bool:
        if settings.IS_LOCAL_MINER:
            return True

        if self.my_hotkey != DONT_CHECK and payload.miner_hotkey != self.my_hotkey:
            logger.warning(
                f"Miner hotkey mismatch in receipt for job_uuid {payload.job_uuid} ({payload.miner_hotkey!r} != {self.my_hotkey!r})"
            )
            return False
        if payload.validator_hotkey != self.validator_key:
            logger.warning(
                f"Validator hotkey mismatch in receipt for job_uuid {payload.job_uuid} ({payload.validator_hotkey!r} != {self.validator_key!r})"
            )
            return False

        keypair = bittensor.Keypair(ss58_address=self.validator_key)
        if keypair.verify(payload.blob_for_signing(), signature):
            return True

        logger.warning(f"Validator signature mismatch in receipt for job_uuid {payload.job_uuid}")
        return False

    async def handle_authentication(self, msg: ValidatorAuthForMiner):
        if settings.DEBUG_TURN_AUTHENTICATION_OFF:
            logger.critical(
                f"Validator {self.validator_key} passed authentication without checking, because "
                f'"DEBUG_TURN_AUTHENTICATION_OFF" is on'
            )
        else:
            authenticated, auth_error_msg = self.verify_auth_msg(msg)
            if not authenticated:
                close_error_msg = (
                    f"Validator {self.validator_key} not authenticated due to: {auth_error_msg}"
                )
                await self.close_with_error_msg(close_error_msg)
                return

        self.validator_authenticated = True

        # Handle messages that may have arrived during the authentication
        for msg in self.msg_queue:
            await self.handle(msg)

        # TODO(post error propagation): decide if this is needed at all
        # disabled for now because this would need a migration and is likely not worth it
        # for job in await AcceptedJob.get_not_reported(self.validator):
        #     if job.status == AcceptedJob.Status.FINISHED:
        #         await self.send(
        #             V0JobFinishedRequest(
        #                 job_uuid=str(job.job_uuid),
        #                 docker_process_stdout=job.stdout,
        #                 docker_process_stderr=job.stderr,
        #                 artifacts=job.artifacts,
        #                 upload_results=job.upload_results,
        #             ).model_dump_json()
        #         )
        #         logger.debug(
        #             f"Job {job.job_uuid} finished reported to validator {self.validator_key}"
        #         )
        #     else:  # job.status == AcceptedJob.Status.FAILED:
        #         await self.send(
        #             V0JobFailedRequest(
        #                 job_uuid=str(job.job_uuid),
        #                 docker_process_stdout=job.stdout,
        #                 docker_process_stderr=job.stderr,
        #                 docker_process_exit_status=job.exit_status,
        #                 error_type=JobFailureReason(job.error_type)
        #                 if job.error_type
        #                 else None,
        #                 error_detail=job.error_detail,
        #             ).model_dump_json()
        #         )
        #         logger.debug(
        #             f"Failed job {job.job_uuid} reported to validator {self.validator_key}"
        #         )
        #     job.result_reported_to_validator = timezone.now()
        #     await job.asave()

        while self.defer_executor_ready:
            job = self.defer_executor_ready.pop()
            await self.send(V0ExecutorReadyRequest(job_uuid=str(job.job_uuid)).model_dump_json())
            logger.debug(
                f"Readiness for job {job.job_uuid} reported to validator {self.validator_key}"
            )

        # we could do this anywhere, but this sounds like a good enough place
        while self.defer_saving_jobs:
            job = self.defer_saving_jobs.pop()
            await job.asave()

    def parse_message(self, raw_msg: str | bytes) -> ValidatorToMinerMessage:
        return TypeAdapter(ValidatorToMinerMessage).validate_json(raw_msg)

    async def handle(self, msg: ValidatorToMinerMessage) -> None:
        if self._maybe_validator is None:
            # An unknown validator should have received an error response from connect() by now.
            # All further incoming messages can be ignored.
            logger.warning(
                f"Dropping message {msg.__class__.__name__} from unknown validator {self.validator_key}"
            )
            return

        logger.debug(f"Received from validator {self.validator_key}: {msg.__class__.__name__}")

        if isinstance(msg, ValidatorAuthForMiner):
            await self.handle_authentication(msg)
            return

        if not self.validator_authenticated:
            self.msg_queue.append(msg)
            return

        if isinstance(msg, V0InitialJobRequest) or isinstance(msg, V0JobRequest):
            # Proactively check volume safety in both requests that may contain a volume
            if msg.volume and not msg.volume.is_safe():
                error_msg = f"Received JobRequest with unsafe volume: {msg.volume}"
                logger.error(error_msg)
                await self.send(GenericError(details=error_msg).model_dump_json())
                return

        if isinstance(msg, V0InitialJobRequest):
            await self.handle_initial_job_request(msg)
            return

        if isinstance(msg, V0JobRequest):
            await self.handle_job_request(msg)
            return

        if isinstance(msg, V0JobAcceptedReceiptRequest):
            await self.handle_job_accepted_receipt(msg)
            return

        if isinstance(msg, V0JobFinishedReceiptRequest):
            await self.handle_job_finished_receipt(msg)
            return

        if isinstance(msg, GenericError):
            logger.error(f"Received GenericError from validator: {msg.details}")
            # Nothing to do here - this doesn't tell us whether the job is dead.
            return

        assert_never(msg)

    async def handle_initial_job_request(self, msg: V0InitialJobRequest):
        validator_blacklisted = await ValidatorBlacklist.objects.filter(
            validator=self.validator
        ).aexists()
        if validator_blacklisted:
            logger.info(
                f"Declining job {msg.job_uuid} from blacklisted validator: {self.validator_key}"
            )
            await self.send(
                V0DeclineJobRequest(
                    job_uuid=msg.job_uuid,
                    reason=JobRejectionReason.VALIDATOR_BLACKLISTED,
                ).model_dump_json()
            )
            return

        await self.handle_job_started_receipt(
            msg.job_started_receipt_payload, msg.job_started_receipt_signature
        )

        # TODO add rate limiting per validator key here
        executor_token = f"{msg.job_uuid}-{uuid.uuid4()}"
        await self.group_add(executor_token)
        # let's create the job object before spinning up the executor, so if this process dies before getting
        # confirmation from the executor_manager the object is there and the executor will get the job details
        job = AcceptedJob(
            validator=self.validator,
            job_uuid=msg.job_uuid,
            executor_token=executor_token,
            initial_job_details=msg.model_dump(),
            status=AcceptedJob.Status.WAITING_FOR_EXECUTOR,
        )
        await job.asave()
        self.pending_jobs[msg.job_uuid] = job

        # This will reserve AND start the executor.
        # Note: the timeout is only passed to the vendor executor manager for reference.
        # This will NOT time out by itself.
        executor_spinup = asyncio.create_task(
            current.executor_manager.reserve_executor_class(
                executor_token,
                msg.executor_class,
                timeout=EXECUTOR_CLASS[msg.executor_class].spin_up_time,
            ),
        )

        try:
            # Wait for a signal from the executor_manager that the executor is reserved.
            # This should be a fast operation, so the validator only waits for a short time for this.
            await current.executor_manager.wait_for_executor_reservation(
                executor_token,
                msg.executor_class,
            )

            # If there is no executor, the above future throws an appropriate exception,
            # so we will never proceed further down.
            await self.send(V0AcceptJobRequest(job_uuid=msg.job_uuid).model_dump_json())

            # Then, wait for the executor to actually start up.
            executor = await executor_spinup
            executor_address = await current.executor_manager.get_executor_public_address(executor)
            job.executor_address = executor_address
            await job.asave()

        except AllExecutorsBusy:
            await self.group_discard(executor_token)
            job.status = AcceptedJob.Status.REJECTED
            await job.asave()
            self.pending_jobs.pop(msg.job_uuid)
            now = msg.job_started_receipt_payload.timestamp
            queryset = (
                JobStartedReceipt.objects.annotate(
                    valid_until=ExpressionWrapper(
                        F("timestamp") + F("ttl") * timedelta(seconds=1),
                        output_field=DateTimeField(),
                    ),
                )
                .filter(
                    is_organic=True,
                    executor_class=msg.executor_class,
                    timestamp__lte=now,
                    valid_until__gte=now,
                    miner_signature__isnull=False,  # miner signature is needed to build a valid Receipt
                )
                .exclude(
                    job_uuid=msg.job_uuid,  # UUIDField doesn't support "__ne=..."
                )
            )
            receipts = [receipt async for receipt in queryset]
            logger.info(
                f"Declining job {msg.job_uuid}: all executors busy. Sending {len(receipts)} excuse receipts:"
            )
            for receipt in receipts:
                valid_until = receipt.timestamp + timedelta(seconds=receipt.ttl)
                logger.debug(
                    f"Receipt for job {receipt.job_uuid} from validator {receipt.validator_hotkey} valid until {valid_until}"
                )
            await self.send(
                V0DeclineJobRequest(
                    job_uuid=msg.job_uuid,
                    reason=JobRejectionReason.BUSY,
                    receipts=[r.to_receipt() for r in receipts],
                ).model_dump_json()
            )

        except ExecutorUnavailable:
            await self.group_discard(executor_token)
            job.status = AcceptedJob.Status.FAILED
            await job.asave()
            self.pending_jobs.pop(msg.job_uuid)
            logger.info(f"Failing job {msg.job_uuid}: executor failed to start")
            await self.send(
                V0HordeFailedRequest(
                    job_uuid=msg.job_uuid,
                    reported_by=JobParticipantType.MINER,
                    reason=HordeFailureReason.EXECUTOR_SPINUP_FAILED,
                    message="Executor failed to start",
                ).model_dump_json()
            )

        finally:
            # In any case, the spinup task must be cleaned up.
            executor_spinup.cancel()
            try:
                await executor_spinup
            except (asyncio.CancelledError, Exception):
                # As we're awaiting this task for the second time, just silence the exceptions as
                # they have been already thrown during the previous await.
                pass

    async def handle_job_request(self, msg: V0JobRequest):
        job = self.pending_jobs.get(msg.job_uuid)
        if job is None:
            error_msg = f"Received JobRequest for unknown job_uuid: {msg.job_uuid}"
            logger.error(error_msg)
            await self.send(GenericError(details=error_msg).model_dump_json())
            return

        if job.initial_job_details.get("volume") is not None and msg.volume is not None:
            # The volume may have been already sent in the initial job request.
            error_msg = f"Received job volume twice job_uuid: {msg.job_uuid}"
            logger.error(error_msg)
            await self.send(GenericError(details=error_msg).model_dump_json())
            return

        await self.send_job_request(job.executor_token, msg)
        logger.debug(f"Passing job details to executor consumer job_uuid: {msg.job_uuid}")
        job.status = AcceptedJob.Status.RUNNING
        job.full_job_details = msg.model_dump()
        await job.asave()

    async def handle_job_started_receipt(self, payload: JobStartedReceiptPayload, signature: str):
        logger.info(
            f"Received job started receipt for"
            f" job_uuid={payload.job_uuid} validator_hotkey={payload.validator_hotkey}"
        )

        if settings.IS_LOCAL_MINER:
            return

        if not self.verify_receipt_payload(payload, signature):
            return

        receipt = await JobStartedReceipt.objects.acreate(
            job_uuid=payload.job_uuid,
            validator_hotkey=payload.validator_hotkey,
            miner_hotkey=payload.miner_hotkey,
            validator_signature=signature,
            miner_signature=get_miner_signature(payload),
            timestamp=payload.timestamp,
            executor_class=payload.executor_class,
            is_organic=payload.is_organic,
            ttl=payload.ttl,
        )
        (await current_store()).store([receipt.to_receipt()])

    async def handle_job_accepted_receipt(self, msg: V0JobAcceptedReceiptRequest):
        logger.info(
            f"Received job accepted receipt for"
            f" job_uuid={msg.payload.job_uuid} validator_hotkey={msg.payload.validator_hotkey}"
        )

        if not self.verify_receipt_payload(msg.payload, msg.signature):
            logger.error("Receipt could not be verified")
            return

        if settings.IS_LOCAL_MINER:
            return

        created_receipt = await JobAcceptedReceipt.objects.acreate(
            validator_signature=msg.signature,
            miner_signature=get_miner_signature(msg),
            job_uuid=msg.payload.job_uuid,
            miner_hotkey=msg.payload.miner_hotkey,
            validator_hotkey=msg.payload.validator_hotkey,
            timestamp=msg.payload.timestamp,
            time_accepted=msg.payload.time_accepted,
            ttl=msg.payload.ttl,
        )

        (await current_store()).store([created_receipt.to_receipt()])

    async def handle_job_finished_receipt(self, msg: V0JobFinishedReceiptRequest):
        logger.info(
            f"Received job finished receipt for"
            f" job_uuid={msg.payload.job_uuid} validator_hotkey={msg.payload.validator_hotkey}"
            f" time_took={msg.payload.time_took} score={msg.payload.score}"
        )

        if not self.verify_receipt_payload(msg.payload, msg.signature):
            logger.error("Receipt could not be verified")
            return

        job = await AcceptedJob.objects.aget(job_uuid=msg.payload.job_uuid)
        job.time_took = msg.payload.time_took
        job.score = msg.payload.score
        await job.asave()

        if settings.IS_LOCAL_MINER:
            return

        created_receipt = await JobFinishedReceipt.objects.acreate(
            validator_signature=msg.signature,
            miner_signature=get_miner_signature(msg),
            job_uuid=msg.payload.job_uuid,
            miner_hotkey=msg.payload.miner_hotkey,
            validator_hotkey=msg.payload.validator_hotkey,
            timestamp=msg.payload.timestamp,
            time_started=msg.payload.time_started,
            time_took_us=msg.payload.time_took_us,
            score_str=msg.payload.score_str,
        )

        (await current_store()).store([created_receipt.to_receipt()])

    async def _executor_ready(self, msg: V0ExecutorReadyRequest):
        logger.debug(f"_executor_ready for {msg}")
        if self.check_missing_token(msg):
            return
        job = await AcceptedJob.objects.aget(executor_token=msg.executor_token)
        job_uuid = str(job.job_uuid)
        assert job_uuid == msg.job_uuid
        self.pending_jobs[job_uuid] = job
        await self.send(msg.model_copy(update={"executor_token": None}).model_dump_json())
        logger.debug(f"Readiness for job {job_uuid} reported to validator {self.validator_key}")

    async def _streaming_job_ready(self, msg: V0StreamingJobReadyRequest):
        if self.check_missing_token(msg):
            return
        job = await AcceptedJob.objects.aget(executor_token=msg.executor_token)
        job_uuid = str(job.job_uuid)
        assert job_uuid == msg.job_uuid
        new_msg = msg.model_copy(
            update={
                "executor_token": None,
                "miner_signature": get_miner_signature(msg),
            }
        )
        await self.send(new_msg.model_dump_json())
        logger.debug(
            f"Streaming readiness for job {job_uuid} reported to validator {self.validator_key}"
        )

    def check_missing_token(self, msg: V0ExecutorReadyRequest | V0StreamingJobReadyRequest):
        if msg.executor_token is None:
            raise MinerValidatorConsumer.MissingExecutorToken(
                f"Validator Client received {msg} with missing executor_token"
            )

    async def _executor_finished(self, msg: V0JobFinishedRequest):
        await self.send(msg.model_dump_json())
        logger.debug(f"Finished job {msg.job_uuid} reported to validator {self.validator_key}")
        job = self.pending_jobs.pop(msg.job_uuid)
        await job.arefresh_from_db()
        job.result_reported_to_validator = timezone.now()
        await job.asave()

    async def _executor_specs(self, msg: V0MachineSpecsRequest):
        await self.send(msg.model_dump_json())
        logger.debug(
            f"Reported specs for job {msg.job_uuid}: {msg.specs} to validator {self.validator_key}"
        )

    async def _job_rejected(self, msg: V0DeclineJobRequest):
        await self.send(msg.model_dump_json())
        logger.debug(f"Rejected job {msg.job_uuid} reported to validator {self.validator_key}")
        job = self.pending_jobs.pop(msg.job_uuid)
        await job.arefresh_from_db()
        job.result_reported_to_validator = timezone.now()
        await job.asave()

    async def _job_failed(self, msg: V0JobFailedRequest):
        await self.send(msg.model_dump_json())
        logger.debug(f"Failed job {msg.job_uuid} reported to validator {self.validator_key}")
        job = self.pending_jobs.pop(msg.job_uuid)
        await job.arefresh_from_db()
        job.result_reported_to_validator = timezone.now()
        await job.asave()

    async def _horde_failed(self, msg: V0HordeFailedRequest):
        await self.send(msg.model_dump_json())
        logger.debug(
            f"Horde failed at job {msg.job_uuid} reported to validator {self.validator_key}"
        )
        job = self.pending_jobs.pop(msg.job_uuid)
        await job.arefresh_from_db()
        job.result_reported_to_validator = timezone.now()
        await job.asave()

    async def _volumes_ready(self, msg: V0VolumesReadyRequest):
        await self.send(msg.model_dump_json())

    async def _execution_done(self, msg: V0ExecutionDoneRequest):
        await self.send(msg.model_dump_json())

    async def disconnect(self, close_code):
        logger.info(f"Validator {self.validator_key} disconnected")
