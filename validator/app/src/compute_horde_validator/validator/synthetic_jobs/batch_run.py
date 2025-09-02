import asyncio
import logging
import math
import random
import statistics
import time
import traceback
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, assert_never

import aiohttp
import bittensor_wallet
import httpx
from asgiref.sync import sync_to_async
from channels.layers import get_channel_layer
from compute_horde.executor_class import EXECUTOR_CLASS
from compute_horde.miner_client.base import (
    AbstractMinerClient,
    UnsupportedMessageReceived,
)
from compute_horde.protocol_consts import JobRejectionReason
from compute_horde.protocol_messages import (
    GenericError,
    MinerToValidatorMessage,
    UnauthorizedError,
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0InitialJobRequest,
    V0JobAcceptedReceiptRequest,
    V0JobFailedRequest,
    V0JobFinishedReceiptRequest,
    V0JobFinishedRequest,
    V0JobRequest,
    V0MachineSpecsRequest,
    V0StreamingJobNotReadyRequest,
    V0StreamingJobReadyRequest,
    ValidatorAuthForMiner,
    ValidatorToMinerMessage,
)
from compute_horde.receipts import Receipt
from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.subtensor import get_peak_cycle
from compute_horde.transport import AbstractTransport, WSTransport
from compute_horde.transport.base import TransportConnectionError
from compute_horde.utils import ValidatorInfo, sign_blob
from compute_horde_core.certificate import generate_certificate_at
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.output_upload import OutputUpload
from compute_horde_core.streaming import StreamingDetails
from compute_horde_core.volume import Volume
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.db.models import BooleanField, Count, ExpressionWrapper, Q
from pydantic import BaseModel, JsonValue, TypeAdapter

from compute_horde_validator.validator import job_excuses
from compute_horde_validator.validator.dynamic_config import (
    LimitsDict,
    aget_config,
    get_default_executor_limits_for_missed_peak,
    get_miner_max_executors_per_class,
    get_streaming_job_executor_classes,
    get_system_event_limits,
)
from compute_horde_validator.validator.models import (
    Miner,
    MinerManifest,
    PromptSample,
    PromptSeries,
    SyntheticJob,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.receipts.default import receipts
from compute_horde_validator.validator.synthetic_jobs.generator import current
from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGenerator,
)
from compute_horde_validator.validator.synthetic_jobs.generator.llm_prompts import (
    LlmPromptsSyntheticJobGenerator,
)
from compute_horde_validator.validator.utils import MACHINE_SPEC_CHANNEL

logger = logging.getLogger(__name__)

_GIVE_AVERAGE_JOB_SEND_TIME_BONUS = False
_SEND_MACHINE_SPECS = False

# asyncio event loop profiling intervals
_LOOP_PROFILING_SLEEP_INTERVAL = 0.1
_LOOP_PROFILING_TIMEOUT_INTERVAL = 1

# always-on executor classes have spin_up_time=0, but realistically
# we need a bit more for all the back-and-forth messaging, especially
# when we talk with a lot of executors
_MIN_SPIN_UP_TIME = 2

_CLOSE_TIMEOUT = 1
_SEND_RECEIPT_TIMEOUT = 5
_SEND_MACHINE_SPECS_TIMEOUT = 5

# extra time to wait for a job response, so we can record the
# responses of slow executors.
# it is not taken into account when scoring, jobs will still
# fail if they take too long, but we'll know how long they took
# when debugging failed jobs
_JOB_RESPONSE_EXTRA_TIMEOUT = 2 * 60

# these two should match, the manifest timeout
# should be enough to fit max debounce count retries
_GET_MANIFEST_TIMEOUT = 35
_MAX_MINER_CLIENT_DEBOUNCE_COUNT = 4  # approximately 32 seconds

_LLM_ANSWERS_DOWNLOAD_MAX_ATTEMPTS = 5
_LLM_ANSWERS_DOWNLOAD_MAX_WORKERS = 100
_LLM_ANSWERS_DOWNLOAD_RETRY_MIN_BACKOFF = 0.2

# Celery job timeouts
SYNTHETIC_JOBS_SOFT_LIMIT = 20 * 60
SYNTHETIC_JOBS_HARD_LIMIT = SYNTHETIC_JOBS_SOFT_LIMIT + 10

# Executor class considered to be the one used for LLM-type jobs
LLM_EXECUTOR_CLASS = ExecutorClass.always_on__llm__a6000


class MinerClient(AbstractMinerClient[MinerToValidatorMessage, ValidatorToMinerMessage]):
    def __init__(
        self,
        ctx: "BatchContext",
        miner_hotkey: str,
        transport: AbstractTransport | None = None,
    ):
        self.ctx = ctx
        self.own_hotkey = ctx.own_keypair.ss58_address
        self.own_keypair = ctx.own_keypair

        self.miner_hotkey = miner_hotkey
        self.miner_address = ctx.miners[miner_hotkey].address
        self.miner_port = ctx.miners[miner_hotkey].port

        name = ctx.names[miner_hotkey]
        transport = transport or WSTransport(
            name, self.miner_url(), max_retries=_MAX_MINER_CLIENT_DEBOUNCE_COUNT
        )
        super().__init__(name, transport)

    def miner_url(self) -> str:
        return f"ws://{self.miner_address}:{self.miner_port}/v0.1/validator_interface/{self.own_hotkey}"

    def parse_message(self, raw_msg: str | bytes) -> MinerToValidatorMessage:
        return TypeAdapter(MinerToValidatorMessage).validate_json(raw_msg)

    async def handle_message(self, msg: MinerToValidatorMessage) -> None:
        if isinstance(msg, GenericError):
            logger.warning("%s error: %s", self.miner_name, msg.model_dump_json())
            is_unauthorized = msg.details is not None and msg.details.lower().startswith(
                ("unknown validator", "inactive validator")
            )
            subtype = (
                SystemEvent.EventSubType.UNAUTHORIZED
                if is_unauthorized
                else SystemEvent.EventSubType.GENERIC_ERROR
            )
            self.ctx.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=subtype,
                description=str(msg),
                miner_hotkey=self.miner_hotkey,
            )
            if is_unauthorized:
                # miner doesn't recognize our authority, close the connection to avoid retries
                logger.warning("%s closing connection", self.miner_name)
                await self.close()
            return

        if isinstance(msg, UnauthorizedError):
            logger.warning("%s unauthorized: %s %s", self.miner_name, msg.code, msg.details)
            self.ctx.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.UNAUTHORIZED,
                description=str(msg),
                miner_hotkey=self.miner_hotkey,
            )
            return

        job_uuid = getattr(msg, "job_uuid", None)
        if job_uuid is not None:
            job = self.ctx.jobs.get(job_uuid)
            if job is not None:
                job.handle_message(msg)
            else:
                logger.info("%s unexpected/old job: %s", self.miner_name, job_uuid)
            return

        raise UnsupportedMessageReceived(msg)

    def generate_authentication_message(self) -> ValidatorAuthForMiner:
        msg = ValidatorAuthForMiner(
            validator_hotkey=self.own_hotkey,
            miner_hotkey=self.miner_hotkey,
            timestamp=int(time.time()),
            signature="",
        )
        msg.signature = sign_blob(self.own_keypair, msg.blob_for_signing())
        return msg

    async def connect(self) -> None:
        await super().connect()
        await self.transport.send(self.generate_authentication_message().model_dump_json())

    async def send_check(self, data: str | bytes) -> None:
        await self.send(data, error_event_callback=self._handle_send_error_event)

    async def _handle_send_error_event(self, msg: str):
        self.ctx.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.MINER_SEND_ERROR,
            description=str(msg),
            miner_hotkey=self.miner_hotkey,
            func="send_check",
        )


@dataclass
class ExceptionInfo:
    exception: BaseException
    miner_hotkey: str
    job_uuid: str
    stage: str


@dataclass
class Job:
    ctx: "BatchContext"

    uuid: str
    # full name for easier debugging: "{miner_hotkey}({ip}:{port}) job {uuid}"
    name: str
    miner_hotkey: str
    executor_class: ExecutorClass
    job_generator: BaseSyntheticJobGenerator
    volume: Volume | None
    output_upload: OutputUpload | None

    # responses

    exception: BaseException | None = None
    # not-exact, approximate time since it's after asyncio.gather returns
    exception_time: datetime | None = None
    exception_stage: str | None = None

    accept_barrier_time: datetime | None = None
    accept_barrier_2_time: datetime | None = None
    accept_before_sent_time: datetime | None = None
    accept_after_sent_time: datetime | None = None
    accept_response: V0AcceptJobRequest | V0DeclineJobRequest | None = None
    accept_response_time: datetime | None = None
    accept_response_event: asyncio.Event = field(default_factory=asyncio.Event)

    executor_response: V0ExecutorFailedRequest | V0ExecutorReadyRequest | None = None
    executor_response_time: datetime | None = None
    executor_response_event: asyncio.Event = field(default_factory=asyncio.Event)

    # streaming job support
    streaming_job_ready_response: (
        V0StreamingJobReadyRequest | V0StreamingJobNotReadyRequest | None
    ) = None
    streaming_job_ready_response_time: datetime | None = None
    streaming_job_ready_response_event: asyncio.Event = field(default_factory=asyncio.Event)

    job_barrier_time: datetime | None = None
    job_before_sent_time: datetime | None = None
    job_after_sent_time: datetime | None = None
    job_response: V0JobFailedRequest | V0JobFinishedRequest | None = None
    job_response_time: datetime | None = None
    job_response_event: asyncio.Event = field(default_factory=asyncio.Event)

    machine_specs: V0MachineSpecsRequest | None = None

    # receipts
    job_started_receipt_payload: JobStartedReceiptPayload | None = None
    job_started_receipt_signature: str | None = None
    job_accepted_receipt: V0JobAcceptedReceiptRequest | None = None
    job_finished_receipt: V0JobFinishedReceiptRequest | None = None

    # scoring

    # !!! time_took can be negative since it's just
    #     `job_response_time - job_before_sent_time`
    #     and miner could have already sent a failed
    #     response before we sent the job details
    time_took: timedelta | None = None
    correct: bool | None = None  # returned correct answer (even if outside time limit)
    success: bool = False  # returned correct answer within time limit
    excused: bool = False  # declined (reason=busy) but provided valid excuse
    excused_with: list[Receipt] = field(
        default_factory=list
    )  # if excused, this is the list of valid excuse receipts.
    comment: str = "failed"
    score: float = 0
    average_job_send_time_bonus: timedelta | None = None

    def handle_message(self, msg: MinerToValidatorMessage) -> None:
        logger.debug("%s received from miner: %s", self.name, msg.message_type)

        # !!! it is very important to not allow a newer message of a
        #     certain kind to override a previously received message
        #     of the same kind. miners could play games with that.
        #     please don't change without discussion
        duplicate = False

        match msg:
            case V0AcceptJobRequest() | V0DeclineJobRequest():
                if self.accept_response is None:
                    self.accept_response = msg
                    self.accept_response_time = datetime.now(tz=UTC)
                    self.accept_response_event.set()
                else:
                    duplicate = True

            case V0ExecutorReadyRequest() | V0ExecutorFailedRequest():
                if self.executor_response is None:
                    self.executor_response = msg
                    self.executor_response_time = datetime.now(tz=UTC)
                    self.executor_response_event.set()
                else:
                    duplicate = True

            case V0StreamingJobReadyRequest() | V0StreamingJobNotReadyRequest():
                if self.streaming_job_ready_response is None:
                    self.streaming_job_ready_response = msg
                    self.streaming_job_ready_response_time = datetime.now(tz=UTC)
                    self.streaming_job_ready_response_event.set()
                else:
                    duplicate = True

            case V0JobFinishedRequest() | V0JobFailedRequest():
                if self.job_response is None:
                    self.job_response = msg
                    self.job_response_time = datetime.now(tz=UTC)
                    self.job_response_event.set()
                else:
                    duplicate = True

            # we don't care if we receive multiple specs messages,
            # doesn't matter which one we keep, miner controls it
            case V0MachineSpecsRequest():
                self.machine_specs = msg

        if duplicate:
            logger.warning("%s duplicate message: %s", self.name, msg.message_type)

    def system_event(
        self,
        *,
        type: SystemEvent.EventType,
        subtype: SystemEvent.EventSubType,
        description: str,
        func: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> SystemEvent | None:
        return self.ctx.system_event(
            type=type,
            subtype=subtype,
            description=description,
            data={**(data or {}), "description": description},
            job_uuid=self.uuid,
            miner_hotkey=self.miner_hotkey,
            func=func,
        )

    def emit_telemetry_event(self) -> SystemEvent | None:
        job_execution_time = self.job_generator.get_execution_time()
        if job_execution_time is None:
            job_execution_time = _timedelta_dump(self.time_took)

        data = dict(
            job_uuid=self.uuid,
            miner_hotkey=self.miner_hotkey,
            validator_hotkey=self.ctx.own_keypair.ss58_address,
            executor_class=self.executor_class.value,
            base_docker_image_name=self.job_generator.base_docker_image_name(),
            docker_image_name=self.job_generator.docker_image_name(),
            docker_run_options_preset=self.job_generator.docker_run_options_preset(),
            timeout_seconds=self.job_generator.timeout_seconds(),
            exception=repr(self.exception) if self.exception is not None else None,
            exception_time=_datetime_dump(self.exception_time),
            exception_stage=self.exception_stage,
            accept_barrier_time=_datetime_dump(self.accept_barrier_time),
            accept_barrier_2_time=_datetime_dump(self.accept_barrier_2_time),
            accept_before_sent_time=_datetime_dump(self.accept_before_sent_time),
            accept_after_sent_time=_datetime_dump(self.accept_after_sent_time),
            accept_response=_model_dump(self.accept_response),
            accept_response_time=_datetime_dump(self.accept_response_time),
            executor_response=_model_dump(self.executor_response),
            streaming_job_ready_response=_model_dump(self.streaming_job_ready_response),
            executor_response_time=_datetime_dump(self.executor_response_time),
            job_barrier_time=_datetime_dump(self.job_barrier_time),
            job_before_sent_time=_datetime_dump(self.job_before_sent_time),
            job_after_sent_time=_datetime_dump(self.job_after_sent_time),
            job_response=_model_dump(self.job_response),
            job_response_time=_datetime_dump(self.job_response_time),
            machine_specs=_model_dump(self.machine_specs),
            total_job_time=_timedelta_dump(self.time_took),
            job_execution_time=job_execution_time,
            success=self.success,
            correct=self.correct,
            comment=self.comment,
            score=self.score,
            average_job_send_time_bonus=_timedelta_dump(self.average_job_send_time_bonus),
        )
        return self.ctx.system_event(
            type=SystemEvent.EventType.VALIDATOR_TELEMETRY,
            subtype=SystemEvent.EventSubType.SYNTHETIC_JOB,
            description="job telemetry",
            data=data,
        )

    def get_spin_up_time(self) -> int:
        spin_up_time = EXECUTOR_CLASS[self.executor_class].spin_up_time
        spin_up_time = max(spin_up_time, _MIN_SPIN_UP_TIME)
        return spin_up_time

    def is_declined(self) -> bool:
        return isinstance(self.accept_response, V0DeclineJobRequest)

    def decline_reason(self) -> JobRejectionReason | None:
        if isinstance(self.accept_response, V0DeclineJobRequest):
            return self.accept_response.reason
        return None

    async def get_valid_decline_excuse_receipts(self) -> list[Receipt]:
        assert self.job_started_receipt_payload is not None
        assert isinstance(self.accept_response, V0DeclineJobRequest)

        # Use the time when the receipt for this job was generated as an excuse time cutoff time
        return await job_excuses.filter_valid_excuse_receipts(
            receipts_to_check=self.accept_response.receipts,
            check_time=self.job_started_receipt_payload.timestamp,
            declined_job_uuid=self.uuid,
            declined_job_executor_class=self.executor_class,
            declined_job_is_synthetic=True,
            miner_hotkey=self.miner_hotkey,
            minimum_validator_stake_for_excuse=self.ctx.batch_config.minimum_validator_stake_for_excuse,
            active_validators=self.ctx.active_validators,
        )


@dataclass
class BatchContext:
    batch: SyntheticJobBatch

    uuid: str
    own_keypair: bittensor_wallet.Keypair

    # validator creds for streaming jobs
    own_public_key: str
    own_certs: tuple[str, str]
    certs_basepath: Path

    # randomized, but order preserving list of miner.hotkeys
    # used to go from indices returned by asyncio.gather() back to miner.hotkey
    hotkeys: list[str]

    # full name for easier debugging: "{miner_hotkey}({ip}:{port})"
    names: dict[str, str]
    miners: dict[str, Miner]  # hotkey -> Miner
    clients: dict[str, MinerClient]
    executors: dict[str, defaultdict[ExecutorClass, int]]
    job_generators: dict[str, dict[ExecutorClass, list[BaseSyntheticJobGenerator]]]
    online_executor_count: dict[str, defaultdict[ExecutorClass, int]]

    # TODO: now `manifests` and `executors` have similar shape due to the protocol change. Do we still need both?
    manifests: dict[str, dict[ExecutorClass, int] | None]

    # randomized, but order preserving list of job.uuid
    # used to go from indices returned by asyncio.gather() back to job.uuid
    job_uuids: list[str]

    # job.uuid as key
    jobs: dict[str, Job]

    # list of validators for which busy excuses are considered valid, if they have enough stake
    active_validators: list[ValidatorInfo]

    # telemetry

    # system events, periodically flushed to database, which is why
    # we need a separate event_count field to track how many we
    # created during a batch run
    events: list[SystemEvent]
    event_count: int

    # events count per type-subtype, this is needed for enforcing limits
    event_limits_usage: LimitsDict

    stage_start_time: dict[str, datetime]

    batch_config: "BatchConfig"

    average_job_send_time: timedelta | None = None

    loop_profiler: "LoopProfiler | None" = None

    # for tests
    _loop: asyncio.AbstractEventLoop | None = None

    def system_event(
        self,
        *,
        type: SystemEvent.EventType,
        subtype: SystemEvent.EventSubType,
        description: str,
        data: dict[str, Any] | None = None,
        job_uuid: str | None = None,
        miner_hotkey: str | None = None,
        func: str | None = None,
        append: bool = True,
    ) -> SystemEvent | None:
        if self.batch_config.event_limits and (type, subtype) in self.batch_config.event_limits:
            if (
                self.event_limits_usage[(type, subtype)]
                >= self.batch_config.event_limits[(type, subtype)]
            ):
                logger.warning(
                    f"Discarding system event for exceeding limit {type=} {subtype=} {description=}"
                )
                return None
        self.event_limits_usage[(type, subtype)] += 1

        if data is None:
            data = {}

        assert "batch_uuid" not in data
        data["batch_uuid"] = self.uuid

        if job_uuid is not None:
            assert "job_uuid" not in data
            data["job_uuid"] = job_uuid

        if miner_hotkey is not None:
            assert "miner_hotkey" not in data
            data["miner_hotkey"] = miner_hotkey

        if func is not None:
            assert "func" not in data
            data["func"] = func

        try:
            event = SystemEvent(
                type=type,
                subtype=subtype,
                long_description=description,
                data=data,
            )
            # checkpoint events are sent directly to the database,
            # don't append them to avoid duplication
            if append:
                self.events.append(event)
            self.event_count += 1
            return event
        except Exception as exc:
            logger.error("Failed to emit system event: %r", exc)
            return None

    # sync_to_async is needed since we use the sync Django ORM
    @sync_to_async
    def checkpoint_system_event(self, stage: str, *, dt: datetime | None = None) -> None:
        try:
            if dt is None:
                dt = datetime.now(tz=UTC)
            logger.info("STAGE: %s", stage)
            self.stage_start_time[stage] = dt

            event = self.system_event(
                type=SystemEvent.EventType.VALIDATOR_TELEMETRY,
                subtype=SystemEvent.EventSubType.CHECKPOINT,
                description=stage,
                data=dict(
                    time=_datetime_dump(dt),
                    stage=stage,
                ),
                append=False,
            )
            if event is not None:
                event.save()
        except Exception as exc:
            logger.error("Failed to checkpoint system event: %r", exc)

    # needed because we query the DB for some data we put in the event payload
    @sync_to_async
    def emit_telemetry_event(self) -> SystemEvent | None:
        """
        Append a "batch" telemetry system event based on this batch to be sent later.
        """
        messages_count: dict[str, int] = defaultdict(int)
        for job in self.jobs.values():
            for msg in (
                job.accept_response,
                job.executor_response,
                job.streaming_job_ready_response,
                job.job_response,
                job.machine_specs,
            ):
                if msg is not None:
                    messages_count[msg.message_type] += 1
        # convert to regular dict for nice logging
        messages_count = dict(messages_count)

        counts = dict(
            miners=len(self.miners),
            manifests=sum(1 for manifest in self.manifests.values() if manifest is not None),
            messages=messages_count,
            system_events=self.event_count,
        )

        counts["jobs"] = self._get_job_count(None)
        for executor_class in ExecutorClass:
            counts[f"jobs:{executor_class.value}"] = self._get_job_count(executor_class)

        data = dict(
            validator_hotkey=self.own_keypair.ss58_address,
            stage_start_time={
                stage: _datetime_dump(dt) for stage, dt in self.stage_start_time.items()
            },
            average_job_send_time=_timedelta_dump(self.average_job_send_time),
            counts=counts,
            llm_counts={
                "llm_executor_count": self.get_executor_count(LLM_EXECUTOR_CLASS),
                **calculate_llm_prompt_sample_counts(),
            },
            manifests=self.manifests,
            loop_profiling=self.loop_profiler.get() if self.loop_profiler is not None else None,
        )
        return self.system_event(
            type=SystemEvent.EventType.VALIDATOR_TELEMETRY,
            subtype=SystemEvent.EventSubType.SYNTHETIC_BATCH,
            description="batch telemetry",
            data=data,
        )

    def get_executor_count(self, executor_class: ExecutorClass) -> int:
        """
        Calculate the total count of executors of given class.
        """
        return sum(
            count
            for executors in self.executors.values()
            for _executor_class, count in executors.items()
            if _executor_class == executor_class
        )

    def _get_job_count(self, executor_class: ExecutorClass | None) -> dict[str, int]:
        jobs = list(self.jobs.values())
        if executor_class is not None:
            jobs = [job for job in jobs if job.executor_class == executor_class]
        return dict(
            total=len(jobs),
            failed=sum(1 for job in jobs if not job.success and not job.excused),
            successful=sum(1 for job in jobs if job.success),
            excused=sum(1 for job in jobs if job.excused),
            correct=sum(1 for job in jobs if job.correct),
            # don't count None as incorrect
            incorrect=sum(1 for job in jobs if job.correct is False),
        )


class LoopProfiler:
    def __init__(self, ctx: BatchContext) -> None:
        self._ctx = ctx

        self._sleep_timings: list[float] = []
        self._sleep_task = asyncio.create_task(
            self._sleep_profiler(), name="LoopProfiler._sleep_profiler"
        )

        self._timeout_timings: list[float] = []
        self._timeout_task = asyncio.create_task(
            self._timeout_profiler(), name="LoopProfiler._timeout_profiler"
        )

    async def close(self) -> None:
        self._sleep_task.cancel()
        try:
            await self._sleep_task
        except asyncio.CancelledError:
            pass

        self._timeout_task.cancel()
        try:
            await self._timeout_task
        except asyncio.CancelledError:
            pass

    def get(self) -> JsonValue:
        return dict(
            sleep=self._get(_LOOP_PROFILING_SLEEP_INTERVAL, self._sleep_timings),
            timeout=self._get(_LOOP_PROFILING_TIMEOUT_INTERVAL, self._timeout_timings),
        )

    def _get(self, timeout: float, timings: list[float]) -> JsonValue:
        stats: JsonValue = dict(
            interval=timeout,
            count=len(timings),
        )
        if len(timings) > 2:
            if isinstance(stats, dict):  # make mypy happy
                stats |= dict(
                    min=min(timings),
                    max=max(timings),
                    mean=statistics.mean(timings),
                    median=statistics.median(timings),
                    stddev=statistics.stdev(timings),
                    variance=statistics.variance(timings),
                )
        return stats

    async def _sleep_profiler(self) -> None:
        while True:
            time_before_ns = time.monotonic_ns()
            await asyncio.sleep(_LOOP_PROFILING_SLEEP_INTERVAL)
            time_after_ns = time.monotonic_ns()

            duration_sec = (time_after_ns - time_before_ns) / 1_000_000_000
            self._sleep_timings.append(duration_sec)

    async def _timeout_profiler(self) -> None:
        while True:
            time_before_ns = time.monotonic_ns()
            try:
                async with asyncio.timeout(_LOOP_PROFILING_TIMEOUT_INTERVAL):
                    # we want the timeout to expire, so wait many times longer
                    await asyncio.sleep(_LOOP_PROFILING_TIMEOUT_INTERVAL * 100)
            except TimeoutError:
                pass
            time_after_ns = time.monotonic_ns()

            duration_sec = (time_after_ns - time_before_ns) / 1_000_000_000
            self._timeout_timings.append(duration_sec)


def _datetime_dump(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def _timedelta_dump(delta: timedelta | None) -> float | None:
    if delta is None:
        return None
    return delta.total_seconds()


def _model_dump(model: BaseModel | None) -> JsonValue:
    if model is None:
        return None
    return model.model_dump(mode="json")


def _handle_exceptions(ctx: BatchContext, exceptions: list[ExceptionInfo]) -> None:
    for exc_info in exceptions:
        name = ctx.jobs[exc_info.job_uuid].name
        text = f"{exc_info.stage}: {exc_info.exception!r}"
        logger.warning("%s %s", name, text)

        if isinstance(exc_info.exception, BaseException):
            subtype = SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT
        else:
            subtype = SystemEvent.EventSubType.GENERIC_ERROR

        ctx.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=subtype,
            description=text,
            miner_hotkey=exc_info.miner_hotkey,
            job_uuid=exc_info.job_uuid,
            func=exc_info.stage,
        )


class _MinerClientFactoryProtocol(Protocol):
    """
    Something that returns a MinerClient given a BatchContext and a miner hotkey
    """

    def __call__(self, ctx: BatchContext, miner_hotkey: str) -> MinerClient: ...


class BatchConfig:
    def __init__(self):
        self.event_limits: LimitsDict | None = None
        self.llm_answer_s3_download_timeout: float | None = None
        self.excused_synthetic_job_score: float | None = None
        self.non_peak_cycle_executor_min_ratio: float = 1.0
        self.default_executor_limits_for_missed_peak: dict[ExecutorClass, int] = {}
        self.minimum_validator_stake_for_excuse: float = 0.0

    async def populate(self):
        self.event_limits = await get_system_event_limits()
        self.llm_answer_s3_download_timeout = await aget_config(
            "DYNAMIC_LLM_ANSWER_S3_DOWNLOAD_TIMEOUT_SECONDS"
        )
        self.excused_synthetic_job_score = await aget_config("DYNAMIC_EXCUSED_SYNTHETIC_JOB_SCORE")
        self.non_peak_cycle_executor_min_ratio = await aget_config(
            "DYNAMIC_NON_PEAK_CYCLE_EXECUTOR_MIN_RATIO"
        )
        self.default_executor_limits_for_missed_peak = (
            await get_default_executor_limits_for_missed_peak()
        )
        self.minimum_validator_stake_for_excuse = await aget_config(
            "DYNAMIC_MINIMUM_VALIDATOR_STAKE_FOR_EXCUSE"
        )


async def _init_context(
    serving_miners: list[Miner],
    active_validators: list[ValidatorInfo],
    batch_id: int,
    create_miner_client: _MinerClientFactoryProtocol | None = None,
) -> BatchContext:
    batch = await SyntheticJobBatch.objects.aget(id=batch_id)

    own_wallet = settings.BITTENSOR_WALLET()
    own_keypair = own_wallet.get_hotkey()
    create_miner_client = create_miner_client or MinerClient
    batch_config = BatchConfig()
    await batch_config.populate()

    # TODO move somewhere else - gen a certificate per batch or not?
    # Generate validator certificate
    dir_path, public_key, certs = generate_certificate_at()

    ctx = BatchContext(
        batch=batch,
        uuid=str(uuid.uuid4()),
        own_keypair=own_keypair,
        certs_basepath=dir_path,
        own_public_key=public_key,
        own_certs=certs,
        hotkeys=[],
        names={},
        miners={},
        clients={},
        executors={},
        job_generators={},
        online_executor_count={},
        manifests={},
        job_uuids=[],
        jobs={},
        active_validators=active_validators,
        events=[],
        event_count=0,
        event_limits_usage=defaultdict(int),
        stage_start_time={},
        _loop=asyncio.get_running_loop(),
        batch_config=batch_config,
    )

    for miner in serving_miners:
        hotkey = miner.hotkey
        ctx.hotkeys.append(hotkey)
        ctx.names[hotkey] = f"{hotkey}({miner.address}:{miner.port})"
        ctx.miners[hotkey] = miner
        ctx.clients[hotkey] = create_miner_client(ctx=ctx, miner_hotkey=hotkey)
        ctx.executors[hotkey] = defaultdict(int)
        ctx.job_generators[hotkey] = {}
        ctx.online_executor_count[hotkey] = defaultdict(int)
        ctx.manifests[hotkey] = None

    return ctx


def _get_max_spin_up_time(ctx: BatchContext) -> int:
    max_spin_up_time = _MIN_SPIN_UP_TIME
    for executors in ctx.executors.values():
        for executor_class, count in executors.items():
            if count > 0:
                spin_up_time = EXECUTOR_CLASS[executor_class].spin_up_time
                max_spin_up_time = max(max_spin_up_time, spin_up_time)
    return max_spin_up_time


def _get_total_executor_count(ctx: BatchContext) -> int:
    total_executor_count = 0
    total_executor_class_count: dict[ExecutorClass, int] = defaultdict(int)
    for executors in ctx.executors.values():
        for executor_class, count in executors.items():
            total_executor_count += count
            total_executor_class_count[executor_class] += count
    for executor_class, count in total_executor_class_count.items():
        logger.info("%s has %d total executors", executor_class, count)
    return total_executor_count


def _generate_job_started_receipt(ctx: BatchContext, job: Job) -> None:
    assert job.job_started_receipt_payload is None
    assert job.job_started_receipt_signature is None

    job_timeout_seconds = job.job_generator.timeout_seconds()
    spinup_leeway_seconds = 5
    ttl_min = 5
    ttl_max = 60 * 5

    ttl = job.get_spin_up_time() + job_timeout_seconds + spinup_leeway_seconds
    ttl_clamped = max(ttl_min, min(ttl_max, ttl))

    payload, signature = receipts().create_job_started_receipt(
        job_uuid=job.uuid,
        miner_hotkey=job.miner_hotkey,
        validator_hotkey=ctx.own_keypair.ss58_address,
        executor_class=ExecutorClass(job.executor_class),
        is_organic=False,
        ttl=ttl_clamped,
    )

    job.job_started_receipt_payload = payload
    job.job_started_receipt_signature = signature


def _generate_job_accepted_receipt(ctx: BatchContext, job: Job) -> None:
    assert job.job_accepted_receipt is None
    assert job.accept_response_time is not None

    payload = JobAcceptedReceiptPayload(
        job_uuid=job.uuid,
        miner_hotkey=job.miner_hotkey,
        validator_hotkey=ctx.own_keypair.ss58_address,
        timestamp=datetime.now(tz=UTC),
        time_accepted=job.accept_response_time,
        ttl=6 * 60,  # FIXME: max time allowed to run the job
    )
    job.job_accepted_receipt = V0JobAcceptedReceiptRequest(
        payload=payload,
        signature=f"0x{ctx.own_keypair.sign(payload.blob_for_signing()).hex()}",
    )


def _generate_job_finished_receipt(ctx: BatchContext, job: Job) -> None:
    assert job.job_finished_receipt is None
    assert job.job_before_sent_time is not None

    if not job.success:
        assert job.score == 0

    if job.time_took is not None:
        time_took_sec = job.time_took.total_seconds()
    else:
        time_took_sec = 0

    payload = JobFinishedReceiptPayload(
        job_uuid=job.uuid,
        miner_hotkey=job.miner_hotkey,
        validator_hotkey=ctx.own_keypair.ss58_address,
        timestamp=datetime.now(tz=UTC),
        time_started=job.job_before_sent_time,
        time_took_us=int(time_took_sec * 1_000_000),
        score_str=f"{job.score:.6g}",
    )
    job.job_finished_receipt = V0JobFinishedReceiptRequest(
        payload=payload,
        signature=f"0x{ctx.own_keypair.sign(payload.blob_for_signing()).hex()}",
    )


async def _get_miner_manifest(
    ctx: BatchContext, start_barrier: asyncio.Barrier, miner_hotkey: str
) -> None:
    await start_barrier.wait()

    miner = ctx.miners[miner_hotkey]

    async with asyncio.timeout(_GET_MANIFEST_TIMEOUT):
        async with aiohttp.ClientSession() as session:
            url = f"http://{miner.address}:{miner.port}/v0.1/manifest"
            async with await session.get(url) as response:
                # Try to parse the response as JSON and ensure that it has an "error" key if
                # anything goes wrong
                try:
                    data = await response.json()
                except Exception:
                    try:
                        data = {"error": await response.text()}
                    except Exception:
                        data = {"error": "No response body"}

                if response.status == 200:
                    manifest = data.get("manifest", {})
                    # Convert manifests back into the enums
                    manifest = {
                        ExecutorClass(executor_class): count
                        for executor_class, count in manifest.items()
                    }
                else:
                    name = ctx.names[miner_hotkey]
                    logger.warning(
                        "%s HTTP connection error (%s): %s", name, response.status, data["error"]
                    )
                    ctx.system_event(
                        type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                        subtype=SystemEvent.EventSubType.MINER_CONNECTION_ERROR,
                        description=f"HTTP connection error ({response.status}): {data['error']}",
                        miner_hotkey=miner_hotkey,
                        func="connect",
                    )
                    return

    ctx.manifests[miner_hotkey] = manifest
    assert manifest is not None

    executors = ctx.executors[miner_hotkey]
    for executor_class, count in manifest.items():
        if count > 0:
            executors[executor_class] += count


async def _close_client(ctx: BatchContext, miner_hotkey: str) -> None:
    client = ctx.clients[miner_hotkey]

    async with asyncio.timeout(_CLOSE_TIMEOUT):
        await client.close()


def calculate_llm_prompt_sample_counts() -> dict[str, int]:
    """
    Calculate counts of LLM jobs, grouped by whether they are used and/or answered.
    """
    prompt_series_total_count = PromptSeries.objects.count()

    prompt_sample_types = (
        PromptSample.objects.annotate(
            is_used=ExpressionWrapper(
                Q(synthetic_job_id__isnull=False),
                output_field=BooleanField(),
            ),
            is_answered=ExpressionWrapper(
                Q(workload__finished_at__isnull=False),
                output_field=BooleanField(),
            ),
        )
        .values("is_used", "is_answered")
        .annotate(count=Count("*"))
    )

    prompt_sample_used_count = 0
    prompt_sample_unused_answered_count = 0
    prompt_sample_unused_unanswered_count = 0
    for prompt_sample_type in prompt_sample_types:
        match prompt_sample_type:
            case {"is_used": True, "count": n}:
                prompt_sample_used_count += n
            case {"is_used": False, "is_answered": True, "count": n}:
                prompt_sample_unused_answered_count += n
            case {"is_used": False, "is_answered": False, "count": n}:
                prompt_sample_unused_unanswered_count += n
            case _:
                logger.warning("unreachable code reached!")

    return {
        "prompt_series_total_count": prompt_series_total_count,
        "prompt_sample_used_count": prompt_sample_used_count,
        "prompt_sample_unused_answered_count": prompt_sample_unused_answered_count,
        "prompt_sample_unused_unanswered_count": prompt_sample_unused_unanswered_count,
    }


@sync_to_async
def _not_enough_prompts_system_event(
    ctx: BatchContext,
) -> None:
    if cache.get("insufficient_prompts_telemetry_sent"):
        logger.warning("skipping INSUFFICIENT_PROMPTS system event, already exists in 24h")
        return

    ctx.system_event(
        type=SystemEvent.EventType.VALIDATOR_TELEMETRY,
        subtype=SystemEvent.EventSubType.INSUFFICIENT_PROMPTS,
        description="not enough prompt samples available in database",
        func="get_llm_prompt_samples",
        data={
            "llm_executor_count": ctx.get_executor_count(LLM_EXECUTOR_CLASS),
            **calculate_llm_prompt_sample_counts(),
        },
    )
    cache.set("insufficient_prompts_telemetry_sent", True, timeout=24 * 60 * 60)


async def get_llm_prompt_samples(ctx: BatchContext) -> list[PromptSample] | None:
    # TODO: refactor into nicer abstraction
    llm_executor_count = ctx.get_executor_count(LLM_EXECUTOR_CLASS)
    prompt_samples_qs = (
        PromptSample.objects.select_related("series", "workload")
        .prefetch_related("prompts")
        .filter(
            synthetic_job__isnull=True,
            workload__finished_at__isnull=False,
        )[:llm_executor_count]
    )
    prompt_samples = [ps async for ps in prompt_samples_qs]
    if len(prompt_samples) < llm_executor_count:
        await _not_enough_prompts_system_event(ctx)
        logger.warning(
            "Not enough prompt samples for llm executors: %d < %d - will NOT run llm synthetic prompt jobs",
            len(prompt_samples),
            llm_executor_count,
        )
        return None
    return prompt_samples


async def _generate_jobs(ctx: BatchContext) -> None:
    streaming_classes = await get_streaming_job_executor_classes()
    start_time = time.time()
    generated_job_count = 0

    prompt_samples = await get_llm_prompt_samples(ctx)
    prompt_samples_iter = iter(prompt_samples) if prompt_samples is not None else None

    for hotkey, executors in ctx.executors.items():
        miner_name = ctx.names[hotkey]
        for executor_class, count in executors.items():
            job_generators = []
            for _ in range(count):
                kwargs = {}
                if executor_class == LLM_EXECUTOR_CLASS:
                    if prompt_samples_iter is None:
                        logger.warning("No llm prompt samples available, skipping llm job")
                        continue
                    prompt_sample = next(prompt_samples_iter, None)
                    if prompt_sample is None:
                        # it means that there is some bug - we want to see it in sentry
                        # and continue, so other executor classes are not affected
                        logger.error(
                            "Dried prompt_samples_iter, this should not happen, skipping llm job"
                        )
                        continue
                    kwargs = {
                        "prompt_sample": prompt_sample,
                        "expected_prompts": list(prompt_sample.prompts.all()),
                        "s3_url": prompt_sample.series.s3_url,
                        "seed": prompt_sample.workload.seed,
                    }
                    # enable streaming for specific llm jobs executor classes
                    if executor_class in streaming_classes:
                        kwargs["streaming"] = True

                job_generator = await current.synthetic_job_generator_factory.create(
                    executor_class, **kwargs
                )
                await job_generator.ainit(miner_hotkey=hotkey)
                job_uuid = str(job_generator.uuid())
                ctx.jobs[job_uuid] = Job(
                    ctx=ctx,
                    uuid=job_uuid,
                    name=f"{miner_name} job {job_uuid}",
                    miner_hotkey=hotkey,
                    executor_class=executor_class,
                    job_generator=job_generator,
                    volume=await job_generator.volume(),
                    output_upload=await job_generator.output_upload(),
                )
                ctx.job_uuids.append(job_uuid)
                job_generators.append(job_generator)
                generated_job_count += 1
            ctx.job_generators[hotkey][executor_class] = job_generators

    duration = time.time() - start_time
    logger.info("Generated %d jobs in %.2f seconds", generated_job_count, duration)


async def _send_initial_job_request(
    ctx: BatchContext,
    start_barrier: asyncio.Barrier,
    serialize_barrier: asyncio.Barrier,
    max_spin_up_time: int,
    job_uuid: str,
) -> None:
    job: Job | None = None
    try:
        streaming_classes = await get_streaming_job_executor_classes()
        await start_barrier.wait()
        barrier_time = datetime.now(tz=UTC)

        job = ctx.jobs[job_uuid]
        job.accept_barrier_time = barrier_time
        client = ctx.clients[job.miner_hotkey]

        _generate_job_started_receipt(ctx, job)
        assert job.job_started_receipt_payload is not None
        assert job.job_started_receipt_signature is not None

        stagger_wait_interval = max_spin_up_time - job.get_spin_up_time()
        assert stagger_wait_interval >= 0

        request = V0InitialJobRequest(
            job_uuid=job.uuid,
            executor_class=job.executor_class,
            docker_image=job.job_generator.base_docker_image_name(),
            timeout_seconds=job.job_generator.timeout_seconds(),
            volume=job.volume if job.job_generator.volume_in_initial_req() else None,
            job_started_receipt_payload=job.job_started_receipt_payload,
            job_started_receipt_signature=job.job_started_receipt_signature,
        )
        if job.executor_class in streaming_classes:
            request.streaming_details = StreamingDetails(public_key=ctx.own_public_key)
        request_json = request.model_dump_json()

    finally:
        # !!! it's very important we wait on this barrier, no matter what happens above,
        #     if we don't wait, other concurrent jobs will hang forever since they will
        #     never pass this barrier
        await serialize_barrier.wait()

        if job is not None:
            job.accept_barrier_2_time = datetime.now(tz=UTC)

    async with asyncio.timeout(max_spin_up_time):
        if stagger_wait_interval > 0:
            await asyncio.sleep(stagger_wait_interval)

        # send can block, so take a timestamp
        # on both sides to detect long send times
        job.accept_before_sent_time = datetime.now(tz=UTC)
        await client.send_check(request_json)
        job.accept_after_sent_time = datetime.now(tz=UTC)

        await job.accept_response_event.wait()
        match job.accept_response:
            case V0AcceptJobRequest():
                logger.info("%s accepted job %s", job.miner_hotkey, job.uuid)
                await _handle_job_accepted(client, ctx, job)
            case V0DeclineJobRequest():
                logger.info(
                    "%s declined job %s (reason=%s)",
                    job.miner_hotkey,
                    job.uuid,
                    job.accept_response.reason.value if job.accept_response.reason else None,
                )
            case None:
                logger.error("Job accept response event fired, but response is missing.")
            case _:
                assert_never(job.accept_response)


async def _handle_job_accepted(client: MinerClient, ctx: BatchContext, job: Job) -> None:
    _generate_job_accepted_receipt(ctx, job)
    assert job.job_accepted_receipt is not None
    try:
        receipt_json = job.job_accepted_receipt.model_dump_json()
        async with asyncio.timeout(_SEND_RECEIPT_TIMEOUT):
            await client.send_check(receipt_json)
    except (Exception, asyncio.CancelledError) as exc:
        logger.warning("%s failed to send job accepted receipt: %r", job.name, exc)
        job.system_event(
            type=SystemEvent.EventType.RECEIPT_FAILURE,
            subtype=SystemEvent.EventSubType.RECEIPT_SEND_ERROR,
            description=repr(exc),
            func="_handle_job_accepted",
        )
    await job.executor_response_event.wait()
    logger.info("%s reported executor ready for job %s", job.miner_hotkey, job.uuid)


async def _send_job_request(
    ctx: BatchContext,
    start_barrier: asyncio.Barrier,
    job_uuid: str,
) -> None:
    await start_barrier.wait()
    barrier_time = datetime.now(tz=UTC)

    job = ctx.jobs[job_uuid]
    job.job_barrier_time = barrier_time
    client = ctx.clients[job.miner_hotkey]

    request = V0JobRequest(
        job_uuid=job.uuid,
        executor_class=job.executor_class,
        docker_image=job.job_generator.docker_image_name(),
        docker_run_options_preset=job.job_generator.docker_run_options_preset(),
        docker_run_cmd=job.job_generator.docker_run_cmd(),
        raw_script=job.job_generator.raw_script(),
        volume=job.volume if not job.job_generator.volume_in_initial_req() else None,
        output_upload=job.output_upload,
    )
    request_json = request.model_dump_json()

    timeout = job.job_generator.timeout_seconds() + _JOB_RESPONSE_EXTRA_TIMEOUT

    async with asyncio.timeout(timeout):
        # send can block, so take a timestamp
        # on both sides to detect long send times
        job.job_before_sent_time = datetime.now(tz=UTC)
        await client.send_check(request_json)
        job.job_after_sent_time = datetime.now(tz=UTC)

        await job.job_response_event.wait()


async def _send_job_request_for_streaming(
    ctx: BatchContext,
    job_uuid: str,
):
    job = ctx.jobs[job_uuid]
    client = ctx.clients[job.miner_hotkey]

    request = V0JobRequest(
        job_uuid=job.uuid,
        executor_class=job.executor_class,
        docker_image=job.job_generator.docker_image_name(),
        docker_run_options_preset=job.job_generator.docker_run_options_preset(),
        docker_run_cmd=job.job_generator.docker_run_cmd(),
        raw_script=job.job_generator.raw_script(),
        volume=job.volume if not job.job_generator.volume_in_initial_req() else None,
        output_upload=job.output_upload,
    )
    request_json = request.model_dump_json()

    job.job_before_sent_time = datetime.now(tz=UTC)
    await client.send_check(request_json)
    job.job_after_sent_time = datetime.now(tz=UTC)

    try:
        async with asyncio.timeout(await job.job_generator.streaming_preparation_timeout()):
            await job.streaming_job_ready_response_event.wait()
    except TimeoutError:
        logger.debug("Timeout waiting for StreamingJobReadyRequest for job_uuid=%s", job_uuid)
        return

    logger.debug(f"Received streaming job ready response for {job_uuid}")

    response = job.streaming_job_ready_response
    if not isinstance(response, V0StreamingJobReadyRequest):
        logger.warning(f"Bad job ready response for {job_uuid}: {response}")


async def _multi_send_job_request_for_streaming(
    ctx: BatchContext,
    executor_ready_jobs: list[tuple[str, bool]],
):
    streaming_jobs = [job_uuid for job_uuid, is_streaming in executor_ready_jobs if is_streaming]
    if not streaming_jobs:
        logger.debug("No streaming jobs to run")
        return
    logger.debug("Sending job request for %s streaming jobs", len(streaming_jobs))

    tasks = [
        asyncio.create_task(
            _send_job_request_for_streaming(ctx, job_uuid),
            name=f"{job_uuid}._send_job_request_for_streaming",
        )
        for job_uuid in streaming_jobs
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    exceptions: list[ExceptionInfo] = []
    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            job_uuid = streaming_jobs[i][0]
            job = ctx.jobs[job_uuid]
            job.exception = result
            job.exception_time = datetime.now(tz=UTC)
            job.exception_stage = "_send_job_request_for_streaming"
            exceptions.append(
                ExceptionInfo(
                    exception=job.exception,
                    miner_hotkey=job.miner_hotkey,
                    job_uuid=job.uuid,
                    stage=job.exception_stage,
                )
            )
        else:
            assert result is None
    _handle_exceptions(ctx, exceptions)


async def _send_job_finished_receipts(ctx: BatchContext) -> None:
    for job in ctx.jobs.values():
        # generate job finished receipts for all jobs
        # which returned a response, even if they failed
        if job.job_response is not None:
            client = ctx.clients[job.miner_hotkey]
            try:
                _generate_job_finished_receipt(ctx, job)
                assert job.job_finished_receipt is not None

                receipt_json = job.job_finished_receipt.model_dump_json()
                async with asyncio.timeout(_SEND_RECEIPT_TIMEOUT):
                    await client.send_check(receipt_json)

            except (Exception, asyncio.CancelledError) as exc:
                logger.warning("%s failed to send job finished receipt: %r", job.name, exc)
                job.system_event(
                    type=SystemEvent.EventType.RECEIPT_FAILURE,
                    subtype=SystemEvent.EventSubType.RECEIPT_SEND_ERROR,
                    description=repr(exc),
                    func="_send_job_finished_receipts",
                )


def _emit_decline_or_failure_events(ctx: BatchContext) -> None:
    for job in ctx.jobs.values():
        if isinstance(job.accept_response, V0DeclineJobRequest) or isinstance(
            job.executor_response, V0ExecutorFailedRequest
        ):
            if job.excused:
                # Excused job is a special case of a declined job.
                logger.warning("%s excused", job.name)
                excused_by = list(set(r.payload.validator_hotkey for r in job.excused_with))
                job.system_event(
                    type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                    subtype=SystemEvent.EventSubType.JOB_EXCUSED,
                    description="excused",
                    data={"excused_by": excused_by},
                )
            else:
                logger.warning("%s declined", job.name)
                job.system_event(
                    type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                    subtype=SystemEvent.EventSubType.JOB_REJECTED,
                    description="declined",
                )

        if isinstance(job.streaming_job_ready_response, V0StreamingJobNotReadyRequest):
            logger.warning("%s failed to start streaming", job.name)
            job.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.JOB_NOT_STARTED,
                description="failed to start streaming",
            )

        if isinstance(job.job_response, V0JobFailedRequest):
            returncode = job.job_response.docker_process_exit_status
            text = f"failed: {returncode=}"
            logger.warning("%s %s", job.name, text)
            job.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.FAILURE,
                description=text,
            )


async def _emit_telemetry_events(ctx: BatchContext) -> None:
    batch_system_event = await ctx.emit_telemetry_event()
    if batch_system_event is not None:
        counts = batch_system_event.data.get("counts")
        logger.info("Batch telemetry counts: %s", counts)

    for job in ctx.jobs.values():
        job.emit_telemetry_event()


async def _send_machine_specs(ctx: BatchContext) -> None:
    channel_layer = get_channel_layer()
    assert channel_layer is not None

    send_exc: BaseException | None = None
    send_exc_count = 0

    for job in ctx.jobs.values():
        # only take into account machine specs from executors which
        # finished the job successfully, to prevent fake executors
        # from pushing specs for non-existing GPUs
        if job.success and job.machine_specs is not None:
            try:
                async with asyncio.timeout(_SEND_MACHINE_SPECS_TIMEOUT):
                    await channel_layer.send(
                        MACHINE_SPEC_CHANNEL,
                        {
                            "type": "machine.specs",
                            "batch_id": ctx.uuid,
                            "miner_hotkey": job.miner_hotkey,
                            "specs": job.machine_specs.specs.specs,
                        },
                    )
            except (Exception, asyncio.CancelledError) as exc:
                send_exc = exc
                send_exc_count += 1
                logger.warning("%s failed to send machine specs: %r", job.name, exc)

    if send_exc_count:
        msg = f"{send_exc_count} exceptions raised when trying to send machine specs. last one: {send_exc!r}"
        ctx.system_event(
            type=SystemEvent.EventType.VALIDATOR_CHANNEL_LAYER_ERROR,
            subtype=SystemEvent.EventSubType.SPECS_SEND_ERROR,
            description=msg,
            func="_send_machine_specs",
        )


async def _multi_get_miner_manifest(ctx: BatchContext) -> None:
    start_barrier = asyncio.Barrier(len(ctx.hotkeys))
    tasks = [
        asyncio.create_task(
            _get_miner_manifest(ctx, start_barrier, miner_hotkey),
            name=f"{miner_hotkey}._get_miner_manifest",
        )
        for miner_hotkey in ctx.hotkeys
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            hotkey = ctx.hotkeys[i]
            name = ctx.names[hotkey]
            logger.warning("%s failed to get manifest: %r", name, result)

            if isinstance(result, TimeoutError | asyncio.CancelledError):
                subtype = SystemEvent.EventSubType.MANIFEST_TIMEOUT
            else:
                subtype = SystemEvent.EventSubType.MANIFEST_ERROR

            ctx.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=subtype,
                description=repr(result),
                miner_hotkey=hotkey,
                func="_get_miner_manifest",
            )
        else:
            assert result is None


async def _adjust_miner_max_executors_per_class(ctx: BatchContext) -> None:
    max_executors_per_class = await get_miner_max_executors_per_class()
    for hotkey, executors in ctx.executors.items():
        for executor_class, miner_reported_count in executors.items():
            if executor_class not in max_executors_per_class:
                continue
            allowed_count = max_executors_per_class[executor_class]
            if miner_reported_count > allowed_count:
                logger.warning(
                    "%s manifest for executor class %s has more executors (%s) than the max limit (%s), capping at limit",
                    ctx.names[hotkey],
                    executor_class,
                    miner_reported_count,
                    allowed_count,
                )
                ctx.system_event(
                    type=SystemEvent.EventType.MINER_EXECUTOR_COUNT_CLIPPED,
                    subtype=SystemEvent.EventSubType.WARNING,
                    data={
                        "allowed_count": allowed_count,
                        "miner_reported_count": miner_reported_count,
                    },
                    description="executor count clipped",
                    miner_hotkey=hotkey,
                    func="_adjust_miner_max_executors_per_class",
                )
                ctx.executors[hotkey][executor_class] = allowed_count


def get_executor_counts(batch: SyntheticJobBatch | None) -> dict[str, dict[ExecutorClass, int]]:
    """In a given batch, get the number of online executors per miner per executor class"""
    if not batch:
        return {}

    result: dict[str, dict[ExecutorClass, int]] = defaultdict(lambda: defaultdict(int))

    for manifest in MinerManifest.objects.select_related("miner").filter(batch_id=batch.id):
        executor_class = ExecutorClass(manifest.executor_class)
        result[manifest.miner.hotkey][executor_class] += manifest.online_executor_count

    return result


@sync_to_async
def _limit_non_peak_executors_per_class(ctx: BatchContext) -> None:
    peak_cycle = get_peak_cycle(ctx.batch.block, netuid=settings.BITTENSOR_NETUID)
    if ctx.batch.block in peak_cycle:
        return

    peak_batch = SyntheticJobBatch.objects.filter(
        block__gte=peak_cycle.start,
        block__lt=peak_cycle.stop,
        should_be_scored=True,
    ).first()

    peak_counts = get_executor_counts(peak_batch)
    non_peak_ratio = ctx.batch_config.non_peak_cycle_executor_min_ratio
    for hotkey, executors in ctx.executors.items():
        miner_peak_counts = peak_counts.get(hotkey, {})
        for executor_class, miner_reported_count in executors.items():
            if executor_class in miner_peak_counts:
                # Miner was present during peak
                miner_peak_count = miner_peak_counts.get(executor_class, 0)
                allowed_count = math.ceil(miner_peak_count * non_peak_ratio)
                # Always allow at least 1 executor
                allowed_count = max(allowed_count, 1)
            else:
                # Miner was not present during peak
                allowed_count = ctx.batch_config.default_executor_limits_for_missed_peak.get(
                    executor_class, 1
                )
            allowed_count = min(miner_reported_count, allowed_count)
            if allowed_count != miner_reported_count:
                ctx.system_event(
                    type=SystemEvent.EventType.MINER_EXECUTOR_COUNT_CLIPPED,
                    subtype=SystemEvent.EventSubType.WARNING,
                    data={
                        "allowed_count": allowed_count,
                        "miner_reported_count": miner_reported_count,
                    },
                    description="non-peak executor count clipped",
                    miner_hotkey=hotkey,
                    func="_limit_non_peak_executors_per_class",
                )
                logger.debug(
                    "%s manifest for executor class %s has more executors (%s) than the allowed limit (%s) for non-peak, capping at limit",
                    ctx.names[hotkey],
                    executor_class,
                    miner_reported_count,
                    allowed_count,
                )
            ctx.executors[hotkey][executor_class] = allowed_count


async def _multi_close_client(ctx: BatchContext) -> None:
    tasks = [
        asyncio.create_task(
            _close_client(ctx, miner_hotkey),
            name=f"{miner_hotkey}._close_client",
        )
        for miner_hotkey in ctx.hotkeys
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            hotkey = ctx.hotkeys[i]
            name = ctx.names[hotkey]
            logger.warning("%s failed to close client: %r", name, result)
        else:
            assert result is None


async def _multi_send_initial_job_request(ctx: BatchContext) -> None:
    max_spin_up_time = _get_max_spin_up_time(ctx)
    logger.debug("Max spin-up time: %d seconds", max_spin_up_time)

    logger.info("Sending initial job requests for %d jobs", len(ctx.job_uuids))
    start_barrier = asyncio.Barrier(len(ctx.job_uuids))
    serialize_barrier = asyncio.Barrier(len(ctx.job_uuids))
    tasks = [
        asyncio.create_task(
            _send_initial_job_request(
                ctx, start_barrier, serialize_barrier, max_spin_up_time, job_uuid
            ),
            name=f"{job_uuid}._send_initial_job_request",
        )
        for job_uuid in ctx.job_uuids
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    exceptions: list[ExceptionInfo] = []
    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            job_uuid = ctx.job_uuids[i]
            job = ctx.jobs[job_uuid]
            job.exception = result
            job.exception_time = datetime.now(tz=UTC)
            job.exception_stage = "_send_initial_job_request"
            exceptions.append(
                ExceptionInfo(
                    exception=job.exception,
                    miner_hotkey=job.miner_hotkey,
                    job_uuid=job.uuid,
                    stage=job.exception_stage,
                )
            )
        else:
            assert result is None
    _handle_exceptions(ctx, exceptions)


async def _get_executor_ready_jobs(ctx: BatchContext) -> list[tuple[str, bool]]:
    streaming_classes = await get_streaming_job_executor_classes()

    executor_ready_jobs = [
        (job.uuid, job.executor_class in streaming_classes)
        for job in ctx.jobs.values()
        if isinstance(job.accept_response, V0AcceptJobRequest)
        and isinstance(job.executor_response, V0ExecutorReadyRequest)
        # occasionally we can get a job response (V0JobFailedRequest | V0JobFinishedRequest)
        # before sending the actual job request (V0JobRequest), for example because
        # the executor decide to abort the job before the details were sent
        and job.job_response is None
    ]
    return executor_ready_jobs


async def _trigger_streaming_job_and_wait_for_job_finish(
    ctx,
    job_uuid,
    start_barrier,
):
    job = ctx.jobs[job_uuid]
    if not isinstance(job.streaming_job_ready_response, V0StreamingJobReadyRequest):
        logger.debug(f"Not triggering execution for streaming job {job_uuid}")
        return

    await job.job_generator.trigger_streaming_job_execution(
        job_uuid,
        start_barrier,
        job.streaming_job_ready_response.public_key,
        ctx.own_certs,
        job.streaming_job_ready_response.ip,
        job.streaming_job_ready_response.port,
    )

    async with asyncio.timeout(job.job_generator.timeout_seconds()):
        await job.job_response_event.wait()


async def _trigger_job_execution(
    ctx: BatchContext, executor_ready_jobs: list[tuple[str, bool]]
) -> None:
    assert executor_ready_jobs
    buffered_tasks = []
    streaming_tasks = []

    start_barrier = asyncio.Barrier(
        len(
            [
                (job_uuid, is_streaming)
                for job_uuid, is_streaming in executor_ready_jobs
                if not is_streaming
                or isinstance(
                    ctx.jobs[job_uuid].streaming_job_ready_response, V0StreamingJobReadyRequest
                )
            ]
        )
    )

    for job_uuid, is_streaming in executor_ready_jobs:
        if not is_streaming:
            buffered_tasks.append(
                asyncio.create_task(
                    _send_job_request(ctx, start_barrier, job_uuid),
                    name=f"{job_uuid}._trigger_job_execution",
                )
            )
        elif isinstance(
            ctx.jobs[job_uuid].streaming_job_ready_response,
            V0StreamingJobReadyRequest,
        ):
            streaming_tasks.append(
                asyncio.create_task(
                    _trigger_streaming_job_and_wait_for_job_finish(
                        ctx,
                        job_uuid,
                        start_barrier,
                    ),
                    name=f"{job_uuid}._trigger_job_execution",
                )
            )
        else:
            logger.debug(f"Not triggering execution for streaming job {job_uuid}")

    logger.info(
        "Sending job requests for %d ready jobs - %s streaming and %s buffered",
        len(streaming_tasks) + len(buffered_tasks),
        len(streaming_tasks),
        len(buffered_tasks),
    )

    results = await asyncio.gather(*(streaming_tasks + buffered_tasks), return_exceptions=True)

    exceptions: list[ExceptionInfo] = []
    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            logger.exception("When running Synthetic jobs", exc_info=result)
            job_uuid = executor_ready_jobs[i][0]
            job = ctx.jobs[job_uuid]
            job.exception = result
            job.exception_time = datetime.now(tz=UTC)
            job.exception_stage = "_trigger_job_execution"
            exceptions.append(
                ExceptionInfo(
                    exception=job.exception,
                    miner_hotkey=job.miner_hotkey,
                    job_uuid=job.uuid,
                    stage=job.exception_stage,
                )
            )
        else:
            assert result is None
    _handle_exceptions(ctx, exceptions)


def _compute_average_send_time(ctx: BatchContext) -> None:
    durations_sec: list[float] = []

    for job in ctx.jobs.values():
        if job.job_after_sent_time is None:
            continue
        assert job.job_before_sent_time is not None
        duration = job.job_after_sent_time - job.job_before_sent_time
        duration_sec = duration.total_seconds()
        durations_sec.append(duration_sec)

    if durations_sec:
        average_duration_sec = statistics.mean(durations_sec)
    else:
        average_duration_sec = 0
    assert average_duration_sec >= 0
    ctx.average_job_send_time = timedelta(seconds=average_duration_sec)
    logger.info("Average job send time: %.6f seconds", average_duration_sec)


async def _score_job(ctx: BatchContext, job: Job) -> None:
    job.score = 0
    job.average_job_send_time_bonus = None
    job.success = False
    job.excused = False
    job.comment = "failed"

    if job.decline_reason() == JobRejectionReason.BUSY:
        relevant_executor_count = ctx.executors[job.miner_hotkey][job.executor_class]
        valid_excuse_receipts = await job.get_valid_decline_excuse_receipts()
        accepted_jobs = sum(
            1
            for other_job in ctx.jobs.values()
            if other_job.miner_hotkey == job.miner_hotkey
            and other_job.executor_class == job.executor_class
            and isinstance(other_job.accept_response, V0AcceptJobRequest)
        )
        excuse_ok = accepted_jobs + len(valid_excuse_receipts) >= relevant_executor_count

        for receipt in valid_excuse_receipts:
            if isinstance(receipt.payload, JobStartedReceiptPayload):
                valid_until = receipt.payload.timestamp + timedelta(seconds=receipt.payload.ttl)
            else:
                valid_until = None
            logger.debug(
                "Receipt for job %s from validator %s valid until %s",
                receipt.payload.job_uuid,
                receipt.payload.validator_hotkey,
                valid_until,
            )
        excuse_detail = (
            f"executors={relevant_executor_count} "
            f"accepted_jobs={accepted_jobs} "
            f"valid_receipts={len(valid_excuse_receipts)}"
        )

        if excuse_ok:
            job.score = ctx.batch_config.excused_synthetic_job_score or 0
            job.excused = True
            job.excused_with = valid_excuse_receipts
            job.comment = "excused (pass)"
            logger.info("%s %s %s", job.name, job.comment, excuse_detail)
            return
        else:
            job.comment = "badly excused (fail)"
            logger.info("%s %s %s", job.name, job.comment, excuse_detail)
            return

    if job.is_declined():
        job.comment = "declined"
        logger.info("%s %s (reason=%s)", job.name, job.comment, job.decline_reason())
        return

    if job.job_response is None:
        job.comment = "timed out"
        logger.info("%s %s", job.name, job.comment)
        return

    if isinstance(job.job_response, V0JobFailedRequest):
        returncode = job.job_response.docker_process_exit_status
        job.comment = f"failed: {returncode=}"
        logger.info("%s %s", job.name, job.comment)
        return

    assert isinstance(job.job_response, V0JobFinishedRequest)
    assert job.job_response_time is not None

    # !!! time_took can be negative if miner sends responses out of order
    if job.job_before_sent_time is not None:
        job.time_took = job.job_response_time - job.job_before_sent_time
    else:
        job.time_took = None

    if job.time_took is None or job.time_took.total_seconds() <= 0:
        job.comment = "out of order job response"
        logger.info("%s %s", job.name, job.comment)
        job.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
            description=job.comment,
        )
        return

    if _GIVE_AVERAGE_JOB_SEND_TIME_BONUS:
        # subtract the average time to send a job request. this will normalize
        # the timing between validators with different upload speeds.
        # if the time becomes negative, set it to 1 sec
        assert ctx.average_job_send_time is not None
        job.average_job_send_time_bonus = ctx.average_job_send_time
        job.time_took -= ctx.average_job_send_time
        if job.time_took.total_seconds() <= 0:
            job.time_took = timedelta(seconds=1)

    time_took_sec = job.time_took.total_seconds()

    job.correct, comment = job.job_generator.verify_correctness(
        job.job_response,
    )

    if job.job_generator.verify_time(time_took_sec) == False:  # noqa ; it can be None, in which case we
        # don't trigger the code below
        job.comment = f"took too long: {time_took_sec=:.2f}"
        logger.info("%s %s", job.name, job.comment)
        job.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
            description=job.comment,
        )
        return

    job.success = job.correct
    job.comment = comment
    job.score = float(job.correct)

    if job.success:
        job.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_SUCCESS,
            subtype=SystemEvent.EventSubType.SUCCESS,
            description=job.comment,
            data=dict(
                executor_class=job.executor_class.value,
            ),
        )
    else:
        job.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.FAILURE,
            description=job.comment,
            data=dict(
                executor_class=job.executor_class.value,
            ),
        )

        # NOTE: We generally want data to be dict[str, str].
        # Since this code block is here only for debugging purpose,
        # we are passing non-conforming data here with 'type: ignore'.
        if isinstance(job.job_generator, LlmPromptsSyntheticJobGenerator):
            job.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.LLM_PROMPT_ANSWERS_MISSING,
                description="failed synthetic llm job details",
                data={
                    "prompts_url": job.job_generator.s3_url,
                    "answers_url": job.job_generator.url_for_download(),
                    "seed": job.job_generator.seed,
                    "known_answers": {
                        p.content: p.answer for p in job.job_generator.expected_prompts
                    },
                    "job_response": job.job_response.model_dump(mode="json"),
                },
            )

    logger.info(
        "%s finished with %s in %.2f seconds with score %.6g: %s",
        job.name,
        "success" if job.success else "failure",
        time_took_sec,
        job.score,
        job.comment,
    )


class LlmAnswerDownloadTask:
    def __init__(self, job: Job):
        self.job = job
        self.attempt = 0
        self.last_tried: datetime | None = None

        assert isinstance(job.job_generator, LlmPromptsSyntheticJobGenerator)
        self.job_generator = job.job_generator


class LlmAnswerDownloadTaskFailed(Exception):
    def __init__(self, msg: str, task: LlmAnswerDownloadTask, last_exception_tb: str | None = None):
        super().__init__(msg)
        self.task = task
        self.last_exception_tb = last_exception_tb


async def _download_llm_prompts_answers_worker(
    queue: asyncio.Queue[LlmAnswerDownloadTask],
    client: httpx.AsyncClient,
) -> list[LlmAnswerDownloadTaskFailed]:
    failures = []
    while True:
        try:
            task = queue.get_nowait()
        except asyncio.QueueEmpty:
            # No task left in the queue, exit worker loop.
            # Note: if a task is put back into the queue for retry after this worker exits,
            # the worker putting it back should still be alive. So the task will not be ignored.
            break

        if task.last_tried:
            backoff_seconds = (
                _LLM_ANSWERS_DOWNLOAD_RETRY_MIN_BACKOFF * (2**task.attempt) + 0.1 * random.random()
            )
            sleep_until = task.last_tried + timedelta(seconds=backoff_seconds)
            if sleep_until > datetime.now(tz=UTC):
                sleep_time = sleep_until - datetime.now(tz=UTC)
                await asyncio.sleep(sleep_time.total_seconds())

        try:
            await task.job_generator.download_answers(client)
        except httpx.HTTPError as exc:
            logger.warning(
                "llm prompt answers download failed at attempt %s with exception: %r",
                task.attempt,
                exc,
            )
            task.last_tried = datetime.now(tz=UTC)
            task.attempt += 1
            if task.attempt < _LLM_ANSWERS_DOWNLOAD_MAX_ATTEMPTS:
                queue.put_nowait(task)
            else:
                msg = "llm prompt answer download task exceeded max attempts"
                logger.warning(msg)
                failures.append(
                    LlmAnswerDownloadTaskFailed(msg, task, last_exception_tb=traceback.format_exc())
                )

    return failures


async def _download_llm_prompts_answers(ctx: BatchContext) -> None:
    start_time = time.time()

    finished_llm_jobs = []
    task_queue: asyncio.Queue[LlmAnswerDownloadTask] = asyncio.Queue()

    for job in ctx.jobs.values():
        if (
            job.executor_class == LLM_EXECUTOR_CLASS
            and isinstance(job.job_generator, LlmPromptsSyntheticJobGenerator)
            and isinstance(job.job_response, V0JobFinishedRequest)
        ):
            finished_llm_jobs.append(job)
            task_queue.put_nowait(LlmAnswerDownloadTask(job))

    num_workers = min(task_queue.qsize(), _LLM_ANSWERS_DOWNLOAD_MAX_WORKERS)
    async with httpx.AsyncClient(timeout=ctx.batch_config.llm_answer_s3_download_timeout) as client:
        workers = [
            _download_llm_prompts_answers_worker(task_queue, client) for _ in range(num_workers)
        ]
        results = await asyncio.gather(*workers, return_exceptions=True)

    for result in results:
        if isinstance(result, BaseException):
            logger.warning("llm prompt answer download worker failed: %r", result)
            ctx.system_event(
                type=SystemEvent.EventType.VALIDATOR_TELEMETRY,
                subtype=SystemEvent.EventSubType.LLM_PROMPT_ANSWERS_DOWNLOAD_WORKER_FAILED,
                description=repr(result),
                func="_download_llm_prompts_answers",
            )
        else:
            assert isinstance(result, list)
            for exc in result:
                assert isinstance(exc, LlmAnswerDownloadTaskFailed)
                job = exc.task.job
                logger.warning("failed to get llm prompt answers of %s: %r", job.name, exc)
                ctx.system_event(
                    type=SystemEvent.EventType.VALIDATOR_TELEMETRY,
                    subtype=SystemEvent.EventSubType.ERROR_DOWNLOADING_FROM_S3,
                    data={"last_exception": exc.last_exception_tb},
                    description=repr(exc),
                    miner_hotkey=job.miner_hotkey,
                    func="_download_llm_prompts_answers",
                )

    duration = time.time() - start_time
    logger.info("Downloaded miners' llm prompt answers in %.2f seconds", duration)


async def _score_jobs(ctx: BatchContext) -> None:
    for job in ctx.jobs.values():
        try:
            await _score_job(ctx, job)
        except (Exception, asyncio.CancelledError) as exc:
            logger.warning("%s failed to score: %r", job.name, exc)
            job.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.MINER_SCORING_ERROR,
                description=repr(exc),
                func="_score_jobs",
            )

    # compute for each hotkey how many executors finished successfully
    for job in ctx.jobs.values():
        if job.success or job.excused:
            ctx.online_executor_count[job.miner_hotkey][job.executor_class] += 1


# sync_to_async is needed since we use the sync Django ORM
@sync_to_async
def _db_persist_system_events(ctx: BatchContext) -> None:
    if not ctx.events:
        return

    logger.info("Persisting %d system events", len(ctx.events))
    try:
        # it's possible some events were already inserted during
        # a previous call, but the operation failed before clearing
        # the events list, so ignore insert conflicts
        SystemEvent.objects.bulk_create(ctx.events, ignore_conflicts=True)
        # we call this function multiple times during a batch,
        # clear the list to avoid persisting the same event
        # multiple times
        ctx.events.clear()
    except Exception as exc:
        logger.error("Failed to persist system events: %r", exc)


# sync_to_async is needed since we use the sync Django ORM
@sync_to_async
def _db_persist_critical(ctx: BatchContext) -> None:
    start_time = time.time()

    # persist the batch and the jobs in the same transaction, to
    # prevent a situation where because of a crash only some of
    # the jobs are saved, which would generate incorrect weights
    with transaction.atomic():
        # accepting_results_until is not used anywhere, it doesn't
        # matter that we pick a somewhat arbitrary time for it
        now = datetime.now(tz=UTC)
        ctx.batch.accepting_results_until = ctx.stage_start_time.get("_multi_send_job_request", now)
        ctx.batch.save()

        synthetic_jobs: list[SyntheticJob] = []
        for job in ctx.jobs.values():
            miner = ctx.miners[job.miner_hotkey]
            if job.success:
                status = SyntheticJob.Status.COMPLETED
            elif job.excused:
                status = SyntheticJob.Status.EXCUSED
            else:
                status = SyntheticJob.Status.FAILED
            synthetic_job = SyntheticJob(
                job_uuid=job.uuid,
                batch=ctx.batch,
                miner=miner,
                miner_address=miner.address,
                miner_address_ip_version=miner.ip_version,
                miner_port=miner.port,
                executor_class=job.executor_class,
                status=status,
                comment=job.comment,
                job_description=job.job_generator.job_description(),
                score=job.score,
            )
            synthetic_jobs.append(synthetic_job)
        synthetic_jobs = SyntheticJob.objects.bulk_create(synthetic_jobs)
    duration = time.time() - start_time
    logger.info("Persisted to database in %.2f seconds", duration)


# sync_to_async is needed since we use the sync Django ORM
@sync_to_async
def _db_persist(ctx: BatchContext) -> None:
    start_time = time.time()

    # TODO: refactor into nicer abstraction
    synthetic_jobs_map: dict[str, SyntheticJob] = {
        str(synthetic_job.job_uuid): synthetic_job
        for synthetic_job in ctx.batch.synthetic_jobs.all()
    }
    prompt_samples: list[PromptSample] = []

    for job in ctx.jobs.values():
        if job.executor_class != LLM_EXECUTOR_CLASS:
            continue
        if not isinstance(job.job_generator, LlmPromptsSyntheticJobGenerator):
            logger.warning(f"Skipped non-LLM job: {job.job_generator.__class__.__name__}")
            continue
        prompt_sample = job.job_generator.prompt_sample
        prompt_sample.synthetic_job = synthetic_jobs_map.get(job.uuid)
        prompt_samples.append(prompt_sample)

    PromptSample.objects.bulk_update(prompt_samples, fields=["synthetic_job"])

    job_started_receipts: list[JobStartedReceipt] = []
    for job in ctx.jobs.values():
        if (
            job.job_started_receipt_payload is not None
            and job.job_started_receipt_signature is not None
        ):
            started_payload = job.job_started_receipt_payload
            job_started_receipts.append(
                JobStartedReceipt(
                    job_uuid=started_payload.job_uuid,
                    miner_hotkey=started_payload.miner_hotkey,
                    validator_hotkey=started_payload.validator_hotkey,
                    validator_signature=job.job_started_receipt_signature,
                    timestamp=started_payload.timestamp,
                    executor_class=started_payload.executor_class,
                    is_organic=False,
                    ttl=started_payload.ttl,
                )
            )
    JobStartedReceipt.objects.bulk_create(job_started_receipts, ignore_conflicts=True)

    job_accepted_receipts: list[JobAcceptedReceipt] = []
    for job in ctx.jobs.values():
        if job.job_accepted_receipt is not None:
            accepted_payload = job.job_accepted_receipt.payload
            job_accepted_receipts.append(
                JobAcceptedReceipt(
                    job_uuid=accepted_payload.job_uuid,
                    miner_hotkey=accepted_payload.miner_hotkey,
                    validator_hotkey=accepted_payload.validator_hotkey,
                    validator_signature=job.job_accepted_receipt.signature,
                    timestamp=accepted_payload.timestamp,
                    time_accepted=accepted_payload.time_accepted,
                    ttl=accepted_payload.ttl,
                )
            )
    JobAcceptedReceipt.objects.bulk_create(job_accepted_receipts, ignore_conflicts=True)

    job_finished_receipts: list[JobFinishedReceipt] = []
    for job in ctx.jobs.values():
        if job.job_finished_receipt is not None:
            finished_payload = job.job_finished_receipt.payload
            job_finished_receipts.append(
                JobFinishedReceipt(
                    job_uuid=finished_payload.job_uuid,
                    miner_hotkey=finished_payload.miner_hotkey,
                    validator_hotkey=finished_payload.validator_hotkey,
                    validator_signature=job.job_finished_receipt.signature,
                    timestamp=finished_payload.timestamp,
                    time_started=finished_payload.time_started,
                    time_took_us=finished_payload.time_took_us,
                    score_str=finished_payload.score_str,
                )
            )
    JobFinishedReceipt.objects.bulk_create(job_finished_receipts, ignore_conflicts=True)

    duration = time.time() - start_time
    logger.info("Persisted to database in %.2f seconds", duration)


def shuffled(list_: list[Any]) -> list[Any]:
    return random.sample(list_, len(list_))


async def _connect_miner_client(ctx: BatchContext, miner_hotkey: str):
    """
    Connect to a miner client and handle connection errors.
    """
    client = ctx.clients[miner_hotkey]

    try:
        await client.connect()
    except TransportConnectionError as exc:
        name = ctx.names[miner_hotkey]
        logger.warning("%s connection error: %r", name, exc)
        ctx.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.MINER_CONNECTION_ERROR,
            description=repr(exc),
            miner_hotkey=miner_hotkey,
            func="connect",
        )
        return


async def _connect_all_clients(ctx):
    tasks = []
    for hotkey in ctx.hotkeys:
        tasks.append(_connect_miner_client(ctx, hotkey))
    await asyncio.gather(*tasks)


async def execute_synthetic_batch_run(
    serving_miners: list[Miner],
    active_validators: list[ValidatorInfo],
    batch_id: int,
    create_miner_client: _MinerClientFactoryProtocol | None = None,
) -> None:
    if not serving_miners:
        logger.warning("No miners provided")
        return

    start_time = datetime.now(tz=UTC)
    logger.info("Executing synthetic jobs batch for %d miners", len(serving_miners))

    # randomize the order of miners each batch to avoid systemic bias
    shuffled(serving_miners)

    ctx = await _init_context(serving_miners, active_validators, batch_id, create_miner_client)
    await ctx.checkpoint_system_event("BATCH_BEGIN", dt=start_time)

    try:
        ctx.loop_profiler = LoopProfiler(ctx)

        # Connect to the miners.
        await _connect_all_clients(ctx)

        await ctx.checkpoint_system_event("_multi_get_miner_manifest")
        await _multi_get_miner_manifest(ctx)
        await _adjust_miner_max_executors_per_class(ctx)

        await ctx.checkpoint_system_event("_limit_non_peak_executors_per_class")
        await _limit_non_peak_executors_per_class(ctx)

        await ctx.checkpoint_system_event("_get_total_executor_count")
        total_executor_count = _get_total_executor_count(ctx)

        if total_executor_count > 0:
            await ctx.checkpoint_system_event("_generate_jobs")
            await _generate_jobs(ctx)

            # randomize the order of jobs each batch to avoid systemic bias
            shuffled(ctx.job_uuids)

            await ctx.checkpoint_system_event("_multi_send_initial_job_request")
            await _multi_send_initial_job_request(ctx)

            executor_ready_jobs = await _get_executor_ready_jobs(ctx)

            any_job_busy = any(
                job.decline_reason() == JobRejectionReason.BUSY for job in ctx.jobs.values()
            )

            if executor_ready_jobs:
                await ctx.checkpoint_system_event("_multi_send_job_request_for_streaming")
                await _multi_send_job_request_for_streaming(ctx, executor_ready_jobs)

                await ctx.checkpoint_system_event("_trigger_job_execution")

                await _trigger_job_execution(ctx, executor_ready_jobs)

                # don't persist system events before this point, we want to minimize
                # any extra interactions which could slow down job processing before
                # we get the responses from the miners
                await _db_persist_system_events(ctx)

                await ctx.checkpoint_system_event("_compute_average_send_time")
                _compute_average_send_time(ctx)

                # NOTE: download the answers for llm prompts jobs before scoring
                await ctx.checkpoint_system_event("_download_llm_prompts_answers")
                await _download_llm_prompts_answers(ctx)
            else:
                logger.warning("No executor ready jobs")

            if executor_ready_jobs or any_job_busy:
                await ctx.checkpoint_system_event("_score_jobs")
                await _score_jobs(ctx)

            if executor_ready_jobs:
                await _db_persist_system_events(ctx)

                await ctx.checkpoint_system_event("_send_job_finished_receipts")
                await _send_job_finished_receipts(ctx)

            if not executor_ready_jobs:
                logger.warning("No jobs accepted")

            await _db_persist_system_events(ctx)

            await ctx.checkpoint_system_event("_emit_decline_or_failure_events")
            _emit_decline_or_failure_events(ctx)

        else:
            logger.warning("No executors available")

        await ctx.checkpoint_system_event("loop_profiler.close")
        await ctx.loop_profiler.close()

    except (Exception, asyncio.CancelledError) as exc:
        logger.error("Synthetic jobs batch failure: ", exc_info=exc)
        ctx.system_event(
            type=SystemEvent.EventType.VALIDATOR_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
            description=repr(exc),
            func="execute_synthetic_batch_run",
        )

    await _db_persist_system_events(ctx)

    await ctx.checkpoint_system_event("_multi_close_client")
    try:
        await _multi_close_client(ctx)
    except (Exception, asyncio.CancelledError) as exc:
        raise exc
        logger.error("Synthetic jobs batch failure: %r", exc)
        ctx.system_event(
            type=SystemEvent.EventType.VALIDATOR_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
            description=repr(exc),
            func="_multi_close_client",
        )

    await ctx.checkpoint_system_event("_db_persist_critical")
    await _db_persist_critical(ctx)

    await ctx.checkpoint_system_event("_emit_telemetry_events")
    try:
        await _emit_telemetry_events(ctx)
    except (Exception, asyncio.CancelledError) as exc:
        raise exc
        logger.error("Synthetic jobs batch failure: %r", exc)
        ctx.system_event(
            type=SystemEvent.EventType.VALIDATOR_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
            description=repr(exc),
            func="_emit_telemetry_events",
        )

    await _db_persist_system_events(ctx)

    await ctx.checkpoint_system_event("_db_persist")
    await _db_persist(ctx)

    # we turn off specs cause it is unreliable to send them over channels and we
    # have already this data in telemetry event - but processing telemetry is slow
    # so we might try this way another time - just turn it of as hotfix
    if _SEND_MACHINE_SPECS:
        # send the machine specs after the batch is done, it can fail or take a long time
        await ctx.checkpoint_system_event("_send_machine_specs")
        try:
            await _send_machine_specs(ctx)
        except (Exception, asyncio.CancelledError) as exc:
            logger.error("Synthetic jobs batch failure: %r", exc)

    await _db_persist_system_events(ctx)

    await ctx.checkpoint_system_event("BATCH_END")
