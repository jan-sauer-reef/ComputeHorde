from datetime import timedelta
from math import ceil
from operator import attrgetter
from uuid import uuid4

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from compute_horde import protocol_consts
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import (
    V0JobCheated,
    V2JobRequest,
)
from compute_horde.fv_protocol.validator_requests import JobStatusMetadata
from compute_horde_core.output_upload import (
    MultiUpload,
    SingleFileUpload,
)
from compute_horde_core.signature import Signature
from compute_horde_core.streaming import StreamingDetails
from compute_horde_core.volume import MultiVolume
from django.conf import settings
from django.contrib.postgres.fields import ArrayField
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.db.models import CheckConstraint, Max, Prefetch, Q, QuerySet, UniqueConstraint
from django.urls import reverse
from django.utils.timezone import now
from django_prometheus.models import ExportModelOperationsMixin
from django_pydantic_field import SchemaField
from structlog import get_logger
from structlog.contextvars import bound_contextvars

from .schemas import (
    MuliVolumeAllowedVolume,
)

log = get_logger(__name__)


class MutuallyExclusiveFieldsError(ValidationError):
    pass


class JobNotFinishedError(Exception):
    pass


class JobCreationDisabledError(Exception):
    pass


class AbstractNodeQuerySet(models.QuerySet):
    def with_last_job_time(self) -> QuerySet:
        return self.annotate(last_job_time=Max("jobs__created_at"))


class AbstractNode(models.Model):
    ss58_address = models.CharField(max_length=48, unique=True)
    is_active = models.BooleanField()

    objects = AbstractNodeQuerySet.as_manager()

    class Meta:
        abstract = True
        constraints = [
            UniqueConstraint(fields=["ss58_address"], name="unique_%(class)s_ss58_address"),
        ]

    def __str__(self) -> str:
        return self.ss58_address


class Validator(AbstractNode):
    version = models.CharField(max_length=255, blank=True, default="")
    runner_version = models.CharField(max_length=255, blank=True, default="")


class Miner(AbstractNode):
    pass


class MinerVersion(models.Model):
    miner = models.ForeignKey(Miner, on_delete=models.CASCADE, related_name="versions")
    version = models.CharField(max_length=255, blank=True, default="")
    runner_version = models.CharField(max_length=255, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["created_at"]),
        ]


class Channel(models.Model):
    """
    This is a simple model to remember Validator-channel association.

    When new WS connection is established, new instance of this class is instantiated,
    thus we always know which channel(s) belong to which validator. When validator
    disconnects from WS, this instance is deleted as well.

    This is a more straightforward approach than using Django Channels groups which
    don't have a built-in method to get all channels within a group.
    """

    name = models.CharField(max_length=255)
    validator = models.ForeignKey(Validator, on_delete=models.CASCADE, related_name="channels")
    last_heartbeat = models.DateTimeField(default=now)

    class Meta:
        constraints = [
            UniqueConstraint(fields=["name"], name="unique_channel_name"),
        ]

    def __str__(self) -> str:
        return self.name


class JobQuerySet(models.QuerySet):
    def with_statuses(self) -> QuerySet:
        return self.prefetch_related(Prefetch("statuses", queryset=JobStatus.objects.order_by("created_at")))


class Job(ExportModelOperationsMixin("job"), models.Model):
    uuid = models.UUIDField(primary_key=True, editable=False, blank=True)
    user = models.ForeignKey("auth.User", on_delete=models.PROTECT, blank=True, null=True, related_name="jobs")
    hotkey = models.CharField(blank=True, help_text="hotkey of job sender if hotkey authentication was used")
    validator = models.ForeignKey(Validator, blank=True, on_delete=models.PROTECT, related_name="jobs")
    signature = models.JSONField()
    created_at = models.DateTimeField(default=now)
    cheated = models.BooleanField(default=False)
    download_time_limit = models.IntegerField()
    execution_time_limit = models.IntegerField()
    streaming_start_time_limit = models.IntegerField()
    upload_time_limit = models.IntegerField()

    executor_class = models.CharField(
        max_length=255, default=DEFAULT_EXECUTOR_CLASS, help_text="executor hardware class"
    )
    docker_image = models.CharField(max_length=255, blank=True, help_text="docker image for job execution")
    job_namespace = models.CharField(
        max_length=100, blank=True, null=True, help_text="namespace specifying where the job comes from"
    )

    args = ArrayField(
        models.TextField(),
        default=list,
        null=True,
        blank=True,
        help_text="arguments passed to the script or docker image",
    )
    env = models.JSONField(blank=True, default=dict, help_text="environment variables for the job")
    use_gpu = models.BooleanField(default=False, help_text="Whether to use GPU for the job")
    target_validator_hotkey = models.TextField(help_text="target validator")
    volumes = SchemaField(schema=list[MuliVolumeAllowedVolume], blank=True, default=list)
    uploads = SchemaField(schema=list[SingleFileUpload], blank=True, default=list)
    artifacts_dir = models.CharField(max_length=255, blank=True, help_text="image mount directory for artifacts")
    artifacts = models.JSONField(blank=True, default=dict)
    on_trusted_miner = models.BooleanField(default=False)
    upload_results = models.JSONField(blank=True, default=dict)

    tag = models.CharField(max_length=255, blank=True, default="", help_text="may be used to group jobs")

    streaming_client_cert = models.TextField(blank=True, default="")
    streaming_server_cert = models.TextField(blank=True, default="")
    streaming_server_address = models.TextField(blank=True, default="")
    streaming_server_port = models.IntegerField(blank=True, default=0)

    objects = JobQuerySet.as_manager()

    class Meta:
        constraints = [
            CheckConstraint(
                check=Q(user__isnull=True) & ~Q(hotkey="") | Q(user__isnull=False) & Q(hotkey=""),
                name="user_or_hotkey",
            ),
        ]
        indexes = [
            models.Index(fields=["validator", "-created_at"], name="idx_job_validator_created_at"),
            models.Index(fields=["hotkey"], name="idx_job_hotkey"),
        ]

    @property
    def filename(self) -> str:
        assert self.uuid
        return f"{self.uuid}.zip"

    def save(self, *args, **kwargs) -> None:
        is_new = self.pk is None

        self.uuid = self.uuid or uuid4()

        # if there is no validator selected -> we need a transaction for locking
        # active validators during selection process
        with transaction.atomic(), bound_contextvars(job=self):
            self.validator = getattr(self, "validator", None) or self.select_validator()
            super().save(*args, **kwargs)
            if is_new:
                job_request = self.as_job_request().model_dump()
                self.send_to_validator(job_request)
                JobStatus.objects.create(
                    job=self,
                    status=protocol_consts.JobStatus.SENT.value,
                )

    def report_cheated(self, signature: Signature) -> None:
        """
        Notify validator of cheated job.
        """
        payload = V0JobCheated(job_uuid=str(self.uuid), signature=signature).model_dump()
        log.debug("sending cheated report", payload=payload)
        self.send_to_validator(payload)

    def select_validator(self) -> Validator:
        """
        Select a validator for the job.

        Currently the one with least recent job request is selected.
        This method is expected to be run within a transaction.
        """

        # select validators which are currently connected via WS
        validator_ids: set[int] = set(
            Channel.objects.filter(last_heartbeat__gte=now() - timedelta(minutes=3)).values_list(
                "validator_id", flat=True
            )
        )
        log.debug("connected validators", validator_ids=validator_ids)

        if self.signature is None:
            raise ValueError("Request must be signed when target_validator_hotkey is set")
        validator = Validator.objects.filter(ss58_address=self.target_validator_hotkey).first()
        if validator and validator.id in validator_ids:
            log.debug("selected (targeted) validator", validator=validator)
            return validator
        raise Validator.DoesNotExist

    @property
    def sender(self) -> str:
        return self.hotkey or self.user.username

    def __str__(self) -> str:
        return f"Job {self.pk} by {self.sender}"

    def get_absolute_url(self) -> str:
        return reverse("job/detail", kwargs={"pk": self.pk})

    @property
    def statuses_ordered(self) -> list["JobStatus"]:
        # sort in python to reuse the prefetch cache
        return sorted(self.statuses.all(), key=attrgetter("created_at"))

    @property
    def status(self) -> "JobStatus":
        return self.statuses_ordered[-1]

    def is_completed(self) -> bool:
        # TODO: TIMEOUTS - Status updates from validator should include a timeout until next update. Job is failed if timeout is reached.
        return self.status.status in protocol_consts.JobStatus.end_states()

    @property
    def elapsed(self) -> timedelta:
        """Time between first and last statuses."""
        statuses = self.statuses_ordered
        return statuses[-1].created_at - statuses[0].created_at

    def as_job_request(self) -> V2JobRequest:
        volume = MultiVolume(volumes=self.volumes) if self.volumes else None
        output_upload = MultiUpload(uploads=self.uploads, system_output=None) if self.uploads else None
        signature = Signature.model_validate(self.signature)
        return V2JobRequest(
            uuid=str(self.uuid),
            executor_class=self.executor_class,
            docker_image=self.docker_image,
            job_namespace=self.job_namespace,
            args=self.args,
            env=self.env,
            use_gpu=self.use_gpu,
            volume=volume,
            output_upload=output_upload,
            signature=signature,
            artifacts_dir=self.artifacts_dir or None,
            on_trusted_miner=self.on_trusted_miner,
            download_time_limit=self.download_time_limit,
            execution_time_limit=self.execution_time_limit,
            streaming_start_time_limit=self.streaming_start_time_limit,
            upload_time_limit=self.upload_time_limit,
            streaming_details=StreamingDetails(public_key=self.streaming_client_cert)
            if self.streaming_client_cert
            else None,
        )

    def send_to_validator(self, payload: dict) -> None:
        channels_names = Channel.objects.filter(validator=self.validator).values_list("name", flat=True)
        log.debug("sending job to validator", job=self, validator=self.validator, channels_names=channels_names)
        channel_layer = get_channel_layer()
        send = async_to_sync(channel_layer.send)
        for channel_name in channels_names:
            send(channel_name, payload)


class JobStatus(ExportModelOperationsMixin("job_status"), models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name="statuses")
    status = models.CharField(choices=protocol_consts.JobStatus.choices())
    metadata = models.JSONField(blank=True, default=dict)
    created_at = models.DateTimeField(default=now)

    class Meta:
        verbose_name_plural = "Job statuses"
        constraints = [
            UniqueConstraint(fields=["job", "status"], name="unique_job_status"),
        ]

    def __str__(self) -> str:
        return self.get_status_display()

    @property
    def meta(self) -> JobStatusMetadata | None:
        if self.metadata:
            return JobStatusMetadata.model_validate(self.metadata)

    def get_legacy_status_display(self) -> str:
        """
        Job serializer uses this for the legacy "status" field.
        - Older SDK clients require the human-readable label
        - The HORDE_FAILED status is new and will not be accepted
        """
        status_display = {
            protocol_consts.JobStatus.SENT.value: "Sent",
            protocol_consts.JobStatus.RECEIVED.value: "Received",
            protocol_consts.JobStatus.ACCEPTED.value: "Accepted",
            protocol_consts.JobStatus.EXECUTOR_READY.value: "Executor Ready",
            protocol_consts.JobStatus.STREAMING_READY.value: "Streaming Ready",
            protocol_consts.JobStatus.VOLUMES_READY.value: "Volumes Ready",
            protocol_consts.JobStatus.EXECUTION_DONE.value: "Execution Done",
            protocol_consts.JobStatus.COMPLETED.value: "Completed",
            protocol_consts.JobStatus.REJECTED.value: "Rejected",
            protocol_consts.JobStatus.FAILED.value: "Failed",
            protocol_consts.JobStatus.HORDE_FAILED.value: "Failed",
        }
        return status_display[self.status]


class JobFeedback(models.Model):
    """
    Represents end user feedback for a job.
    """

    job = models.OneToOneField(Job, on_delete=models.CASCADE, related_name="feedback")
    user = models.ForeignKey("auth.User", on_delete=models.PROTECT, related_name="feedback")
    created_at = models.DateTimeField(default=now)

    result_correctness = models.FloatField(default=1, help_text="<0-1> where 1 means 100% correct")
    expected_duration = models.FloatField(blank=True, null=True, help_text="Expected duration of the job in seconds")
    signature = models.JSONField(blank=True, null=True)

    def __str__(self) -> str:
        return f"Feedback for job {self.job.uuid} by {self.user.username}"


class Subnet(models.Model):
    name = models.CharField(max_length=255)
    uid = models.PositiveSmallIntegerField()

    class Meta:
        constraints = [
            UniqueConstraint(fields=["name"], name="unique_subnet_name"),
            UniqueConstraint(fields=["uid"], name="unique_subnet_uid"),
        ]

    def __str__(self) -> str:
        return f"{self.name} (SN{self.uid})"


class GPU(models.Model):
    name = models.CharField(max_length=255, unique=True)
    capacity = models.PositiveIntegerField(default=0, help_text="in GB")
    memory_type = models.CharField(max_length=255, default="")
    bus_width = models.PositiveIntegerField(default=0, help_text="in bits")
    core_clock = models.PositiveIntegerField(default=0, help_text="in MHz")
    memory_clock = models.PositiveIntegerField(default=0, help_text="in MHz")
    fp16 = models.FloatField(default=0, help_text="in TFLOPS")
    fp32 = models.FloatField(default=0, help_text="in TFLOPS")
    fp64 = models.FloatField(default=0, help_text="in TFLOPS")

    created_at = models.DateTimeField(default=now)

    def __str__(self) -> str:
        return self.name

    def get_verbose_name(self) -> str:
        memory_gb = ceil(self.capacity / 1024)
        return f"{self.name} {memory_gb}GB"


class GpuCount(models.Model):
    subnet = models.ForeignKey(Subnet, on_delete=models.CASCADE, related_name="gpu_counts")
    gpu = models.ForeignKey(GPU, on_delete=models.CASCADE, related_name="counts")
    count = models.PositiveIntegerField()
    measured_at = models.DateTimeField(blank=True, default=now)

    class Meta:
        constraints = [
            UniqueConstraint(fields=["subnet", "gpu", "measured_at"], name="unique_gpu_count"),
        ]

    def __str__(self) -> str:
        return f"{self.count}x {self.gpu.name}"


class HardwareState(models.Model):
    subnet = models.ForeignKey(Subnet, on_delete=models.CASCADE, related_name="hardware_states")
    state = models.JSONField()
    measured_at = models.DateTimeField(default=now)

    def __str__(self) -> str:
        return f"{self.measured_at}"


class OtherSpecs(models.Model):
    # other specs
    os = models.CharField(max_length=255, blank=True, null=True, default="")
    virtualization = models.CharField(max_length=255, blank=True, null=True, default="")
    total_ram = models.PositiveIntegerField(default=0, blank=True, null=True, help_text="in GB")
    total_hdd = models.PositiveIntegerField(default=0, blank=True, null=True, help_text="in GB")
    asn = models.PositiveIntegerField(default=0, blank=True, null=True)

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["os", "virtualization", "total_ram", "total_hdd", "asn"], name="unique_other_specs"
            ),
        ]


class CpuSpecs(models.Model):
    # cpu specs
    cpu_model = models.CharField(max_length=255, default="")
    cpu_count = models.PositiveIntegerField(default=0)

    class Meta:
        constraints = [
            UniqueConstraint(fields=["cpu_model", "cpu_count"], name="unique_cpu_specs"),
        ]


class RawSpecsData(models.Model):
    data = models.JSONField()
    created_at = models.DateTimeField(default=now)

    class Meta:
        constraints = [
            UniqueConstraint(fields=["data"], name="unique_raw_specs_data"),
        ]
        indexes = [
            models.Index(fields=["data"], name="idx_raw_spec_data"),
        ]


class ExecutorSpecsSnapshot(ExportModelOperationsMixin("executor_specs_snapshot"), models.Model):
    batch_id = models.UUIDField(default=None, null=True, blank=True, help_text="job batch id")
    miner = models.ForeignKey(Miner, on_delete=models.CASCADE)
    validator = models.ForeignKey(Validator, on_delete=models.CASCADE)
    measured_at = models.DateTimeField(default=now)

    raw_specs = models.ForeignKey(RawSpecsData, on_delete=models.CASCADE, related_name="raw_specs_data")

    def __str__(self) -> str:
        return (
            f"raw spec for miner: {self.miner.ss58_address}, batch_id {self.batch_id} measured_at: {self.measured_at}"
        )


class ParsedSpecsData(models.Model):
    id = models.OneToOneField(RawSpecsData, primary_key=True, on_delete=models.CASCADE)
    cpu_specs = models.ForeignKey(CpuSpecs, on_delete=models.PROTECT, related_name="cpu_specs")
    other_specs = models.ForeignKey(OtherSpecs, on_delete=models.PROTECT, related_name="other_specs")


class GpuSpecs(ExportModelOperationsMixin("gpu_specs"), models.Model):
    parsed_specs = models.ForeignKey(ParsedSpecsData, on_delete=models.CASCADE, related_name="specs")

    gpu_model = models.ForeignKey(GPU, default=0, on_delete=models.CASCADE)
    gpu_count = models.PositiveIntegerField(default=0)

    capacity = models.PositiveIntegerField(default=0, help_text="in MB")
    cuda = models.CharField(max_length=255, default="", help_text="version")
    driver = models.CharField(max_length=255, default="", help_text="version")
    graphics_speed = models.PositiveIntegerField(default=0, help_text="in MHz")
    memory_speed = models.PositiveIntegerField(default=0, help_text="in MHz")
    power_limit = models.FloatField(default=0, help_text="in MHz")
    uuid = models.CharField(max_length=255, blank=True, null=True, default="")
    serial = models.CharField(max_length=255, blank=True, null=True, default="")

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=[
                    "parsed_specs",
                    "gpu_model",
                    "gpu_count",
                    "capacity",
                    "cuda",
                    "driver",
                    "graphics_speed",
                    "memory_speed",
                    "power_limit",
                    "uuid",
                    "serial",
                ],
                name="unique_gpu_specs",
            ),
        ]


class HotkeyWhitelist(models.Model):
    ss58_address = models.CharField(max_length=255, unique=True)
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, blank=True, null=True, related_name="hotkey_user"
    )

    def __str__(self) -> str:
        return self.ss58_address


# TO BE DEPRECATED
class RawSpecsSnapshot(models.Model):
    miner = models.ForeignKey(Miner, on_delete=models.CASCADE, related_name="raw_specs")
    validator = models.ForeignKey(Validator, on_delete=models.CASCADE, related_name="raw_specs")
    state = models.JSONField()
    measured_at = models.DateTimeField(default=now)

    def __str__(self) -> str:
        return f"raw spec for miner: {self.miner.ss58_address} measured_at: {self.measured_at}"

    class Meta:
        constraints = [
            UniqueConstraint(fields=["miner", "measured_at"], name="unique_raw_spec_miner_measured_at"),
        ]
        indexes = [
            models.Index(fields=["miner", "measured_at"], name="idx_raw_spec_miner_measured_at"),
        ]
