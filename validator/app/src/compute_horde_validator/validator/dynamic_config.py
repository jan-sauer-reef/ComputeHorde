from collections.abc import Callable
from contextlib import suppress
from typing import Any, TypeAlias

from asgiref.sync import sync_to_async
from compute_horde_core.executor_class import ExecutorClass
from constance import config
from django.conf import settings

from compute_horde_validator.validator.models import SystemEvent


async def aget_config(key: str) -> Any:
    return await sync_to_async(lambda: getattr(config, key))()


async def aget_weights_version() -> int:
    if settings.DEBUG_OVERRIDE_WEIGHTS_VERSION is not None:
        return int(settings.DEBUG_OVERRIDE_WEIGHTS_VERSION)
    return int(await aget_config("DYNAMIC_WEIGHTS_VERSION"))


def get_weights_version() -> int:
    if settings.DEBUG_OVERRIDE_WEIGHTS_VERSION is not None:
        return int(settings.DEBUG_OVERRIDE_WEIGHTS_VERSION)
    return int(config.DYNAMIC_WEIGHTS_VERSION)


# this is called from a sync context, and rarely, so we don't need caching
def get_synthetic_jobs_flow_version():
    if settings.DEBUG_OVERRIDE_SYNTHETIC_JOBS_FLOW_VERSION is not None:
        return settings.DEBUG_OVERRIDE_SYNTHETIC_JOBS_FLOW_VERSION
    return config.DYNAMIC_SYNTHETIC_JOBS_FLOW_VERSION


def executor_class_value_map_parser(
    value_map_str: str, value_parser: Callable[[str], Any] | None = None
) -> dict[ExecutorClass, Any]:
    result = {}
    for pair in value_map_str.split(","):
        # ignore errors for misconfiguration, i,e. non-existent executor classes,
        # non-integer/negative counts etc.
        with suppress(ValueError):
            executor_class_str, value_str = pair.split("=")
            executor_class = ExecutorClass(executor_class_str)
            if value_parser is not None:
                parsed_value = value_parser(value_str)
            else:
                parsed_value = value_str
            result[executor_class] = parsed_value
    return result


def executor_class_array_parser(value_map_str: str) -> set[ExecutorClass]:
    result = set()
    for executor_class_str in value_map_str.split(","):
        with suppress(ValueError):
            result.add(ExecutorClass(executor_class_str))
    return result


async def get_miner_max_executors_per_class() -> dict[ExecutorClass, int]:
    miner_max_executors_per_class: str = await aget_config("DYNAMIC_MINER_MAX_EXECUTORS_PER_CLASS")
    result = {
        executor_class: count
        for executor_class, count in executor_class_value_map_parser(
            miner_max_executors_per_class, value_parser=int
        ).items()
        if count >= 0
    }
    return result


def get_miner_max_executors_per_class_sync() -> dict[ExecutorClass, int]:
    miner_max_executors_per_class: str = config.DYNAMIC_MINER_MAX_EXECUTORS_PER_CLASS
    result = {
        executor_class: count
        for executor_class, count in executor_class_value_map_parser(
            miner_max_executors_per_class, value_parser=int
        ).items()
        if count >= 0
    }
    return result


async def get_default_executor_limits_for_missed_peak() -> dict[ExecutorClass, int]:
    default_limits: str = await aget_config("DYNAMIC_DEFAULT_EXECUTOR_LIMITS_FOR_MISSED_PEAK")
    return executor_class_value_map_parser(default_limits, value_parser=int)


def get_executor_class_weights() -> dict[ExecutorClass, float]:
    return executor_class_value_map_parser(
        config.DYNAMIC_EXECUTOR_CLASS_WEIGHTS, value_parser=float
    )


LimitsDict: TypeAlias = dict[tuple[SystemEvent.EventType, SystemEvent.EventSubType], int]


def parse_system_event_limits(raw_limits: str) -> LimitsDict:
    limits = {}
    for limit_item in raw_limits.split(";"):
        with suppress(ValueError):
            type_str, subtype_str, limit_str = limit_item.split(",")
            type_ = SystemEvent.EventType(type_str)
            subtype = SystemEvent.EventSubType(subtype_str)
            limits[(type_, subtype)] = int(limit_str)
    return limits


async def get_system_event_limits() -> LimitsDict:
    raw_limits: str = await aget_config("DYNAMIC_SYSTEM_EVENT_LIMITS")
    return parse_system_event_limits(raw_limits)


async def get_streaming_job_executor_classes() -> set[ExecutorClass]:
    value = await aget_config("DYNAMIC_SYNTHETIC_STREAMING_JOB_EXECUTOR_CLASSES")
    return executor_class_array_parser(value)
