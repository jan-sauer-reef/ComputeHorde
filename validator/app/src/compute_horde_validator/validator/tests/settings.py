import os
import pathlib

import bittensor_wallet
from compute_horde.test_wallet import (
    VALIDATOR_WALLET_HOTKEY,
    VALIDATOR_WALLET_NAME,
    get_validator_wallet,
)

os.environ.update(
    {
        "DEBUG_TOOLBAR": "False",
    }
)

from compute_horde_validator.settings import *  # noqa: E402,F403

PROMETHEUS_EXPORT_MIGRATIONS = False

BITTENSOR_NETUID = 12
BITTENSOR_NETWORK = "local"

CELERY_TASK_ALWAYS_EAGER = True

BITTENSOR_WALLET_DIRECTORY = pathlib.Path("~").expanduser() / ".bittensor" / "wallets"
BITTENSOR_WALLET_NAME = VALIDATOR_WALLET_NAME
BITTENSOR_WALLET_HOTKEY_NAME = VALIDATOR_WALLET_HOTKEY

STATS_COLLECTOR_URL = "http://fakehost:8000"


def BITTENSOR_WALLET() -> bittensor_wallet.Wallet:
    return get_validator_wallet()


DEFAULT_ADMIN_PASSWORD = "fake_admin_password"

AWS_ACCESS_KEY_ID = "fake_access_key_id"
AWS_SECRET_ACCESS_KEY = "fake_secret_access_key"

S3_BUCKET_NAME_PROMPTS = "fake_bucket_prompts"
S3_BUCKET_NAME_ANSWERS = "fake_bucket_answers"

TRUSTED_MINER_ADDRESS = "fakehost"
TRUSTED_MINER_PORT = 1234

CACHES = {"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}
CONSTANCE_DATABASE_CACHE_BACKEND = None

DEBUG_MINER_KEY = None
