import datetime
from contextlib import contextmanager
from unittest import mock
from unittest.mock import patch

import turbobt
from compute_horde.test_wallet import get_test_validator_wallet
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.allowance.utils import supertensor

from ...tests.helpers import mock_manifest_endpoints

NUM_MINERS = 250
NUM_VALIDATORS = 6
NUM_BLOCKS = 100
START_BLOCK = 1000
START_CHANGING_MANIFESTS_BLOCK = 1010
START_CHANGING_STAKE_BLOCK = 1005
SYNC_MANIFESTS_INTERVAL = 25
MANIFEST_CHANGE_INTERVAL = 300
MANIFEST_FETCHING_TIMEOUT = 1.0

MINER_HOTKEYS = {
    **{
        i: f"stable_miner_{i:03d}" for i in range(NUM_MINERS // 2)
    },  # the manifests of these miners won't change
    **{i: f"whacky_miner_{i:03d}" for i in range(NUM_MINERS // 2, NUM_MINERS - 4)},
    # the manifests of these miners will be crazy, after block START_CHANGING_MANIFESTS_BLOCK
    NUM_MINERS - 5: f"always_increasing_miner_{NUM_MINERS - 5}",
    # the manifest of this miner will increase with each block after START_CHANGING_MANIFESTS_BLOCK
    NUM_MINERS - 4: f"forgetting_miner_{NUM_MINERS - 4}",
    # the manifest of this miner will be missing entries after START_CHANGING_MANIFESTS_BLOCK
    NUM_MINERS - 3: f"deregging_miner_{NUM_MINERS - 3}",
    # this miner will occasionally deregister after START_CHANGING_MANIFESTS_BLOCK
    NUM_MINERS - 2: f"timing_out_miner_{NUM_MINERS - 2}",
    # this miner will occasionally time out when sending the manifest after START_CHANGING_MANIFESTS_BLOCK
    NUM_MINERS - 1: f"malforming_miner_{NUM_MINERS - 1}",
    # this miner will occasionally send a malformed manifest after START_CHANGING_MANIFESTS_BLOCK
}


def cmbm(block_number):
    """
    Curated (trimmed to START_BLOCK if block number lower than START_CHANGING_MANIFESTS_BLOCK) manifest block number.
    This allows us to have a simple manifest evolution function AND make the manifests constant for the first
    `START_CHANGING_MANIFESTS_BLOCK - START_BLOCK` (+/-1) blocks.
    """
    if block_number < START_CHANGING_MANIFESTS_BLOCK:
        return START_BLOCK
    return block_number


VALIDATOR_HOTKEYS = {
    **{i: f"regular_validator_{i}" for i in range(NUM_VALIDATORS - 2) if i != 2},
    2: get_test_validator_wallet().get_hotkey().ss58_address,
    # these validators will have a steadily increasing stake
    NUM_VALIDATORS - 2: f"stake_loosing_validator_{NUM_VALIDATORS - 2}",
    # this validator will occasionally get a stake lower than 1000
    NUM_VALIDATORS
    - 1: f"deregging_validator_{NUM_VALIDATORS - 1}",  # this validator will occasionally deregister
}

EXECUTOR_CLASSES = [
    ExecutorClass.spin_up_4min__gpu_24gb,
    ExecutorClass.always_on__gpu_24gb,
    ExecutorClass.always_on__llm__a6000,
]

EXECUTOR_CAP = {
    ExecutorClass.spin_up_4min__gpu_24gb: 10,
    ExecutorClass.always_on__gpu_24gb: 5,
    ExecutorClass.always_on__llm__a6000: 3,
}


def manifest_responses(block_number) -> list[tuple[str, dict[ExecutorClass, int] | None, float]]:
    assert block_number >= START_BLOCK
    return [
        *[
            (
                hotkey,
                {ec: (ind + uid) % (EXECUTOR_CAP[ec]) for ind, ec in enumerate(EXECUTOR_CLASSES)},
                0,
            )
            for uid, hotkey in MINER_HOTKEYS.items()
            if hotkey.startswith("stable_miner_")
        ],
        *[
            (
                hotkey,
                {
                    ec: (ind + uid + cmbm(block_number)) % (EXECUTOR_CAP[ec])
                    for ind, ec in enumerate(EXECUTOR_CLASSES)
                },
                0,
            )
            for uid, hotkey in MINER_HOTKEYS.items()
            if hotkey.startswith("whacky_miner_")
        ],
        *[
            (
                hotkey,
                {
                    ec: (ind + uid) % (EXECUTOR_CAP[ec]) + cmbm(block_number) - START_BLOCK
                    for ind, ec in enumerate(EXECUTOR_CLASSES)
                },
                0,
            )
            for uid, hotkey in MINER_HOTKEYS.items()
            if hotkey.startswith("always_increasing_miner_")
        ],
        *[
            (
                hotkey,
                {
                    ec: (ind + uid + cmbm(block_number)) % (EXECUTOR_CAP[ec])
                    for ind, ec in enumerate(EXECUTOR_CLASSES)
                    if (block_number < START_CHANGING_MANIFESTS_BLOCK or (block_number + ind) % 2)
                },
                0,
            )
            for uid, hotkey in MINER_HOTKEYS.items()
            if (hotkey.startswith("forgetting_miner_") or hotkey.startswith("deregging_miner_"))
        ],
        *[
            (
                hotkey,
                {ec: (ind + uid) % (EXECUTOR_CAP[ec]) for ind, ec in enumerate(EXECUTOR_CLASSES)},
                0
                if block_number < START_CHANGING_MANIFESTS_BLOCK
                else (MANIFEST_FETCHING_TIMEOUT * 2 * (block_number % 2)),
            )
            for uid, hotkey in MINER_HOTKEYS.items()
            if (hotkey.startswith("timing_out_miner_"))
        ],
        *[
            (
                hotkey,
                {ec: (ind + uid) % (EXECUTOR_CAP[ec]) for ind, ec in enumerate(EXECUTOR_CLASSES)}
                if (block_number < START_CHANGING_MANIFESTS_BLOCK or not (block_number % 2))
                else None,
                0,
            )
            for uid, hotkey in MINER_HOTKEYS.items()
            if (hotkey.startswith("malforming_miner_"))
        ],
    ]


def _make_neuron(uid, key, stake, is_miner: bool, is_shielded) -> turbobt.Neuron:
    return turbobt.Neuron(
        subnet=mock.MagicMock(),
        uid=uid,
        coldkey=key,
        hotkey=key,
        active=True,
        axon_info=turbobt.neuron.AxonInfo(
            ip="0.0.0.0"
            if not is_miner
            else (f"192.168.1.{uid}" if not is_shielded else f"http://{key}.com"),
            port=8000 + uid if is_miner else 0,
            protocol=turbobt.neuron.AxonProtocolEnum.HTTP,
        ),
        prometheus_info=mock.MagicMock(),
        stake=stake,
        rank=0,
        emission=0,
        incentive=0,
        consensus=0,
        trust=0,
        validator_trust=0,
        dividends=0,
        last_update=0,
        validator_permit=False,
        pruning_score=0,
    )


def stake(block_number: int, ind: int, hotkey: str) -> float:
    base_stake = 1001 * ((ind + 1) ** 2)

    if block_number < START_CHANGING_STAKE_BLOCK:
        return float(base_stake)

    blocks_elapsed = block_number - START_CHANGING_STAKE_BLOCK

    growth_multiplier = 1 + (blocks_elapsed * (0.001 + ind * 0.0002))

    stake = base_stake * growth_multiplier

    if hotkey.startswith("stake_loosing_validator_"):
        if not blocks_elapsed % 5:
            return 500.0
        else:
            return float(stake)

    return float(stake)


def list_validators(block_number: int, filter_=True) -> list[turbobt.Neuron]:
    assert block_number >= START_BLOCK
    return list(
        filter(
            (lambda n: n.stake >= 1000) if filter_ else lambda n: True,
            [
                _make_neuron(
                    ind + NUM_MINERS,
                    hotkey,
                    stake=stake(block_number, ind, hotkey),
                    is_miner=False,
                    is_shielded=False,
                )
                for ind, hotkey in VALIDATOR_HOTKEYS.items()
                if (
                    block_number < START_CHANGING_STAKE_BLOCK
                    or not hotkey.startswith("deregging_validator_")
                    or block_number % 6
                )
            ],
        )
    )


def list_neurons(block_number: int, with_shield: bool) -> list[turbobt.Neuron]:
    assert block_number >= START_BLOCK
    return [
        *[
            _make_neuron(
                ind,
                hotkey,
                stake=float(ind),
                is_miner=True,
                is_shielded=bool(ind % 2) if with_shield else False,
            )
            for ind, hotkey in MINER_HOTKEYS.items()
            if (
                block_number < START_CHANGING_MANIFESTS_BLOCK
                or not hotkey.startswith("deregging_miner_")
                or block_number % 5
            )
        ],
        *list_validators(block_number, filter_=False),
    ]


def get_block_timestamp(block_number):
    base_time = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    return base_time + datetime.timedelta(
        seconds=(block_number - START_BLOCK) * 12 + (0.01 if not (block_number % 5) else 0)
    )


@contextmanager
def set_block_number(block_number_):
    class MockSuperTensor(supertensor.BaseSuperTensor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.block_number = block_number_

        def get_current_block(self):
            return self.block_number

        def get_shielded_neurons(self):
            return list_neurons(block_number_, with_shield=False)

        def list_neurons(self, block_number):
            return list_neurons(block_number, with_shield=False)

        def list_validators(self, block_number):
            return list_validators(block_number, filter_=True)

        def get_block_timestamp(self, block_number):
            return get_block_timestamp(block_number)

        def wallet(self):
            return get_test_validator_wallet()

    with patch.object(supertensor, "_supertensor_instance", MockSuperTensor()):
        yield


@contextmanager
def mock_manifest_endpoints_for_block_number(block_number_):
    # Create miner configs from the manifest response and the corresponding IP/port combination
    def create_miner_config():
        miners = {
            miner.hotkey: (miner.axon_info.ip, miner.axon_info.port)
            for miner in list_neurons(block_number_, with_shield=False)
        }
        responses = manifest_responses(block_number_)
        configs = []
        for _hotkey, _manifest, _delay in responses:
            # Some miners are designed to be missing for certain block numbers
            try:
                ip, port = miners[_hotkey]
            except KeyError:
                continue

            configs.append(
                {
                    "hotkey": _hotkey,
                    "address": ip,
                    "port": port,
                    "manifest": _manifest,
                    "wait_before": _delay,
                }
            )
        return configs

    with mock_manifest_endpoints(create_miner_config()):
        yield
