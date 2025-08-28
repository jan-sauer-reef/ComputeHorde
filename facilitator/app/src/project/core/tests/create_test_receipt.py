import uuid
from datetime import UTC, datetime

from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.receipts import Receipt
from compute_horde.receipts.schemas import JobFinishedReceiptPayload, JobStartedReceiptPayload
from compute_horde.test_wallet import get_test_miner_wallet, get_test_validator_wallet

miner_hotkey = get_test_miner_wallet().get_hotkey()
validator_hotkey = get_test_validator_wallet().get_hotkey()

# JobStartedReceipt

started_payload = JobStartedReceiptPayload(
    job_uuid=str(uuid.uuid4()),
    miner_hotkey=miner_hotkey.ss58_address,
    validator_hotkey=validator_hotkey.ss58_address,
    timestamp=datetime.now(tz=UTC),
    executor_class=DEFAULT_EXECUTOR_CLASS,
    is_organic=True,
    ttl=10,
)

started_payload_blob = started_payload.blob_for_signing()
started_miner_signature = f"0x{miner_hotkey.sign(started_payload_blob).hex()}"
started_validator_signature = f"0x{validator_hotkey.sign(started_payload_blob).hex()}"

started_receipt = Receipt(
    payload=started_payload,
    miner_signature=started_miner_signature,
    validator_signature=started_validator_signature,
)
started_receipt_json = started_receipt.model_dump_json()
print(started_receipt_json)
print()

# JobFinishedReceipt

finished_payload = JobFinishedReceiptPayload(
    job_uuid=str(uuid.uuid4()),
    miner_hotkey=miner_hotkey.ss58_address,
    validator_hotkey=validator_hotkey.ss58_address,
    timestamp=datetime.now(tz=UTC),
    time_started=datetime.now(tz=UTC),
    time_took_us=30_000_000,
    score_str="0.1234",
)

finished_payload_blob = finished_payload.blob_for_signing()
finished_miner_signature = f"0x{miner_hotkey.sign(finished_payload_blob).hex()}"
finished_validator_signature = f"0x{validator_hotkey.sign(finished_payload_blob).hex()}"

finished_receipt = Receipt(
    payload=finished_payload,
    miner_signature=finished_miner_signature,
    validator_signature=finished_validator_signature,
)
finished_receipt_json = finished_receipt.model_dump_json()
print(finished_receipt_json)
