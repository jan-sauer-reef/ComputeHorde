# Redis channel names
HEARTBEAT_CHANNEL = "facilitator_connector_heartbeat"
JOB_STATUS_UPDATE_CHANNEL = "facilitator_connector_job_status_updates"
JOB_REQUEST_CHANNEL = "facilitator_connector_job_requests"
CHEATED_JOB_REPORT_CHANNEL = "facilitator_connector_cheated_job_reports"

# Various timeouts and intervals
POLL_INTERVAL = 1.0
LOCAL_MESSAGE_SEND_TIMEOUT = 10.0
TRANSPORT_LAYER_MESSAGE_SEND_TIMEOUT = 10.0