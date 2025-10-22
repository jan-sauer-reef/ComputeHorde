class FacilitatorClient:
    """
    Handles job requests and cheated job reports sent from the facilitator.
    """

    def __init__(self) -> None:
        ...

    def _job_request_handler(self) -> None:
        """
        Listens for messages on the local job requests channel and forwards these to the job dispatcher.
        """
        ...

    def _cheated_job_report_handler(self) -> None:
        """
        Listens for messages on the local cheated job reports channel and processes them.
        """
        ...

    def is_running(self) -> bool:
        ...

    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...