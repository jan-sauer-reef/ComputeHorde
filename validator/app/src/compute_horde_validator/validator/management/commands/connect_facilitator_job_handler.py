import logging

import asyncio
import signal
from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.organic_jobs.facilitator_client import FacilitatorClient
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    FACILITATOR_CLIENT_CLASS = FacilitatorClient
    STOP_EVENT = asyncio.Event()

    def __init__(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self.shutdown)

    @async_to_sync
    async def handle(self, *args, **options):
        logger.info(f"Starting facilitator client job handler")

        facilitator_client = self.FACILITATOR_CLIENT_CLASS()
        
        async def lifecycle():
            self.STOP_EVENT.clear()
            await facilitator_client.start()
            await self.STOP_EVENT.wait()
            await facilitator_client.stop()
        
        task = asyncio.create_task(lifecycle())
        await task

    def shutdown(self, *args):
        """Set global stop event to trigger the shutdown of the components."""
        if not self.STOP_EVENT.is_set():
            self.STOP_EVENT.set()


        
