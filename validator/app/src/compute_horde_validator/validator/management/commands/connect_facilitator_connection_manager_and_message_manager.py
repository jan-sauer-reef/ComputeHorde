import logging

import asyncio
import signal
from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.organic_jobs.facilitator_client import (
    MessageManager,
    ConnectionManager,
)
from compute_horde.transport import WSTransport
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    MESSAGE_MANAGER_CLASS = MessageManager
    CONNECTION_MANAGER_CLASS = ConnectionManager
    TRANSPORT_LAYER_CLASS = WSTransport
    STOP_EVENT = asyncio.Event()

    def __init__(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self.shutdown)

    @async_to_sync
    async def handle(self, *args, **options):
        keypair = settings.BITTENSOR_WALLET().get_hotkey()
        logger.info(
            f"Connecting to facilitator at {settings.FACILITATOR_URI} and "
            "starting connection manager and message manager"
        )

        transport_layer = self.TRANSPORT_LAYER_CLASS(name="facilitator", url=settings.FACILITATOR_URI)
        connection_manager = self.CONNECTION_MANAGER_CLASS(keypair=keypair, transport_layer=transport_layer)
        message_manager = self.MESSAGE_MANAGER_CLASS(connection_manager=connection_manager)
        
        async def lifecycle():
            self.STOP_EVENT.clear()
            await connection_manager.start()
            await message_manager.start()
            await self.STOP_EVENT.wait()
            await message_manager.stop()
            await connection_manager.stop()
        
        task = asyncio.create_task(lifecycle())
        await task

    def shutdown(self, *args):
        """Set global stop event to trigger the shutdown of the components."""
        if not self.STOP_EVENT.is_set():
            self.STOP_EVENT.set()


        
