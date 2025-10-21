class HeartbeatManager:
    """
    Periodically sends heartbeat messages to the Django default channel layer.
    """
    HEARTBEAT_INTERVAL = 60.0

    def __init__(self) -> None:
        ...
    
    def set_heartbeat_callback(self, callback: Callable[[], None]) -> None:
        """Set callback for heartbeat messages"""
        self._on_heartbeat = callback
    