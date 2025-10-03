# FacilitatorClient redesign

## Problem statement

The facilitator connector is in a sorry state:
- No transport abstraction - Refactor FacilitatorClient to use an abstract Transport instead of straight Webstocket
- Randomly disconnects, but the errors do not indicate the issue
- Convoluted connect / send message retry logic, effectively nested retries
- Doesn’t handle asyncio cancellation well, gets stuck on SIGINTs
- Doesn’t emit any metrics or system events
- Is a mess of concurrent asyncio tasks
- Undocumented and surprising (e.g. requires an “async with” context before entering the loop, but doesn’t document or check for it)
- Makes no effort to preserve the order of queued messages after reconnecting to faci (important for status updates)

## Redesign overview

To address these issues and improve maintainability, this design aims to make the FacilitatorClient more modular. The following core components are part of the new redesign:

- **TransportLayer**: an abtraction of the websocket connection (or any other connection method). The existing `compute_horde.transport.ws.WSTransport` can be used for this with minor upgrades (e.g. adding a `is_connected()` method for easier management)
- **ConnectionManager**: handles connection tracking and reconnection logic.
- **MessageQueue**: a robust queue that ensures message order remains consistent despite disconnects/reconnects to the facilitator. Handles sending messages to the facilitator and ensures that messages are only sent if an active connection exists.
- **JobStatusManager**: listens for updates for a given job UUID and adds them as messages to the message queue.
- **HeartbeatManager**: sends periodic heartbeats along a transport layer.
- **PerformanceMonitor**: collects metrics related to operation, e.g. number of messages sent, disconnects and reconnects to the facilitator, etc., and emits SystemEvents.
- **JobDispatcher**: a simple utility class that dispatches jobs into a message queue, where they can be picked up by the proposed Routing/Jobs modules of the redesign.

## Migration strategy

To ensure compatibility with existing code, the migration can be split into two distinct phases

### Phase 1: Internal-only components
This phase impacts only internal components as well as usage of the `FacilitatorClient` itself and requires only minimal changes to the code base.
- Implement the `ConnectionManager`, `MessageQueue`, `JobStatusManager`, `HeartbeatManager`, and `PerformanceMonitor` classes and refactor the `FacilitatorClient` to use these as well as the existing `compute_horde.transport.ws.WSTransport` class.
- Implement an interim version of the `JobDispatcher` that still uses the current logic, i.e. the `FacilitatorClient` determines the job route itself and submits the job to the miner directly (see `FacilitatorClient._process_job_request`)

### Phase 2: Upgrade JobDispatcher
Once the new Routing/Jobs components exist, the JobDispatcher class can be updated to have the intended functionality, i.e. submit tasks to a message queue and not be concerned with the actual logic of routing and submitting jobs

## How current issues are addressed

- Uses transport abstraction that decouples it from the websocket logic itself.
- Connection and message sending logic is cleanly separated into separate classes. The MessageQueue can be instructed to wait until an active connection exists, ensuring that send-message retries are only triggered if the message actually fails to send, and not if the connection is closed, as is currently the case.
- asyncio cancellation will be explicitly handled by all components by ensuring that they cease operation if an asyncio.CancelledError is raised.
- Metrics and system events will be captured by the PerformanceMonitor, hopefully enabling more details on why connections fail and avoiding the current issue of disconnects occurring without known issue.
- Each component manages its own lifecycle, cleaning up the current asyncio task spaghetti. Furthermore, the job dispatcher will avoid the dynamic generation of asyncio tasks as it simply pushes messages into a queue for other components to handle.
- The `FacilitatorClient` entrypoint is cleaned up to eliminate the need for the current convoluted usage (`async with` and then `await facilitator_client.run_forever()` inside the context). Instead, `await client.start()` will set up the client and start the main operating loop while the teardown will be handled automatically via `try-except-finally` logic in the main loop.
- Message order is preserved via the `MessageQueue` class.

## Class skeletons and pseudo-code

This section should be considered as an example and the pseudo-code and skeletons here may very well be changed as issues are identified during implementation.

### TransportLayer

See `compute_horde.transport.ws.WSTransport`

### ConnectionManager

```Python
class ConnectionManager:
    def start():
        """Sets up the connection management and starts the main loop"""
    def _main_loop():
        """
        Main "run forever" loop that checks that the connection is still active
        and triggers reconnection attempts if not. Emits SystemEvents whenever
        a disconnect is detected
        """
    def stop():
        """Ends connection management and disconnects the transport layer"""
```

### MessageQueue

```Python
class MessageQueue:
    def enqueue_message():
        """Adds message to the end of the queue"""
    def get_next_message():
        """Gets the oldest message in the queue (FIFO principle)"""
    def retry_message():
        """
        Inserts message into the front of the queue again to retry sending it.
        Keeps track of how often a message was retried for logging and 
        optionally emits a SystemEvent (or exception?) if a message cannot be
        sent
        """
    def send_queued_messages():
        """
        Attempts to send all queued messages in their order. Will retry if a
        message fails to send
        """
```

### HeartbeatManager

```Python
class HeartbeatManager:
    def start():
        """Sets up the heartbeat management and starts the main loop"""
    def _main_loop():
        """
        Main "run forever" loop that periodically adds heartbeat message to
        the MessageQueue
        """
    def stop():
        """Ends heartbeat management and disconnects the transport layer"""
```

### PerformanceMonitor

```Python
class PerformanceMonitor:
    """
    This is really just a glorified data class that keeps track of metrics,
    like the number of messages sent. Exact details will depend on how metrics
    should be logged or propagated.
    """
```

### JobDispatcher

```Python
class JobDispatcher:
    def dispatch_job():
        """
        Submit a job to a message queue.
        
        For the interim version of the job dispatcher, this method can be 
        hijacked to utilize the old logic, i.e. the
        FacilitatorClient.process_job_request functionality. This will make
        migration to the message-dispatching system painless.
        """
```

### FacilitatorClient

```Python
class FacilitatorClient:
    def __init__():
        # Set up:
        #   transport layer
        #   connection manager
        #   message queue
        #   heartbeat manager
        #   job dispatcher
        #   performance monitor
    
    def start():
        # Start all run-forever components:
        #   connection manager
        #   heartbeat manager

        # Start the main process loop