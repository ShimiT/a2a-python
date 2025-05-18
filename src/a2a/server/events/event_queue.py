import asyncio
import logging

from typing import Any

from a2a.types import (
    A2AError,
    JSONRPCError,
    Message,
    Task,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
)
from a2a.utils.telemetry import SpanKind, trace_class


logger = logging.getLogger(__name__)


Event = (
    Message
    | Task
    | TaskStatusUpdateEvent
    | TaskArtifactUpdateEvent
    | A2AError
    | JSONRPCError
)


@trace_class(kind=SpanKind.SERVER)
class EventQueue:
    """Event queue for A2A responses from agent."""

    def __init__(self) -> None:
        self.queue: asyncio.Queue[Event] = asyncio.Queue()
        self._children: list[EventQueue] = []
        self._closing: bool = False
        logger.debug('EventQueue initialized.')

    def enqueue_event(self, event: Event):
        logger.debug(f'Enqueuing event of type: {type(event)}')
        if self._closing:
            logger.warning(f"Attempt to enqueue event on a closing queue: {type(event)}. Ignoring.")
            return
        self.queue.put_nowait(event)
        for child in self._children:
            child.enqueue_event(event)

    async def dequeue_event(self, no_wait: bool = False) -> Event:
        if no_wait:
            logger.debug('Attempting to dequeue event (no_wait=True).')
            event = self.queue.get_nowait()
            logger.debug(
                f'Dequeued event (no_wait=True) of type: {type(event)}'
            )
            return event

        logger.debug('Attempting to dequeue event (waiting).')
        event = await self.queue.get()
        logger.debug(f'Dequeued event (waited) of type: {type(event)}')
        return event

    def task_done(self) -> None:
        logger.debug('Marking task as done in EventQueue.')
        self.queue.task_done()

    def tap(self) -> Any:
        """Taps the event queue to branch the future events."""
        queue = EventQueue()
        self._children.append(queue)
        return queue

    async def close(self):
        """
        Closes the queue for future enqueue events, puts a sentinel for dequeue,
        and recursively closes children.
        """
        if self._closing:
            return
        self._closing = True
        logger.debug(f'Closing EventQueue {id(self)}: putting sentinel.')

        try:
            # Put a sentinel value to signal consumers that no more items will come.
            await self.queue.put(_SENTINEL)
        except Exception as e:
            # This should ideally not happen with an unbounded queue or if close is called gracefully.
            logger.error(f"Error putting sentinel in queue during close: {e}")

        for child in self._children:
            await child.close()
