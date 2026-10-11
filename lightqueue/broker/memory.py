"""
Zero-dependency, in-memory async broker using asyncio.Queue and in-flight tracking.
"""

import asyncio
import heapq
import time
from typing import Dict, List, Optional, Tuple
from lightqueue.broker.base import BaseBroker
from lightqueue.models import TaskMessage

class InMemoryBroker(BaseBroker):
    """
    An in-memory broker ideal for unit tests and local development.
    """

    def __init__(self):
        # Ready queue: holds tasks ready for immediate execution
        self._ready_queue: asyncio.Queue[TaskMessage] = asyncio.Queue()

        # Min-Heap priority queue for delayed tasks:
        # Stores tuples: (eta_timestamp, task_id, task_message)
        self.scheduled_heap: List[Tuple[float, str, TaskMessage]] = []

        # In-flight storage: {task_id: TaskMessage}
        self.in_flight: Dict[str, TaskMessage] = {}

        # Dead-Letter Queue: list of all dicts recording failed tasks and errors
        self.dlq: List[Dict[str, str]] = []

        # Lock to protect shared dictionary modifications
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        # In-memory broker is already ready; no network socket needed
        pass

    async def disconnect(self) -> None:
        pass

    async def enqueue(self, task: TaskMessage) -> None:
        """
        If task has future ETA, push it into the min-heap.
        Otherwise, push it directly to the ready queue.
        """
        now = time.time()
        async with self._lock:
            if task.eta is not None and task.eta > now:

                # heapq sorts by the first element of the tuple (eta)
                # We include task.id as tiebreaker so it never tries to compare TaskMessage objects
                heapq.heappush(self.scheduled_heap, (task.eta, task.id, task))
            
            else:
                await self._ready_queue.put(task)

    async def poll_scheduled(self) -> int:
        """
        Checks the min-heap and moves all due tasks into ready queue.
        Returns the count of tasks promoted.
        """

        now = time.time()
        promoted = 0

        async with self._lock:
            # While the heap is not empty and the earliest task is due
            while self.scheduled_heap and self.scheduled_heap[0][0] <= now:
                _,_, task = heapq.heappop(self.scheduled_heap)
                await self._ready_queue.put(task)
                promoted +=1
            return promoted

    async def dequeue(self, timeout: float = 1.0) -> Optional[TaskMessage]:
        """
        Wait upto 'timeout' seconds for a task from ready queue.
        if a task is retreived, place it in 'in_flight' before returning.
        """
        try:
            # asyncio.wait_for cancels the queue.get() if timeout seconds elapse
            task = await asyncio.wait_for(self._ready_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
    
        # Mark task as in-flight
        async with self._lock:
            self.in_flight[task.id] = task

        return task

    async def ack(self, task: TaskMessage) -> None:
        """Acknowledge task completion and remove from in-flight storage."""
        async with self._lock:
            self.in_flight.pop(task.id, None)

    async def push_dlq(self, task: TaskMessage, error: str) -> None:
        """Move task to the Dead-Letter Queue and remove from in-flight."""
        async with self._lock:
            self.in_flight.pop(task.id, None)
            self.dlq.append({
                "task_id": task.id,
                "task_name": task.name,
                "error": error,
            })
