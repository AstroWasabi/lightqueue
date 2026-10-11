"""
Abstract base class defining the contract for all broker backends.
"""

from abc import ABC, abstractmethod
from typing import Optional
from lightqueue.models import TaskMessage

class BaseBroker(ABC):
    """
    Contract that all broker implementations (InMemory, Redis, etc.) must fulfill.
    """

    @abstractmethod
    async def connect(self) -> None:
        """Initialize connections, pools, or internal data structures."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """close connection cleanly"""
        pass

    @abstractmethod
    async def enqueue(self, task: TaskMessage) -> None:
        """Push a task to the broker."""
        pass

    @abstractmethod
    async def dequeue(self, timeout: float = 1.0) -> Optional[TaskMessage]:
        """
        Wait up to 'timeout' seconds to retreive the next ready task.
        Returns None if no task is available within timeout.
        """
        pass

    @abstractmethod
    async def ack(self, task: TaskMessage) -> None:
        """Acknowledge successful completion and remove from in-flight tracking."""
        pass

    @abstractmethod
    async def push_dlq(self, task: TaskMessage, error: str) -> None:
        """Move a permanently failed task to the Dead_Letter Queue."""
        pass

    @abstractmethod
    async def poll_scheduled(self) -> int:
        """
        Check scheduled/delayed tasks and move any whose ETA has arrived
        into ready queue. Returns the number of tasks promoted.
        """
        pass

