#Defines the message envelopes for tasks and execution results.

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
import json
import time
import uuid


@dataclass
class TaskMessage:
    """
    The envelope for a single unit of background work.
    """

    # The name of the registered function to execute (e.g, 'tasks.send_email')
    name: str

    # Positional arguments passed to the function e.g [user_id, "welcome"]
    args: List[Any] = field(default_factory=list)

    # Keyword arguments passed to the function: e.g. {"urgent": True}
    kwargs: Dict[str, Any] = field(default_factory=dict)

    # Unique identifier for tracking, logging, and idempotency
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Number of times this task has already failed and been retried
    retries: int = 0

    # Maximum number of times to retry before moving this task to the Dead-Letter Queue
    max_retries: int = 3

    # Estimated Time of Arrival: epoch timestamp in seconds for scheduled/delayed tasks.
    # If None, the task should run as soon as possible.
    eta: Optional[float] = None

    # Hard timeout in seconds: if the task runs longer than this, kill it.
    timeout: float = 30.0

    # Timestamp of when this task was created
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str,Any]:
        """Convert the dataclass into a standard python dictionary."""
        return asdict(self)

    
    def to_json(self) -> str:
        """Serialize the message into a JSON string for network or broker transport."""
        return json.dumps(self.to_dict())

    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskMessage":
        """Reconstruct a TaskMessage from a dictionary."""
        return cls(**data)


    @classmethod
    def from_json(cls, json_str: str) -> "TaskMessage":
        """Parse a JSON string back into a TaskMessage instance."""
        data = json.loads(json_str)
        return cls.from_dict(data)


@dataclass
class TaskResult:
    """
    The envelope returned by a worker after executing a task.
    """

    # Matches the id of the TaskMessage that was executed.
    task_id: str

    # True if the function returned normally, False if it raised an exception or timed out.
    success: bool

    # The return value of the function (if successful)
    result: Optional[Any] = None

    # Error message if the function failed.
    error: Optional[str] = None

    # Full stack trace string to help debugging failed jobs 
    traceback: Optional[str] = None

    # How many seconds the tasks took to execute
    duration: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskResult":
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str) -> "TaskResult":
        return cls.from_dict(json.loads(json_str))

