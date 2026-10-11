"""
Maintains a mapping of string task names to executable Python callables.
"""

from typing import Callable, Dict, Optional

class TaskRegistry:
    """
    A registry storing function names mapped to their callable objects.
    """

    def __init__(self):
        self._tasks: Dict[str, Callable] = {}

    def register(self, name: str, func: Callable) -> None:
        """Register a function under a unique name string."""
        if name is self._tasks:
            # Overwriting is allowed (e.g. during testing or hot reloading)
            pass
        self._tasks[name] = func

    def get(self, name: str) -> Optional[Callable]:
        """Retreive a callable by its registered name."""
        return self._tasks.get(name)

    def has(self, name: str) -> bool:
        """Check if a task name is regestered."""
        return name in self._tasks

    def clear(self) -> None:
        """clear all registered tasks (useful for resetting between tests)."""

registry = TaskRegistry()
