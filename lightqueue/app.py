"""
Defines the LightQueue application and the @app.task decorator.
"""

import functools
import time
from typing import Any, AnyStr, Callable, Dict, Optional
from lightqueue.models import TaskMessage
from lightqueue.registry import registry

class TaskWrapper:
    """
    Wraps a user function to allow both direct execution:
        result = my_task(1,2)
    and asynchronous enqueuing:
        await my_task.delay(1,2)
    """

    def __init__(
        self,
        func: Callable,
        app: "LightQueue",
        name: str,
        max_retries: int = 3,
        timeout: float = 30.0,
    ):
        self.func = func
        self.app = app
        self.task_name = name
        self.max_retries  = max_retries
        self.timeout = timeout

        # functools.wraps copies the original function's docstring, name, and module
        functools.update_wrapper(self, func)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Calling 'my_task(1,2)' directly executes the underlying function.
        """
        return self.func(*args, **kwargs)

    async def delay(self, *args: Any, **kwargs: Any) -> TaskMessage:
        """
        Shortcut to enqueue a task with default parameters:
            await my_task(arg1, arg2)
        """
        return await self.apply_async(args=list(args), kwargs=kwargs)

    async def apply_async(
        self,
        args: Optional[list] = None,
        kwargs: Optional[dict] = None,
        countdown: float = 0.0,
        eta: Optional[float] = None,
        max_retries: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> TaskMessage:
        """
        Enqueue a task with custom options like delays (countdown/eta) or retries.
        """
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        # If a countdown (e.g countdown = 50 seconds) is given, compute the future ETA
        if countdown > 0 and eta is None:
            eta = time.time() + countdown

        task_msg = TaskMessage(
            name = self.task_name,
            args=args,
            kwargs=kwargs,
            retries=0,
            max_retries=self.max_retries if max_retries is None else max_retries,
            eta=eta,
            timeout=self.timeout if timeout is None else timeout,
        )
        if self.app.broker is not None:
            await self.app.broker.enqueue(task_msg)

        return task_msg

class LightQueue:
    """
    The main application instance managing configuration and tasks.
    """
    def __init__(self, broker=None):
        self.broker = broker

    def task(
        self,
        name: Optional[str] = None,
        max_retries: int=3,
        timeout: float=30.0,
    ) -> Callable:
        """
        Decorator to register a function as a LightQueue task.

        Usage:
            @app.task(max_retries=5, timeout=10.0)
            def compute(x,y):
            return x + y
        """
        def decorator(func: Callable) -> TaskWrapper:
            # Generate a qualified name if none was explicitly provided
            # e,g, "my_module.my_func"
            task_name = name or f"{func.__module__}.{func.__name__}"

            # Register the underlying function in the global registry
            registry.register(task_name, func)

            # Return TaskWrapper object
            return TaskWrapper(
                func=func,
                app=self,
                name=task_name,
                max_retries=max_retries,
                timeout=timeout,
            )
        return decorator
