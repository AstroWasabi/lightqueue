"""
Executes task callables in worker subprocesses using ProcessPoolExecutor,
enforcing timeouts and capturing exceptions and tracebacks safely.
Supports both synchronous CPU-bound tasks (in ProcessPool) and async I/O tasks.
"""

import asyncio
from concurrent.futures import ProcessPoolExecutor
import inspect
import importlib
import time
import traceback
from typing import Any, Optional
from lightqueue.models import TaskMessage, TaskResult
from lightqueue.registry import registry


def _worker_entrypoint(task_dict: dict) -> dict:
    """
    Top-level helper function executed inside the worker child process.
    Must be picklable across process boundaries.
    """
    start_time = time.time()
    task = TaskMessage.from_dict(task_dict)

    func = registry.get(task.name)

    if func is None and "." in task.name:
        module_path, func_name = task.name.rsplit(".", 1)
        try:
            mod = importlib.import_module(module_path)
            func = getattr(mod, func_name, None)
        except Exception:
            func = None

    if func is None:
        duration = time.time() - start_time
        return TaskResult(
            task_id=task.id,
            success=False,
            error=f"Task '{task.name}' is not registered in the worker process.",
            duration=duration,
        ).to_dict()

    try:
        # Execute the user function
        result_value = func(*task.args, **task.kwargs)
        duration = time.time() - start_time
        return TaskResult(
            task_id=task.id,
            success=True,
            result=result_value,
            duration=duration,
        ).to_dict()
    except Exception as exc:
        duration = time.time() - start_time
        tb_str = traceback.format_exc()
        return TaskResult(
            task_id=task.id,
            success=False,
            error=f"{type(exc).__name__}: {str(exc)}",
            traceback=tb_str,
            duration=duration,
        ).to_dict()


class TaskExecutor:
    """
    Dispatches tasks to a ProcessPoolExecutor (for CPU tasks) or executes
    coroutine tasks directly on the async event loop (for async I/O tasks).
    """

    def __init__(self, max_workers: Optional[int] = None):
        self.max_workers = max_workers
        self._pool = ProcessPoolExecutor(max_workers=max_workers)

    async def execute(self, task: TaskMessage) -> TaskResult:
        """
        Runs the task with timeout enforcement.
        """
        loop = asyncio.get_running_loop()
        start_time = time.time()

        # Check if the task is an async coroutine registered in the coordinator loop
        func = registry.get(task.name)
        if func is not None and inspect.iscoroutinefunction(func):
            try:
                result_val = await asyncio.wait_for(
                    func(*task.args, **task.kwargs),
                    timeout=task.timeout,
                )
                duration = time.time() - start_time
                return TaskResult(
                    task_id=task.id,
                    success=True,
                    result=result_val,
                    duration=duration,
                )
            except asyncio.TimeoutError:
                duration = time.time() - start_time
                return TaskResult(
                    task_id=task.id,
                    success=False,
                    error=f"Task exceeded hard timeout of {task.timeout}s",
                    duration=duration,
                )
            except Exception as exc:
                duration = time.time() - start_time
                return TaskResult(
                    task_id=task.id,
                    success=False,
                    error=f"{type(exc).__name__}: {str(exc)}",
                    traceback=traceback.format_exc(),
                    duration=duration,
                )

        # Synchronous CPU tasks are dispatched to ProcessPoolExecutor
        try:
            future = loop.run_in_executor(
                self._pool,
                _worker_entrypoint,
                task.to_dict(),
            )

            result_dict = await asyncio.wait_for(future, timeout=task.timeout)
            return TaskResult.from_dict(result_dict)

        except asyncio.TimeoutError:
            duration = time.time() - start_time
            return TaskResult(
                task_id=task.id,
                success=False,
                error=f"Task exceeded hard timeout of {task.timeout}s",
                duration=duration,
            )
        except Exception as exc:
            duration = time.time() - start_time
            return TaskResult(
                task_id=task.id,
                success=False,
                error=f"Execution failed: {str(exc)}",
                traceback=traceback.format_exc(),
                duration=duration,
            )

    def shutdown(self, wait: bool = True) -> None:
        """Cleanly shutdown the process pool."""
        self._pool.shutdown(wait=wait, cancel_futures=True)
