"""                                                                                                                                                                                                
lightqueue/worker/coordinator.py                                                                                                                                                                   
                                                                                                                                                                                                    
Asynchronous coordinator managing broker polling, concurrency limits via Semaphore,                                                                                                                
task dispatching, exponential backoff retries, DLQ routing, and graceful shutdown.                                                                                                                 
"""                                                                                                                                                                                                
                                                                                                                                                                                                    
import asyncio                                                                                                                                                                                     
import logging                                                                                                                                                                                     
import signal                                                                                                                                                                                      
import sys                                                                                                                                                                                         
import time                                                                                                                                                                                        
from typing import Optional, Set                                                                                                                                                                   
from lightqueue.broker.base import BaseBroker                                                                                                                                                      
from lightqueue.models import TaskMessage, TaskResult                                                                                                                                              
from lightqueue.utils.backoff import calculate_backoff                                                                                                                                             
from lightqueue.worker.executor import TaskExecutor                                                                                                                                                
                                                                                                                                                                                                    
logger = logging.getLogger(__name__)                                                                                                                                                               
                                                                                                                                                                                                    
                                                                                                                                                                                                    
class WorkerCoordinator:                                                                                                                                                                           
    """                                                                                                                                                                                            
    Coordinates polling from the broker, bounded execution via a semaphore,                                                                                                                        
    exponential retries, DLQ routing, and graceful signal-handling shutdown.                                                                                                                       
    """                                                                                                                                                                                            
                                                                                                                                                                                                    
    def __init__(                                                                                                                                                                                  
        self,                                                                                                                                                                                      
        broker: BaseBroker,                                                                                                                                                                        
        executor: Optional[TaskExecutor] = None,                                                                                                                                                   
        concurrency: int = 4,                                                                                                                                                                      
        poll_interval: float = 0.1,                                                                                                                                                                
        graceful_timeout: float = 10.0,                                                                                                                                                            
    ):                                                                                                                                                                                             
        self.broker = broker                                                                                                                                                                       
        self.concurrency = concurrency                                                                                                                                                             
        self.poll_interval = poll_interval                                                                                                                                                         
        self.graceful_timeout = graceful_timeout                                                                                                                                                   
                                                                                                                                                                                                    
        self.executor = executor or TaskExecutor(max_workers=concurrency)                                                                                                                          
        self.semaphore = asyncio.Semaphore(concurrency)                                                                                                                                            
        self._active_tasks: Set[asyncio.Task] = set()                                                                                                                                              
                                                                                                                                                                                                    
        self._running = False                                                                                                                                                                      
        self._is_shutting_down = False                                                                                                                                                             
        self._scheduler_task: Optional[asyncio.Task] = None                                                                                                                                        
                                                                                                                                                                                                    
    def install_signal_handlers(self) -> None:                                                                                                                                                     
        """                                                                                                                                                                                        
        Attaches listeners for SIGINT (Ctrl+C) and SIGTERM (container stop).                                                                                                                       
        (Supported on POSIX systems: macOS and Linux).                                                                                                                                             
        """                                                                                                                                                                                        
        # On Windows, add_signal_handler is not fully implemented in asyncio                                                                                                                       
        if sys.platform != "win32":                                                                                                                                                                
            loop = asyncio.get_running_loop()                                                                                                                                                      
            for sig in (signal.SIGINT, signal.SIGTERM):                                                                                                                                            
                loop.add_signal_handler(                                                                                                                                                           
                    sig,                                                                                                                                                                           
                    lambda s=sig: asyncio.create_task(self.stop(signal_name=s.name)),                                                                                                              
                )                                                                                                                                                                                  
                                                                                                                                                                                                    
    async def _scheduler_loop(self) -> None:                                                                                                                                                       
        """Background coroutine to periodically promote delayed/retrying tasks."""                                                                                                                 
        while self._running:                                                                                                                                                                       
            try:                                                                                                                                                                                   
                await self.broker.poll_scheduled()                                                                                                                                                 
            except Exception as exc:                                                                                                                                                               
                logger.error(f"Error in scheduler loop: {exc}")                                                                                                                                    
            await asyncio.sleep(self.poll_interval)                                                                                                                                                
                                                                                                                                                                                                    
    async def _on_task_failure(self, task: TaskMessage, result: TaskResult) -> None:                                                                                                               
        """                                                                                                                                                                                        
        Determines whether to retry a failed task with exponential backoff                                                                                                                         
        or route it to the Dead-Letter Queue.                                                                                                                                                      
        """                                                                                                                                                                                        
        error_msg = result.error or "Unknown error"                                                                                                                                                
                                                                                                                                                                                                    
        if task.retries < task.max_retries:                                                                                                                                                        
            delay = calculate_backoff(retries=task.retries, base=1.0, jitter=True)                                                                                                                 
            task.retries += 1                                                                                                                                                                      
            task.eta = time.time() + delay                                                                                                                                                         
                                                                                                                                                                                                    
            logger.warning(                                                                                                                                                                        
                f"Task {task.id} ({task.name}) failed: {error_msg}. "                                                                                                                              
                f"Retrying in {delay:.2f}s (attempt {task.retries}/{task.max_retries})"                                                                                                            
            )                                                                                                                                                                                      
                                                                                                                                                                                                    
            await self.broker.enqueue(task)                                                                                                                                                        
            await self.broker.ack(task)                                                                                                                                                            
        else:                                                                                                                                                                                      
            logger.error(                                                                                                                                                                          
                f"Task {task.id} ({task.name}) exceeded max retries ({task.max_retries}). "                                                                                                        
                f"Routing to DLQ: {error_msg}"                                                                                                                                                     
            )                                                                                                                                                                                      
            await self.broker.push_dlq(task, error=error_msg)                                                                                                                                      
                                                                                                                                                                                                    
    async def _handle_task(self, task: TaskMessage) -> TaskResult:                                                                                                                                 
        """                                                                                                                                                                                        
        Executes a task in the executor and manages ACK/Retry/DLQ.                                                                                                                                 
        """                                                                                                                                                                                        
        try:                                                                                                                                                                                       
            result = await self.executor.execute(task)                                                                                                                                             
                                                                                                                                                                                                    
            if result.success:                                                                                                                                                                     
                await self.broker.ack(task)                                                                                                                                                        
            else:                                                                                                                                                                                  
                await self._on_task_failure(task, result)                                                                                                                                          
                                                                                                                                                                                                    
            return result                                                                                                                                                                          
        finally:                                                                                                                                                                                   
            self.semaphore.release()                                                                                                                                                               
                                                                                                                                                                                                    
    async def process_one(self) -> Optional[TaskResult]:                                                                                                                                           
        """Fetches and processes exactly one task synchronously."""                                                                                                                                
        task = await self.broker.dequeue(timeout=self.poll_interval)                                                                                                                               
        if task is None:                                                                                                                                                                           
            return None                                                                                                                                                                            
                                                                                                                                                                                                    
        result = await self.executor.execute(task)                                                                                                                                                 
        if result.success:                                                                                                                                                                         
            await self.broker.ack(task)                                                                                                                                                            
        else:                                                                                                                                                                                      
            await self._on_task_failure(task, result)                                                                                                                                              
        return result                                                                                                                                                                              
                                                                                                                                                                                                    
    async def run(self) -> None:                                                                                                                                                                   
        """Main worker execution loop."""                                                                                                                                                          
        self._running = True                                                                                                                                                                       
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())                                                                                                                         
                                                                                                                                                                                                    
        while self._running:                                                                                                                                                                       
            await self.semaphore.acquire()                                                                                                                                                         
                                                                                                                                                                                                    
            if not self._running:                                                                                                                                                                  
                self.semaphore.release()                                                                                                                                                           
                break                                                                                                                                                                              
                                                                                                                                                                                                    
            task = await self.broker.dequeue(timeout=self.poll_interval)                                                                                                                           
            if task is None:                                                                                                                                                                       
                self.semaphore.release()                                                                                                                                                           
                await asyncio.sleep(self.poll_interval)                                                                                                                                            
                continue                                                                                                                                                                           
                                                                                                                                                                                                    
            coro = asyncio.create_task(self._handle_task(task))                                                                                                                                    
            self._active_tasks.add(coro)                                                                                                                                                           
            coro.add_done_callback(self._active_tasks.discard)                                                                                                                                     
                                                                                                                                                                                                    
    async def stop(self, signal_name: str = "MANUAL") -> None:                                                                                                                                     
        """                                                                                                                                                                                        
        Gracefully stops the worker:                                                                                                                                                               
        1. Halts pulling new tasks from the broker.                                                                                                                                                
        2. Waits up to `graceful_timeout` seconds for in-flight tasks to complete.                                                                                                                 
        3. Shuts down executor.                                                                                                                                                                    
        """                                                                                                                                                                                        
        if self._is_shutting_down:                                                                                                                                                                 
            return                                                                                                                                                                                 
        self._is_shutting_down = True                                                                                                                                                              
        self._running = False                                                                                                                                                                      
                                                                                                                                                                                                    
        logger.info(f"Received {signal_name}. Initiating graceful shutdown...")                                                                                                                    
                                                                                                                                                                                                    
        # Cancel the scheduler loop                                                                                                                                                                
        if self._scheduler_task and not self._scheduler_task.done():                                                                                                                               
            self._scheduler_task.cancel()                                                                                                                                                          
                                                                                                                                                                                                    
        # Drain in-flight tasks                                                                                                                                                                    
        if self._active_tasks:                                                                                                                                                                     
            logger.info(f"Waiting for {len(self._active_tasks)} in-flight tasks to finish...")                                                                                                     
            try:                                                                                                                                                                                   
                # Wait up to graceful_timeout for in-flight tasks                                                                                                                                  
                await asyncio.wait_for(                                                                                                                                                            
                    asyncio.gather(*self._active_tasks, return_exceptions=True),                                                                                                                   
                    timeout=self.graceful_timeout,                                                                                                                                                 
                )                                                                                                                                                                                  
            except asyncio.TimeoutError:                                                                                                                                                           
                logger.warning("Graceful shutdown timeout exceeded. Cancelling lingering tasks.")                                                                                                  
                for t in self._active_tasks:                                                                                                                                                       
                    t.cancel()                                                                                                                                                                     
                                                                                                                                                                                                    
        self.executor.shutdown(wait=True)                                                                                                                                                          
        logger.info("Worker shutdown complete.")                                                                                                                                                   
                                                                                                                                                                                                    
    async def run_until_empty(self, idle_timeout: float = 0.3) -> None:                                                                                                                            
        """Runs until all tasks and active executions are finished."""                                                                                                                             
        self._running = True                                                                                                                                                                       
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())                                                                                                                         
                                                                                                                                                                                                    
        idle_time = 0.0                                                                                                                                                                            
        while self._running:                                                                                                                                                                       
            await self.semaphore.acquire()                                                                                                                                                         
                                                                                                                                                                                                    
            task = await self.broker.dequeue(timeout=self.poll_interval)                                                                                                                           
            if task is None:                                                                                                                                                                       
                self.semaphore.release()                                                                                                                                                           
                idle_time += self.poll_interval                                                                                                                                                    
                if idle_time >= idle_timeout and len(self._active_tasks) == 0:                                                                                                                     
                    break                                                                                                                                                                          
                await asyncio.sleep(self.poll_interval)                                                                                                                                            
                continue                                                                                                                                                                           
                                                                                                                                                                                                    
            idle_time = 0.0                                                                                                                                                                        
            coro = asyncio.create_task(self._handle_task(task))                                                                                                                                    
            self._active_tasks.add(coro)                                                                                                                                                           
            coro.add_done_callback(self._active_tasks.discard)                                                                                                                                     
                                                                                                                                                                                                    
        await self.stop()                                                                                                                                                                          
                                                                                                                                                                                                    
    async def shutdown(self) -> None:                                                                                                                                                              
        """Alias for stop()."""                                                                                                                                                                    
        await self.stop()                                
