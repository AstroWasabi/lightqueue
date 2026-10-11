import pytest
import time
from lightqueue.models import TaskMessage
from lightqueue.worker.executor import TaskExecutor
from lightqueue.registry import registry 

# Sample task functions defined at top level for pickle support                              
def slow_task(seconds: float):                                                               
    time.sleep(seconds)                                                                      
    return "done"                                                                            
                                                                                                 
                                                                                                 
def failing_task():                                                                          
    raise ValueError("Something went wrong")     


# Register at module level so any spawned worker child process knows about them
registry.register("tests.test_executor.slow_task", slow_task)
registry.register("tests.test_executor.failing_task", failing_task)
                                                                                                 
@pytest.mark.asyncio                                                                         
async def test_executor_success_and_error():                                                 
    registry.register("tests.test_executor.slow_task", slow_task)                            
    registry.register("tests.test_executor.failing_task", failing_task)                      
                                                                                                 
    executor = TaskExecutor(max_workers=2)                                                   
                                                                                                 
    try:                                                                                     
        # 1. Success execution                                                               
        task1 = TaskMessage(name="tests.test_executor.slow_task", args=[0.05], timeout=1.0)  
        result1 = await executor.execute(task1)                                              
        assert result1.success is True                                                       
        assert result1.result == "done"                                                      
        assert result1.error is None                                                         
        assert result1.duration >= 0.04                                                      
                                                                                                 
        # 2. Exception execution                                                             
        task2 = TaskMessage(name="tests.test_executor.failing_task", timeout=1.0)            
        result2 = await executor.execute(task2)                                              
        assert result2.success is False                                                      
        assert "ValueError: Something went wrong" in (result2.error or "")                   
        assert result2.traceback is not None                                                 
    finally:                                                                                 
        executor.shutdown(wait=True)                                                         
                                                                                                 
                                                                                                 
@pytest.mark.asyncio                                                                         
async def test_executor_timeout():                                                           
    registry.register("tests.test_executor.slow_task", slow_task)                            
    executor = TaskExecutor(max_workers=2)                                                   
                                                                                                 
    try:                                                                                     
        # Task runs for 0.5s, but timeout is set to 0.1s!                                    
        task = TaskMessage(name="tests.test_executor.slow_task", args=[0.5], timeout=0.1)    
        result = await executor.execute(task)                                                
                                                                                                 
        assert result.success is False                                                       
        assert "exceeded hard timeout" in (result.error or "")                               
    finally:                                                                                 
        executor.shutdown(wait=False)
