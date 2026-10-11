                                                                                                                                                                                                
"""                                                                                                                                                                                                
tests/test_graceful_shutdown.py                                                                                                                                                                    
"""                                                                                                                                                                                                
                                                                                                                                                                                                    
import pytest                                                                                                                                                                                      
import asyncio                                                                                                                                                                                     
import time                                                                                                                                                                                        
from lightqueue.models import TaskMessage                                                                                                                                                          
from lightqueue.broker.memory import InMemoryBroker                                                                                                                                                
from lightqueue.worker.coordinator import WorkerCoordinator                                                                                                                                        
from lightqueue.registry import registry                                                                                                                                                           
                                                                                                                                                                                                    
                                                                                                                                                                                                    
def slow_job(duration: float):                                                                                                                                                                     
    time.sleep(duration)                                                                                                                                                                           
    return "done"                                                                                                                                                                                  
                                                                                                                                                                                                    
                                                                                                                                                                                                    
registry.register("tests.test_graceful_shutdown.slow_job", slow_job)                                                                                                                               
                                                                                                                                                                                                    
                                                                                                                                                                                                    
@pytest.mark.asyncio                                                                                                                                                                               
async def test_graceful_drain_on_stop():                                                                                                                                                           
    broker = InMemoryBroker()                                                                                                                                                                      
    await broker.connect()                                                                                                                                                                         
                                                                                                                                                                                                    
    coordinator = WorkerCoordinator(broker=broker, concurrency=2, graceful_timeout=2.0)                                                                                                            
                                                                                                                                                                                                    
    # Enqueue a task that takes 0.2 seconds                                                                                                                                                        
    task = TaskMessage(name="tests.test_graceful_shutdown.slow_job", args=[0.2])                                                                                                                   
    await broker.enqueue(task)                                                                                                                                                                     
                                                                                                                                                                                                    
    # Start the worker running in the background                                                                                                                                                   
    worker_task = asyncio.create_task(coordinator.run())                                                                                                                                           
                                                                                                                                                                                                    
    # Wait briefly for worker to pick up task and mark it in-flight                                                                                                                                
    await asyncio.sleep(0.05)                                                                                                                                                                      
    assert len(broker.in_flight) == 1                                                                                                                                                              
                                                                                                                                                                                                    
    # Trigger graceful shutdown while the task is still executing!                                                                                                                                 
    await coordinator.stop(signal_name="SIGTERM")                                                                                                                                                  
    await worker_task                                                                                                                                                                              
                                                                                                                                                                                                    
    # The in-flight task was allowed to finish cleanly and got ACKed!                                                                                                                              
    assert len(broker.in_flight) == 0