import pytest
from lightqueue.models import TaskMessage
from lightqueue.broker.memory import InMemoryBroker
from lightqueue.worker.coordinator import WorkerCoordinator
from lightqueue.registry import registry 
from lightqueue.utils.backoff import calculate_backoff

def test_backoff_formula():
    #Without jitter verify pure exponential values
    assert calculate_backoff(retries=0, base=1.0, jitter=False) == 1.0
    assert calculate_backoff(retries=1, base=1.0, jitter=False) == 2.0
    assert calculate_backoff(retries=2, base=1.0, jitter=False) == 4.0
    assert calculate_backoff(retries=3, base=1.0, jitter=False) == 8.0

    # With jitter, delay should be between base and base + 1.0
    jittered = calculate_backoff(retries=0, base=1.0, jitter=True)
    assert 1.0 <= jittered <= 2.0

def flaky_task():
    raise RuntimeError("Flaky error encountered")

registry.register("tests.test_retries_and_dlq.flaky_task", flaky_task)

@pytest.mark.asyncio
async def test_task_retries_and_routes_to_dlq():
    broker = InMemoryBroker()
    await broker.connect()

    coordinator = WorkerCoordinator(broker=broker, concurrency=1)

    # Create task with max_retries = 1                                                       
    task = TaskMessage(name="tests.test_retries_and_dlq.flaky_task", max_retries=1)          
    await broker.enqueue(task)                                                               
                                                                                                
    # 1. First execution attempt: should fail, increment retries to 1, and schedule retry    
    result1 = await coordinator.process_one()                                                
    assert result1 is not None                                                               
    assert result1.success is False                                                          
                                                                                                
    # DLQ should still be empty because it gets 1 retry                                      
    assert len(broker.dlq) == 0                                                              
    # Task should now be in the scheduled min-heap                                           
    assert len(broker.scheduled_heap) == 1                                                   
                                                                                                
    # Retrieve the scheduled task from the heap and make it ready immediately                
    _, _, scheduled_task = broker.scheduled_heap[0]                                          
    assert scheduled_task.retries == 1                                                       
    scheduled_task.eta = None  # clear delay for test                                        
    broker.scheduled_heap.clear()                                                            
    await broker.enqueue(scheduled_task)                                                     
                                                                                                
    # 2. Second execution attempt: fails again, but max_retries reached!                     
    result2 = await coordinator.process_one()                                                
    assert result2 is not None                                                               
    assert result2.success is False                                                          
                                                                                                
    # Now it should be permanently moved to DLQ                                              
    assert len(broker.dlq) == 1                                                              
    assert broker.dlq[0]["task_id"] == task.id                                               
    assert "Flaky error encountered" in broker.dlq[0]["error"]                              
    assert len(broker.in_flight) == 0                                                        
                                                                                                
    await coordinator.shutdown() 





