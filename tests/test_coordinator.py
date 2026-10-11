import pytest 
import asyncio
from lightqueue.models import TaskMessage
from lightqueue.broker.memory import InMemoryBroker
from lightqueue.worker.coordinator import WorkerCoordinator
from lightqueue.registry import registry

#Deine Top-Level task function for multiprocessing support
def add_numbers(a: int, b: int) -> int:
    return a + b

registry.register("tests.test_coordinator.add_numbers", add_numbers)

@pytest.mark.asyncio
async def test_worker_coordinator_lifecycle():
    broker = InMemoryBroker()
    await broker.connect()

    coordinator = WorkerCoordinator(broker=broker, concurrency=2)

    #Enqueue two tasks
    task1 = TaskMessage(name="tests.test_coordinator.add_numbers", args=[10,20])
    task2 = TaskMessage(name="test_coordinator.add_numbers", args=[30,40])
    await broker.enqueue(task1)
    await broker.enqueue(task2)

    assert len(broker.in_flight) == 0

    #Run the coordinator until all tasks are processed
    await coordinator.run_until_empty(idle_timeout=0.3)

    #After run_until_empty, tasks should be executed and ACKed (removed from in-flight)
    assert len(broker.in_flight) == 0
