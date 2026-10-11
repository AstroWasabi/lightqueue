import pytest
import asyncio
import time
from lightqueue.models import TaskMessage
from lightqueue.broker.memory import InMemoryBroker

@pytest.mark.asyncio
async def test_memory_broker_enqueue_dequeue_ack():
    broker = InMemoryBroker()
    await broker.connect()

    task1 = TaskMessage(name="task.one", args=[1])
    task2 = TaskMessage(name="task.two", args=[2])

    await broker.enqueue(task1)
    await broker.enqueue(task2)

    # Dequeue task 1 (FIFO order)
    popped1 = await broker.dequeue(timeout=0.1)
    assert popped1 is not None
    assert popped1.id == task1.id

    # Verify task 1 (it should be removed from inflight)
    assert task1.id in broker.in_flight

    # Acknowledge task 1 (it should be removed from in-flight)
    await broker.ack(popped1)
    assert task1.id not in broker.in_flight

    # Dequeue task 2
    popped2 = await broker.dequeue(timeout=0.1)
    assert popped2 is not None
    assert popped2.id == task2.id
    await broker.ack(popped2)


    # Queue should now be empty: timeout returns to None
    empty = await broker.dequeue(timeout=0.05)
    assert empty is None

@pytest.mark.asyncio
async def test_memory_broker_dlq():
    broker = InMemoryBroker()
    await broker.connect()

    task = TaskMessage(name="failing.task")
    await broker.enqueue(task)

    popped = await broker.dequeue(timeout=0.1)
    assert popped is not None
    assert popped.id in broker.in_flight

    # Push to DLQ with error message
    await broker.push_dlq(popped, error="ZeroDivisionError")

    # Task should be in dlq and removed from in-flight
    assert len(broker.dlq) == 1
    assert broker.dlq[0]["task_id"] == task.id
    assert broker.dlq[0]["error"] == "ZeroDivisionError"
    assert task.id not in broker.in_flight

@pytest.mark.asyncio
async def test_delayed_task_scheduling():
    broker = InMemoryBroker()
    await broker.connect()

    now = time.time()
    # Task 1 is scheduled 0.2 seconds in the future
    future_task = TaskMessage(name="future.task", eta=now + 0.2)
    # Task 2 is ready immediately 
    ready_task = TaskMessage(name="ready.task")

    await broker.enqueue(future_task)
    await broker.enqueue(ready_task)

    # immediately, only ready_task should be popped
    popped = await broker.dequeue(timeout=0.05)
    assert popped is not None
    assert popped.id == ready_task.id
    await broker.ack(popped)

    # future_task is not due yet: dequeue should return to None
    empty = await broker.dequeue(timeout=0.05)
    assert empty is None

    # Wait for the future time to pass
    await asyncio.sleep(0.25)

    # poll_scheduled should promote exactly 1 task into the ready queue
    promoted = await broker.poll_scheduled()
    assert promoted == 1

    # Now future task is available om the ready queue
    popped_delayed = await broker.dequeue(timeout=0.05)
    assert popped_delayed is not None
    assert popped_delayed.id == future_task.id
    assert broker.ack(popped_delayed)

