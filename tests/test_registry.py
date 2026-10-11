import pytest
from lightqueue.app import LightQueue
from lightqueue.registry import registry

class DummyBroker:
    """A mock broker to capture enqueued tasks in tests."""
    def __init__(self):
        self.enqueued = []

    async def enqueue(self, task):
        self.enqueued.append(task)


def test_task_direct_call_and_metadata():
    app = LightQueue()

    @app.task(name="test.add", max_retries=5, timeout=12.0)
    def add(a: int, b: int) -> int:
        """Add two numbers together."""
        return a + b

    # 1. Calling directly runs the function synchronously 
    assert add(3,4) == 7

    # 2. Metadata is preserved via functools.update_wrapper
    assert add.__name__=="add"
    assert add.__doc__=="Add two numbers together."
    assert add.task_name == "test.add"
    assert add.max_retries == 5
    assert add.timeout == 12.0

    # 3. Check registry contains the functon
    assert registry.has("test.add")
    assert registry.get("test.add") is not None

@pytest.mark.asyncio
async def test_task_delay_and_apply_async():
    broker = DummyBroker()
    app = LightQueue(broker=broker)

    @app.task(name="test.greet")
    def greet(name: str) -> str:
        return f"Hello, {name}"

    # Enqueue using .delay()
    msg = await greet.delay("Alice")
    assert len(broker.enqueued) == 1
    assert broker.enqueued[0].name == "test.greet"
    assert broker.enqueued[0].args == ["Alice"]
    assert broker.enqueued[0].id == msg.id

    # Enqueue using .apply_async() with countdown
    msg2 = await greet.apply_async(args=["Bob"], countdown=15.0)
    assert len(broker.enqueued) == 2
    assert broker.enqueued[1].args == ["Bob"]
    assert broker.enqueued[1].eta is not None
