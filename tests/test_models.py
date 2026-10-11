import pytest
from lightqueue.models import TaskMessage, TaskResult

def test_task_message_creation_and_defaults():
    msg = TaskMessage(name="tasks.add", args=[1,2], kwargs={"round": True})

    #Verify defaults
    assert len(msg.id) == 36
    assert msg.name == "tasks.add"
    assert msg.args == [1,2]
    assert msg.kwargs == {"round": True}
    assert msg.retries == 0
    assert msg.max_retries == 3
    assert msg.eta is None
    assert msg.timeout == 30.0



def test_task_message_json_roundtrip():
    msg = TaskMessage(name="tasks.send_email", args=["alice@example.com"])
    json_str = msg.to_json()


    assert isinstance(json_str,str)

    restored = TaskMessage.from_json(json_str)
    assert restored.id == msg.id
    assert restored.name == msg.name
    assert restored.args == msg.args
    assert restored.retries == msg.retries


def test_task_result_model():
    res = TaskResult(
        task_id = "abc-123",
        success=True,
        result={"status": "ok"},
        duration= 0.042,
    )
    assert res.task_id == "abc-123"
    assert res.success is True
    assert res.result == {"status": "ok"}
    assert res.error is None
    assert res.duration == 0.042
