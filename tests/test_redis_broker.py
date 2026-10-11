                                                                                                                                                                                                
"""                                                                                                                                                                                                
tests/test_redis_broker.py                                                                                                                                                                         
"""                                                                                                                                                                                                
                                                                                                                                                                                                    
import pytest                                                                                                                                                                                      
import time                                                                                                                                                                                        
import fakeredis.aioredis                                                                                                                                                                          
from lightqueue.models import TaskMessage                                                                                                                                                          
from lightqueue.broker.redis import RedisBroker                                                                                                                                                    
                                                                                                                                                                                                    
                                                                                                                                                                                                    
@pytest.mark.asyncio                                                                                                                                                                               
async def test_redis_broker_enqueue_dequeue_ack():                                                                                                                                                 
    # Use fakeredis in-memory client                                                                                                                                                               
    fake_client = fakeredis.aioredis.FakeRedis(decode_responses=True)                                                                                                                              
    broker = RedisBroker(client=fake_client)                                                                                                                                                       
                                                                                                                                                                                                    
    task1 = TaskMessage(name="email.welcome", args=["user1@example.com"])                                                                                                                          
    task2 = TaskMessage(name="email.newsletter", args=["user2@example.com"])                                                                                                                       
                                                                                                                                                                                                    
    await broker.enqueue(task1)                                                                                                                                                                    
    await broker.enqueue(task2)                                                                                                                                                                    
                                                                                                                                                                                                    
    # Dequeue task 1 (FIFO order)                                                                                                                                                                  
    popped1 = await broker.dequeue(timeout=0.1)                                                                                                                                                    
    assert popped1 is not None                                                                                                                                                                     
    assert popped1.id == task1.id                                                                                                                                                                  
                                                                                                                                                                                                    
    # Verify task 1 is recorded in the in_flight hash                                                                                                                                              
    in_flight_payload = await fake_client.hget(broker.in_flight_key, task1.id)                                                                                                                     
    assert in_flight_payload is not None                                                                                                                                                           
                                                                                                                                                                                                    
    # ACK task 1                                                                                                                                                                                   
    await broker.ack(popped1)                                                                                                                                                                      
    in_flight_after = await fake_client.hget(broker.in_flight_key, task1.id)                                                                                                                       
    assert in_flight_after is None                                                                                                                                                                 
                                                                                                                                                                                                    
    # Dequeue task 2                                                                                                                                                                               
    popped2 = await broker.dequeue(timeout=0.1)                                                                                                                                                    
    assert popped2 is not None                                                                                                                                                                     
    assert popped2.id == task2.id                                                                                                                                                                  
    await broker.ack(popped2)                                                                                                                                                                      
                                                                                                                                                                                                    
    # Empty queue returns None                                                                                                                                                                     
    empty = await broker.dequeue(timeout=0.05)                                                                                                                                                     
    assert empty is None                                                                                                                                                                           
                                                                                                                                                                                                    
                                                                                                                                                                                                    
@pytest.mark.asyncio                                                                                                                                                                               
async def test_redis_broker_scheduled_and_dlq():                                                                                                                                                   
    fake_client = fakeredis.aioredis.FakeRedis(decode_responses=True)                                                                                                                              
    broker = RedisBroker(client=fake_client)                                                                                                                                                       
                                                                                                                                                                                                    
    now = time.time()                                                                                                                                                                              
    future_task = TaskMessage(name="future.job", eta=now + 100.0)                                                                                                                                  
    await broker.enqueue(future_task)                                                                                                                                                              
                                                                                                                                                                                                    
    # Not ready yet                                                                                                                                                                                
    immediate_pop = await broker.dequeue(timeout=0.05)                                                                                                                                             
    assert immediate_pop is None                                                                                                                                                                   
                                                                                                                                                                                                    
    # Move ETA to the past and poll                                                                                                                                                                
    future_task.eta = now - 1.0                                                                                                                                                                    
    await fake_client.zadd(broker.scheduled_key, {future_task.to_json(): now - 1.0})                                                                                                               
                                                                                                                                                                                                    
    promoted = await broker.poll_scheduled()                                                                                                                                                       
    assert promoted >= 1                                                                                                                                                                           
                                                                                                                                                                                                    
    popped = await broker.dequeue(timeout=0.1)                                                                                                                                                     
    assert popped is not None                                                                                                                                                                      
    assert popped.name == "future.job"                                                                                                                                                             
                                                                                                                                                                                                    
    # Push to DLQ                                                                                                                                                                                  
    await broker.push_dlq(popped, error="DatabaseConnectionRefused")                                                                                                                               
    dlq_items = await fake_client.lrange(broker.dlq_key, 0, -1)                                                                                                                                    
    assert len(dlq_items) == 1                                                                                                                                                                     
    assert "DatabaseConnectionRefused" in dlq_items[0]