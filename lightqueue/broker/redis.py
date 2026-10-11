"""                                                                                                                                                                                                
lightqueue/broker/redis.py                                                                                                                                                                         
                                                                                                                                                                                                    
Production-grade distributed broker implementation using Redis Lists,                                                                                                                              
Sorted Sets, and Hashes via redis.asyncio.                                                                                                                                                         
"""                                                                                                                                                                                                
                                                                                                                                                                                                    
import json                                                                                                                                                                                        
import time                                                                                                                                                                                        
from typing import Optional                                                                                                                                                                        
from lightqueue.broker.base import BaseBroker                                                                                                                                                      
from lightqueue.models import TaskMessage                                                                                                                                                          
                                                                                                                                                                                                    
try:                                                                                                                                                                                               
    import redis.asyncio as aioredis                                                                                                                                                               
except ImportError:                                                                                                                                                                                
    aioredis = None                                                                                                                                                                                
                                                                                                                                                                                                    
                                                                                                                                                                                                    
class RedisBroker(BaseBroker):                                                                                                                                                                     
    """                                                                                                                                                                                            
    Distributed broker backed by Redis.                                                                                                                                                            
    """                                                                                                                                                                                            
                                                                                                                                                                                                    
    def __init__(                                                                                                                                                                                  
        self,                                                                                                                                                                                      
        url: str = "redis://localhost:6379/0",                                                                                                                                                     
        prefix: str = "lightqueue",                                                                                                                                                                
        client=None,                                                                                                                                                                               
    ):                                                                                                                                                                                             
        self.url = url                                                                                                                                                                             
        self.prefix = prefix                                                                                                                                                                       
        self._client = client                                                                                                                                                                      
                                                                                                                                                                                                    
        # Redis key names                                                                                                                                                                          
        self.ready_key = f"{prefix}:ready"                                                                                                                                                         
        self.scheduled_key = f"{prefix}:scheduled"                                                                                                                                                 
        self.in_flight_key = f"{prefix}:in_flight"                                                                                                                                                 
        self.dlq_key = f"{prefix}:dlq"                                                                                                                                                             
                                                                                                                                                                                                    
    async def connect(self) -> None:                                                                                                                                                               
        """Connect to the Redis server."""                                                                                                                                                         
        if self._client is None:                                                                                                                                                                   
            if aioredis is None:                                                                                                                                                                   
                raise ImportError("Please install redis: pip install redis")                                                                                                                       
            self._client = aioredis.from_url(self.url, decode_responses=True)                                                                                                                      
                                                                                                                                                                                                    
    async def disconnect(self) -> None:                                                                                                                                                            
        """Close connection to Redis."""                                                                                                                                                           
        if self._client is not None:                                                                                                                                                               
            await self._client.aclose()                                                                                                                                                            
                                                                                                                                                                                                    
    async def enqueue(self, task: TaskMessage) -> None:                                                                                                                                            
        """                                                                                                                                                                                        
        If task has a future ETA, push to Sorted Set (ZADD).                                                                                                                                       
        Otherwise, push to Ready List (LPUSH).                                                                                                                                                     
        """                                                                                                                                                                                        
        now = time.time()                                                                                                                                                                          
        payload = task.to_json()                                                                                                                                                                   
                                                                                                                                                                                                    
        if task.eta is not None and task.eta > now:                                                                                                                                                
            # Store in Sorted Set scored by ETA timestamp                                                                                                                                          
            await self._client.zadd(self.scheduled_key, {payload: task.eta})                                                                                                                       
        else:                                                                                                                                                                                      
            # Store in Ready List                                                                                                                                                                  
            await self._client.lpush(self.ready_key, payload)                                                                                                                                      
                                                                                                                                                                                                    
    async def poll_scheduled(self) -> int:                                                                                                                                                         
        """                                                                                                                                                                                        
        Atomically inspects the Sorted Set and moves due tasks to the ready queue.                                                                                                                 
        """                                                                                                                                                                                        
        now = time.time()                                                                                                                                                                          
        # Fetch all tasks whose score (timestamp) <= now                                                                                                                                           
        due_items = await self._client.zrangebyscore(self.scheduled_key, min=0, max=now)                                                                                                           
                                                                                                                                                                                                    
        promoted = 0                                                                                                                                                                               
        for payload in due_items:                                                                                                                                                                  
            # Remove from sorted set and push to ready queue                                                                                                                                       
            removed = await self._client.zrem(self.scheduled_key, payload)                                                                                                                         
            if removed > 0:                                                                                                                                                                        
                await self._client.lpush(self.ready_key, payload)                                                                                                                                  
                promoted += 1                                                                                                                                                                      
                                                                                                                                                                                                    
        return promoted                                                                                                                                                                            
                                                                                                                                                                                                    
    async def dequeue(self, timeout: float = 1.0) -> Optional[TaskMessage]:                                                                                                                        
        """                                                                                                                                                                                        
        Pops a task from the right of the ready queue using atomic BRPOP,                                                                                                                          
        and records it in the in-flight Hash.                                                                                                                                                      
        """                                                                                                                                                                                        
        # brpop timeout in redis expects an integer or float                                                                                                                                       
        # returns tuple: (key_name, popped_value) or None                                                                                                                                          
        result = await self._client.brpop(self.ready_key, timeout=timeout)                                                                                                                         
        if result is None:                                                                                                                                                                         
            return None                                                                                                                                                                            
                                                                                                                                                                                                    
        _, payload = result                                                                                                                                                                        
        task = TaskMessage.from_json(payload)                                                                                                                                                      
                                                                                                                                                                                                    
        # Track in in-flight hash                                                                                                                                                                  
        await self._client.hset(self.in_flight_key, task.id, payload)                                                                                                                              
        return task                                                                                                                                                                                
                                                                                                                                                                                                    
    async def ack(self, task: TaskMessage) -> None:                                                                                                                                                
        """Remove task from in-flight Hash."""                                                                                                                                                     
        await self._client.hdel(self.in_flight_key, task.id)                                                                                                                                       
                                                                                                                                                                                                    
    async def push_dlq(self, task: TaskMessage, error: str) -> None:                                                                                                                               
        """Remove from in-flight and save error report to DLQ List."""                                                                                                                             
        await self._client.hdel(self.in_flight_key, task.id)                                                                                                                                       
                                                                                                                                                                                                    
        dlq_entry = json.dumps({                                                                                                                                                                   
            "task_id": task.id,                                                                                                                                                                    
            "task_name": task.name,                                                                                                                                                                
            "error": error,                                                                                                                                                                        
            "payload": task.to_dict(),                                                                                                                                                             
        })                                                                                                                                                                                         
        await self._client.lpush(self.dlq_key, dlq_entry)