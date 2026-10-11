"""                                                                                                                                                                                                
examples/quickstart.py                                                                                                                                                                             
                                                                                                                                                                                                    
End-to-end demonstration of LightQueue:                                                                                                                                                            
1. Defines background tasks with @app.task                                                                                                                                                         
2. Enqueues immediate and delayed tasks                                                                                                                                                            
3. Runs the worker coordinator to execute them                                                                                                                                                     
"""                                                                                                                                                                                                
                                                                                                                                                                                                    
import asyncio                                                                                                                                                                                     
import time                                                                                                                                                                                        
from lightqueue.app import LightQueue                                                                                                                                                              
from lightqueue.broker.memory import InMemoryBroker                                                                                                                                                
from lightqueue.worker.coordinator import WorkerCoordinator                                                                                                                                        
                                                                                                                                                                                                    
# 1. Create the broker and application                                                                                                                                                             
broker = InMemoryBroker()                                                                                                                                                                          
app = LightQueue(broker=broker)                                                                                                                                                                    
                                                                                                                                                                                                    
                                                                                                                                                                                                    
# 2. Define background tasks with decorators                                                                                                                                                       
@app.task(max_retries=2, timeout=5.0)                                                                                                                                                              
def send_welcome_email(user_email: str) -> str:                                                                                                                                                    
    print(f"📧 Sending welcome email to {user_email}...")                                                                                                                                          
    time.sleep(0.1)  # Simulate network I/O                                                                                                                                                        
    return f"Email delivered to {user_email}"                                                                                                                                                      
                                                                                                                                                                                                    
                                                                                                                                                                                                    
@app.task(max_retries=2, timeout=5.0)                                                                                                                                                              
def generate_pdf_report(report_id: int) -> str:                                                                                                                                                    
    print(f"📊 Generating PDF report #{report_id}...")                                                                                                                                             
    time.sleep(0.2)  # Simulate CPU rendering                                                                                                                                                      
    return f"Report #{report_id} generated"                                                                                                                                                        
                                                                                                                                                                                                    
                                                                                                                                                                                                    
@app.task(max_retries=3, timeout=5.0)                                                                                                                                                              
def sync_inventory_item(item_id: str) -> str:                                                                                                                                                      
    print(f"📦 Synchronizing inventory item: {item_id}")                                                                                                                                           
    return f"Inventory {item_id} synced"                                                                                                                                                           
                                                                                                                                                                                                    
                                                                                                                                                                                                    
async def main():                                                                                                                                                                                  
    print("=" * 60)                                                                                                                                                                                
    print("🚀 Starting LightQueue Quickstart Demo")                                                                                                                                                
    print("=" * 60)                                                                                                                                                                                
                                                                                                                                                                                                    
    # 3. Enqueue immediate tasks using .delay()                                                                                                                                                    
    print("\n1. Enqueueing immediate tasks via .delay()...")                                                                                                                                       
    await send_welcome_email.delay("alice@example.com")                                                                                                                                            
    await send_welcome_email.delay("bob@example.com")                                                                                                                                              
    await generate_pdf_report.delay(101)                                                                                                                                                           
                                                                                                                                                                                                    
    # 4. Enqueue a delayed task (runs 1.0 second in future) via .apply_async()                                                                                                                     
    print("2. Enqueueing delayed task with countdown=1.0s...")                                                                                                                                     
    await sync_inventory_item.apply_async(args=["SKU-9999"], countdown=1.0)                                                                                                                        
                                                                                                                                                                                                    
    # 5. Start the worker coordinator with 2 worker subprocesses                                                                                                                                   
    print("\n3. Starting WorkerCoordinator (concurrency=2)...")                                                                                                                                    
    coordinator = WorkerCoordinator(broker=broker, concurrency=2)                                                                                                                                  
                                                                                                                                                                                                    
    # Process all tasks until queue is empty                                                                                                                                                       
    await coordinator.run_until_empty(idle_timeout=1.5)                                                                                                                                            
                                                                                                                                                                                                    
    print("\n" + "=" * 60)                                                                                                                                                                         
    print("✅ All tasks successfully executed and acknowledged!")                                                                                                                                  
    print("=" * 60)                                                                                                                                                                                
                                                                                                                                                                                                    
                                                                                                                                                                                                    
if __name__ == "__main__":                                                                                                                                                                         
    asyncio.run(main())