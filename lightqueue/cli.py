"""                                                                                                                                                                                                
lightqueue/cli.py                                                                                                                                                                                  
                                                                                                                                                                                                    
Command-line interface to launch LightQueue worker processes.                                                                                                                                      
Usage:                                                                                                                                                                                             
    python -m lightqueue.cli worker --concurrency 4 --broker memory                                                                                                                                
"""                                                                                                                                                                                                
                                                                                                                                                                                                    
import argparse                                                                                                                                                                                    
import asyncio                                                                                                                                                                                     
import importlib                                                                                                                                                                                   
import logging                                                                                                                                                                                     
import sys                                                                                                                                                                                         
from lightqueue.broker.memory import InMemoryBroker                                                                                                                                                
from lightqueue.broker.redis import RedisBroker                                                                                                                                                    
from lightqueue.worker.coordinator import WorkerCoordinator                                                                                                                                        
                                                                                                                                                                                                    
logging.basicConfig(                                                                                                                                                                               
    level=logging.INFO,                                                                                                                                                                            
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",                                                                                                                                  
)                                                                                                                                                                                                  
logger = logging.getLogger("lightqueue")                                                                                                                                                           
                                                                                                                                                                                                    
                                                                                                                                                                                                    
def build_parser() -> argparse.ArgumentParser:                                                                                                                                                     
    """Build command line argument parser."""                                                                                                                                                      
    parser = argparse.ArgumentParser(                                                                                                                                                              
        prog="lightqueue",                                                                                                                                                                         
        description="LightQueue Distributed Task Engine CLI",                                                                                                                                      
    )                                                                                                                                                                                              
    subparsers = parser.add_subparsers(dest="command", required=True)                                                                                                                              
                                                                                                                                                                                                    
    # `worker` subcommand                                                                                                                                                                          
    worker_parser = subparsers.add_parser("worker", help="Start a background worker process")                                                                                                      
    worker_parser.add_argument(                                                                                                                                                                    
        "-c", "--concurrency",                                                                                                                                                                     
        type=int,                                                                                                                                                                                  
        default=4,                                                                                                                                                                                 
        help="Number of concurrent worker subprocesses (default: 4)",                                                                                                                              
    )                                                                                                                                                                                              
    worker_parser.add_argument(                                                                                                                                                                    
        "-b", "--broker",                                                                                                                                                                          
        type=str,                                                                                                                                                                                  
        choices=["memory", "redis"],                                                                                                                                                               
        default="memory",                                                                                                                                                                          
        help="Broker backend to use: 'memory' or 'redis' (default: memory)",                                                                                                                       
    )                                                                                                                                                                                              
    worker_parser.add_argument(                                                                                                                                                                    
        "--redis-url",                                                                                                                                                                             
        type=str,                                                                                                                                                                                  
        default="redis://localhost:6379/0",                                                                                                                                                        
        help="Redis URL if using redis broker",                                                                                                                                                    
    )                                                                                                                                                                                              
    worker_parser.add_argument(                                                                                                                                                                    
        "-a", "--app",                                                                                                                                                                             
        type=str,                                                                                                                                                                                  
        default=None,                                                                                                                                                                              
        help="Optional module path containing task definitions to import (e.g. 'myapp.tasks')",                                                                                                    
    )                                                                                                                                                                                              
                                                                                                                                                                                                    
    return parser                                                                                                                                                                                  
                                                                                                                                                                                                    
                                                                                                                                                                                                    
async def run_worker(args: argparse.Namespace) -> None:                                                                                                                                            
    """Initializes the broker and coordinator, then starts the worker loop."""                                                                                                                     
    # 1. Import task definitions if specified so they register                                                                                                                                     
    if args.app:                                                                                                                                                                                   
        logger.info(f"Loading application tasks from '{args.app}'...")                                                                                                                             
        try:                                                                                                                                                                                       
            importlib.import_module(args.app)                                                                                                                                                      
        except Exception as exc:                                                                                                                                                                   
            logger.error(f"Failed to load application module '{args.app}': {exc}")                                                                                                                 
            sys.exit(1)                                                                                                                                                                            
                                                                                                                                                                                                    
    # 2. Instantiate broker                                                                                                                                                                        
    if args.broker == "redis":                                                                                                                                                                     
        logger.info(f"Connecting to Redis broker at {args.redis_url}...")                                                                                                                          
        broker = RedisBroker(url=args.redis_url)                                                                                                                                                   
        await broker.connect()                                                                                                                                                                     
    else:                                                                                                                                                                                          
        logger.info("Using InMemoryBroker (local memory only)...")                                                                                                                                 
        broker = InMemoryBroker()                                                                                                                                                                  
        await broker.connect()                                                                                                                                                                     
                                                                                                                                                                                                    
    # 3. Create and launch coordinator                                                                                                                                                             
    coordinator = WorkerCoordinator(                                                                                                                                                               
        broker=broker,                                                                                                                                                                             
        concurrency=args.concurrency,                                                                                                                                                              
    )                                                                                                                                                                                              
    coordinator.install_signal_handlers()                                                                                                                                                          
                                                                                                                                                                                                    
    logger.info(f"Worker ready! Concurrency: {args.concurrency}. Press Ctrl+C to exit.")                                                                                                           
    try:                                                                                                                                                                                           
        await coordinator.run()                                                                                                                                                                    
    finally:                                                                                                                                                                                       
        await broker.disconnect()                                                                                                                                                                  
                                                                                                                                                                                                    
                                                                                                                                                                                                    
def main() -> None:                                                                                                                                                                                
    """CLI entrypoint."""                                                                                                                                                                          
    parser = build_parser()                                                                                                                                                                        
    args = parser.parse_args()                                                                                                                                                                     
                                                                                                                                                                                                    
    if args.command == "worker":                                                                                                                                                                   
        try:                                                                                                                                                                                       
            asyncio.run(run_worker(args))                                                                                                                                                          
        except KeyboardInterrupt:                                                                                                                                                                  
            pass                                                                                                                                                                                   
                                                                                                                                                                                                    
                                                                                                                                                                                                    
if __name__ == "__main__":                                                                                                                                                                         
    main()