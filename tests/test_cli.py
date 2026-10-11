"""                                                                                                                                                                                                
tests/test_cli.py                                                                                                                                                                                  
"""                                                                                                                                                                                                
                                                                                                                                                                                                    
from lightqueue.cli import build_parser                                                                                                                                                            
                                                                                                                                                                                                    
                                                                                                                                                                                                    
def test_cli_parser_defaults():                                                                                                                                                                    
    parser = build_parser()                                                                                                                                                                        
    args = parser.parse_args(["worker"])                                                                                                                                                           
                                                                                                                                                                                                    
    assert args.command == "worker"                                                                                                                                                                
    assert args.concurrency == 4                                                                                                                                                                   
    assert args.broker == "memory"                                                                                                                                                                 
    assert args.app is None                                                                                                                                                                        
                                                                                                                                                                                                    
                                                                                                                                                                                                    
def test_cli_parser_custom_flags():                                                                                                                                                                
    parser = build_parser()                                                                                                                                                                        
    args = parser.parse_args([                                                                                                                                                                     
        "worker",                                                                                                                                                                                  
        "--concurrency", "8",                                                                                                                                                                      
        "--broker", "redis",                                                                                                                                                                       
        "--redis-url", "redis://redis-server:6379/1",                                                                                                                                              
        "--app", "my_project.tasks",                                                                                                                                                               
    ])                                                                                                                                                                                             
                                                                                                                                                                                                    
    assert args.command == "worker"                                                                                                                                                                
    assert args.concurrency == 8                                                                                                                                                                   
    assert args.broker == "redis"                                                                                                                                                                  
    assert args.redis_url == "redis://redis-server:6379/1"                                                                                                                                         
    assert args.app == "my_project.tasks"