# membership
Membership protocol over atomic broadcast

## Running Cluster
-v for verbose (debug logging messages)  
`tox -e run -- cluster -c [# of servers] [-v]`

ctrl-c to kill cluster

### Managing the cluster
`ls` to list the currently running servers  
`add [id]` to add server id to the cluster  
`remove [id]` to remove server id from the cluster  
NOTE: id has to be within [0, 100]  

`send [server_id] [sever cmd]` to send a command to a sever  

Server commands available:  
`bc -f [config_file] -m [message]` - broadcast message from server using config  
`config -f [config_file]` - configure channel behavior using config_file  


Examples:  
`add 5`  
`remove 5`  


`send 0 bc -f config.json -m test_message` - use configuration in config.json  
`send 0 bc -m test_message` - use default configuration  
`config 0 bc -m test_message` - use configuration in config.json  

## Running Unittests and Linter
`tox`

## Logging
Log for each server is located in logs/
