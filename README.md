# membership
Membership protocol over atomic broadcast

## Running Cluster
-v for verbose
`tox -e run -- cluster -c [# of servers] [-v]`

ctrl-c to kill cluster

### Managing the cluster
`ls` to list the currently running servers  
`add [id]` to add server id to the cluster  
`remove [id]` to remove server id from the cluster  
NOTE: id has to be within [0, 100]  

## Running Unittests and Linter
`tox`

## Logging
Log for each server is located in logs/
