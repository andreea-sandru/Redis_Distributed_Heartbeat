# Distributed heartbeat and locking with Redis

Python version: 2.7

- Python program that simulates a distributed heartbeat mechanism using Redis

- Manager: simple process that starts a worker every M seconds

- Worker: each worker node is required to send periodic signals to indicate that it is alive

- the heartbeat info from all the nodes is stored in a Redis zset key called 'heartbeats'

- an element of the 'heartbeats' zset contains:
  
  - score: date of the last heartbeat
  
  - member: id of the node

- if no heartbeat is received from a node for some time, the node is assumed to have failed

- every T seconds, the worker should update its entry in heartbeats zset with the current timestamp

- every W seconds, the worker needs to update a Redis string key called fib with the next term of the sequence

- every C seconds, the worker has 10% chance to crash
