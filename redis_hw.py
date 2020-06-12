#!/usr/bin/env python
import redis
import threading
import time
from math import *
import random

# define our connection information for Redis
redis_host = "localhost"
redis_port = 6379
# insert here your own redis password
redis_password = ""
threadLock = threading.Lock()
# create the Redis Connection object
r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)

M = 30
T = 10
W = 20
C = 10
N = 10

LOCK_TIMEOUT = 4
lock_key = 'lock.foo'

# returns the next fibonacci number 
def nextFibo(n): 
    if n == None:
        return 1
    return int(round(float(n) * (1 + sqrt(5))/2.0))

# executes a function every 'interval' seconds
def do_every (interval, func):
    threading.Timer (
        interval,
        do_every, [interval, func]
    ).start ()
    func ()

# executes a function with arguments every 'interval' seconds
def do_every_heartbeat (interval, func, zname, worker_id):
    threading.Timer (
        interval, 
        do_every_heartbeat, [interval, func, zname, worker_id]
    ).start ()
    func(zname, worker_id)

# updates the workers' heartbeat
def worker_heartbeat(zname, worker_id):
    r.zadd(zname, {worker_id: time.time()})

# updates a Redis string key called 'fib'        
def worker_fib():
    timeout = acquire_lock()
    next_fibo = nextFibo(r.get('fib'))
    print 'UPDATED FIBO to ' + str(next_fibo)
    r.getset('fib', next_fibo)
    release_lock(timeout)        

# the worker has a 10% chance to crash
def worker_crash():
    chance = random.randint(1, 10)
    if chance == 1:
        print 'WORKER CRASHED'
        exit()

# lock acquire
def acquire_lock():
    lock_val = 0
    while lock_val == 0:
        now = time.time()
        timeout = now + LOCK_TIMEOUT + 1
        lock_val = r.setnx(lock_key, timeout)
        if lock_val == 1 or now > r.get(lock_key) and now > r.getset(lock_key, timeout):
            return timeout
        else:
            time.sleep(0.001)
    return timeout

# lock release
def release_lock(timeout):
    if time.time() < timeout:
        r.delete(lock_key)

def do_work(worker_id):
    try:
        zname = 'heartbeats'
        zset = r.zrange(zname, 0, -1, withscores=True)
        # score: date of the last heartbeat
        # member: id of the worker
        for pair in zset:
            id = pair[0]
            date = pair[1]
            # delete outdated elements (score < (now() - T)) 
            if date < time.time() - T:
                print 'DELETING outdated member' + str(id)
                r.zrem(zname, id)
        
        count = r.zcount(zname, float('-inf'), float('inf'))
        # if there are less than N active elements => add the worker to the zset, otherwise exit
        if count < N:
            print 'ADDING new worker'
            r.zadd(zname, {worker_id: time.time()})
        else: 
            print 'EXITING worker ' + str(worker_id)
            exit()

        # every T seconds, the worker updates its entry in heartbeats zset with the current timestamp
        do_every_heartbeat (T, worker_heartbeat, zname, worker_id)

        # every W seconds, the worker updates a Redis string key 'fib'
        do_every (W, worker_fib)

        # every C seconds, the worker has 10% chance to crash
        do_every (C, worker_crash)
    
    except Exception as e:
        print(e)

class WorkerThread (threading.Thread):
   def __init__(self, threadID):
      threading.Thread.__init__(self)
      self.threadID = threadID
   def run(self):
        print "Starting worker no. " + str(self.threadID)
        do_work(self.threadID)
       
class ManagerThread (threading.Thread):
   def __init__(self):
        threading.Thread.__init__(self)
   def run(self):
        print "Starting manager thread\n"
        # make sure that the lock is deleted
        r.delete(lock_key)
        threads = []
        worker_id = 0
        
        while(True):
            time.sleep(M)
            worker_id += 1
            thread = WorkerThread(worker_id)
            # Start the new Worker
            thread.start()
            threads.append(thread)
        
        for t in threads:
            t.join()

if __name__ == '__main__':
    manager = ManagerThread()
    # Start Manager Thread
    manager.start()
    # Wait for Manager Thread to complete
    manager.join()
