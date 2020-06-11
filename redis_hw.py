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

LOCK_TIMEOUT = 3
lock_key = 'lock.foo'

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

#lock release
def release_lock(timeout):
    if time.time() < timeout:
        r.delete(lock_key)

# returns the next fibonacci number 
def nextFibo(n): 
    if n == None:
        return 1
    return int(round(float(n) * (1 + sqrt(5))/2.0))

def do_work(worker_id):

    try:
        zname = 'heartbeats'
        zset = r.zrange(zname, 0, -1, withscores=True)
        # score: date of the last heartbeat (unix timestamp in seconds)
        # member: id of the worker
        for pair in zset:
            id = pair[0]
            date = pair[1]
            # delete outdated elements (score < (now() - T) seconds) 
            if date < time.time() - T:
                print 'DELETING outdated member' + str(id)
                r.zrem(zname, id)
        
        count = r.zcount(zname, float('-inf'), float('inf'))
        # if there are less than N active elements => add the worker to the zset, otherwise => exit
        if count < N:
            print 'ADDING new worker'
            r.zadd(zname, {worker_id: time.time()})
        else: 
            return

        init= time.time()
        
        fin_time1 = init + T
        fin_time2 = init + W
        fin_time3 = init + C
 
        while(True):
            # every T seconds, the worker updates its entry in heartbeats zset with the current timestamp
            if time.time() >= fin_time1:
                r.zadd(zname, {worker_id: time.time()})
                fin_time1 = time.time() + T
            
            # every W seconds, the worker updates a Redis string key 'fib'
            if time.time() >= fin_time2:
                timeout = acquire_lock()
                next_fibo = nextFibo(r.get('fib'))
                print 'UPDATED FIBO to ' + str(next_fibo)
                r.getset('fib', next_fibo)
                fin_time2 = time.time() + W
                release_lock(timeout)
            
            # every C seconds, the worker has 10% chance to crash
            if time.time() >= fin_time3:
                chance = random.randint(1, 10)
                if chance == 1:
                    return
                fin_time3 = time.time() + C

    except Exception as e:
        print(e)

class WorkerThread (threading.Thread):
   def __init__(self, threadID):
      threading.Thread.__init__(self)
      self.threadID = threadID
   def run(self):
        print "Starting thread no. " + str(self.threadID) + "\n"
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
