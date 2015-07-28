#!/usr/bin/python2

import time
from Queue import PriorityQueue
from threading import Condition

class TimedPriorityQueue(PriorityQueue):

    PRI_HIGH = -1
    PRI_NORMAL = 0
    PRI_LOW = 1

    def __init__(self, *args, **kargs):
        PriorityQueue.__init__(self, *args, **kargs)

    def put(self, item, priority=PRI_NORMAL):
        PriorityQueue.put(self, (priority, time.time(), item))

    def get(self, *args, **kargs):
        _, _, item = PriorityQueue.get(self, *args, **kargs)
        return item

class RLUQueue:

    def __init__(self, size):
        self.size = size
        self.prev = [None] * size
        self.next = [None] * size
        self.prev.append(size)
        self.next.append(size)
        self.cv = Condition()

    ## interface

    def put(self, idx):
        assert 0 <= idx < self.size
        self.cv.acquire()
        if self.in_list(idx):
            self.remove(idx)
        self.append(idx)
        self.cv.notify()
        self.cv.release()

    def get(self, block=True):
        self.cv.acquire()
        if not block and self._empty():
            self.cv.release()
            return None
        while self._empty():
            self.cv.wait()
        ret = self.remove(self.next[-1])
        self.cv.release()
        return ret

    def unget(self, idx):
        assert 0 <= idx < self.size
        with self.cv:
            assert not self.in_list(idx)
            self.prepend(idx)

    def pop(self, idx):
        assert 0 <= idx < self.size
        with self.cv:
            return self.remove(idx) if self.in_list(idx) else None

    def empty(self):
        with self.cv:
            return self._empty()
    
    ## helper

    def _empty(self):
        return self.prev[-1] == self.size

    def in_list(self, idx):
        return self.prev[idx] is not None

    def remove(self, idx):
        self.next[self.prev[idx]] = self.next[idx]
        self.prev[self.next[idx]] = self.prev[idx]
        self.prev[idx] = None
        self.next[idx] = None
        return idx

    def append(self, idx):
        self.next[idx] = self.size
        self.prev[idx] = self.prev[-1]
        self.prev[self.next[idx]] = idx
        self.next[self.prev[idx]] = idx

    def prepend(self, idx):
        self.next[idx] = self.next[-1]
        self.prev[idx] = self.size
        self.prev[self.next[idx]] = idx
        self.next[self.prev[idx]] = idx

