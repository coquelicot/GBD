import os
import struct
import hashlib
import time
import logging
from threading import Thread, Lock, Condition, Semaphore
from Queue import Queue
from util import TimedPriorityQueue, RLUQueue
from gbd import GBD

logger = logging.getLogger('gbd')

class CachedGBD:

    EMPTY = 0xffffffffffffffff

    def __init__(self, cache_file, dirty=False, *args, **kargs):

        if 'workers' not in kargs:
            kargs['workers'] = 16

        self.done = False

        self.gbd = GBD(*args, **kargs)
        self.uuid = self.gbd.uuid
        self.block_size = self.gbd.block_size
        self.block_count = self.gbd.block_count
        self.total_size = self.gbd.total_size

        self.cache = open(cache_file, 'r+b')
        self.cache_lock = Lock()
        self.entry_count = self.calc_entry_count()
        self.clean_que = RLUQueue(self.entry_count)
        self.dirty_que = RLUQueue(self.entry_count)
        self.last_modify = [0] * self.entry_count
        self.map = {}
        self.rmap = [self.EMPTY] * self.entry_count
        self.map_lock = Lock()
        self.load_cache(dirty)

        self.wb_sem = Semaphore(8)
        self.wb_daemon = Thread(target=self.do_writeback)
        self.wb_daemon.daemon = True
        self.wb_daemon.start()

        self.pull_que = TimedPriorityQueue()
        self.dque_lock = Lock()
        self.pull_delay_que = {}
        self.pull_daemon = Thread(target=self.do_pull)
        self.pull_daemon.daemon = True
        self.pull_daemon.start()

        self.done = True

    ## init

    def load_cache(self, dirty=True):

        self.cache.seek(0, os.SEEK_SET)
        cache_uuid = self.cache.read(len(self.uuid))
        if cache_uuid == "\0" * len(self.uuid):
            logger.info("The cache file is empty, not loading anything")
            for i in xrange(self.entry_count):
                self.clean_que.put(i)
            return
        if cache_uuid != self.uuid:
            raise AssertionError("It's not the correct cache device. (uuid mismatch)")

        self.cache.seek(len(self.uuid), os.SEEK_SET)
        record = self.cache.read(8 * self.entry_count)
        for i in xrange(0, self.entry_count):
            entry = struct.unpack("!Q", record[i*8:i*8+8])[0]
            if entry != self.EMPTY:
                assert entry < self.block_count and entry not in self.map
                self.map[entry] = i
                self.rmap[i] = entry
                if dirty:
                    self.dirty_que.put(i)
                else:
                    self.clean_que.put(i)
                logger.debug("Map {0} => {1}".format(entry, i))
            else:
                self.clean_que.put(i)

    ## interface

    def read(self, offset, length, callback=None):

        assert 0 <= offset < offset + length <= self.total_size

        idxl = offset // self.block_size
        idxr = (offset + length - 1) // self.block_size

        cv = Condition()
        state = [idxr + 1 - idxl, None]
        data_list = [None] * state[0]

        for idx in xrange(idxl, idxr + 1):

            rngl = max(offset, idx * self.block_size)
            rngr = min(offset + length, (idx + 1) * self.block_size)
            shift = rngl % self.block_size
            to_read = rngr - rngl

            def gcb(idx, shift, to_read):
                def cb(err, obj, data):
                    with cv:
                        if state[1] is not None:
                            return False
                        if err:
                            state[1] = err
                            if callback:
                                callback(err, None)
                            else:
                                cv.notify()
                            return False
                        if to_read == self.block_size:
                            data_list[idx - idxl] = data
                        else:
                            with self.cache_lock:
                                self.cache.seek(self.calc_offset(obj) + shift, os.SEEK_SET)
                                data_list[idx - idxl] = self.cache.read(to_read)
                        state[0] = state[0] - 1
                        if state[0] == 0:
                            if callback is None:
                                cv.notify()
                            else:
                                callback(None, ''.join(data_list))
                    return False
                return cb
            self.pull(idx, read_data=(to_read == self.block_size), callback=gcb(idx, shift, to_read))

        if callback is None:
            cv.acquire()
            while state[0] != 0:
                cv.wait()
            cv.release()
            if state[1]:
                raise state[1]
            else:
                assert all(x is not None for x in data_list)
                return ''.join(data_list)

    def write(self, offset, data, callback=None):

        assert 0 <= offset < offset + len(data) <= self.total_size

        idxl = offset // self.block_size
        idxr = (offset + len(data) - 1) // self.block_size

        lock = Lock()
        state = [idxr + 1 - idxl, None]

        for idx in xrange(idxl, idxr + 1):

            rngl = max(offset, idx * self.block_size)
            rngr = min(offset + len(data), (idx + 1) * self.block_size)
            ndata = data[rngl-offset:rngr-offset]
            shift = rngl % self.block_size

            def gcb(shift, ndata):
                def cb(err, obj, _):
                    with lock:
                        if state[1] is not None:
                            return False
                        if err:
                            state[1] = err
                            if callback:
                                callback(err)
                            return False
                        with self.cache_lock:
                            self.cache.seek(self.calc_offset(obj) + shift, os.SEEK_SET)
                            self.cache.write(ndata)
                        state[0] = state[0] - 1
                        if state[0] == 0 and callback:
                            callback(None)
                    return True
                return cb
            cb = gcb(shift, ndata)

            if len(ndata) == self.block_size:
                obj = self.pull(idx, pull_data=False, callback=cb)
            else:
                obj = self.pull(idx, callback=cb)

    def save_map(self):

        def pack(ull):
            return ''.join(chr((ull >> i) % 256) for i in xrange(56, -1, -8))

        logger.info("Saving map...")
        with self.cache_lock:
            self.cache.seek(0, 0)
            self.cache.write(self.uuid)
            self.cache.write(''.join(pack(ent) for ent in self.rmap))
            self.cache.close()

    def sync(self):
        logger.info("Flushing all request to gbd...")
        while True:
            with self.dque_lock:
                if self.dirty_que.empty() and self.pull_que.empty() and len(self.pull_delay_que) == 0:
                    break
            time.sleep(1)
        self.gbd.sync()

    def end(self, force=False):
        if not force:
            self.sync()
        self.gbd.end(True)
        self.save_map()
        logger.info("End CachedGBD")

    ## helper

    def calc_entry_count(self):
        self.cache.seek(0, os.SEEK_END)
        entry_count = (self.cache.tell() - len(self.uuid)) // (self.block_size + 8)
        assert entry_count > 0
        return entry_count

    def calc_offset(self, idx):
        return len(self.uuid) + 8 * self.entry_count + idx * self.block_size

    def pull(self, idx, pull_data=True, read_data=False, callback=None):
        assert 0 <= idx < self.block_count
        assert pull_data or not read_data
        self.pull_que.put((idx, pull_data, read_data, callback))

    ## daemon

    def check_delay_pull(self, idx):
        with self.dque_lock:
            if idx in self.pull_delay_que:
                pack = self.pull_delay_que[idx].get()
                logging.debug("Put pack {0}".format(pack))
                self.pull_que.put(pack, TimedPriorityQueue.PRI_HIGH)
                if self.pull_delay_que[idx].empty():
                    del self.pull_delay_que[idx]

    def do_pull(self):

        while True:

            pack = self.pull_que.get()

            data = None
            modify = False
            idx, pull_data, read_data, callback = pack

            with self.map_lock:
                if idx in self.map:
                    new_block = False
                    obj = self.map[idx]
                    with self.dque_lock:
                        cobj = self.clean_que.pop(obj)
                        dobj = self.dirty_que.pop(obj)
                        assert cobj is None or dobj is None
                        if cobj is None and dobj is None:
                            logging.debug("Delay {0}".format(pack))
                            if idx not in self.pull_delay_que:
                                self.pull_delay_que[idx] = Queue()
                            self.pull_delay_que[idx].put(pack)
                            continue
                else:
                    new_block = True
                    obj = self.clean_que.get()
                    if self.rmap[obj] != self.EMPTY:
                        del self.map[self.rmap[obj]]
                    self.rmap[obj] = idx
                    self.map[idx] = obj

            if not new_block:
                if read_data:
                    with self.cache_lock:
                        self.cache.seek(self.calc_offset(obj), os.SEEK_SET)
                        data = self.cache.read(self.block_size)

            else:
                logger.debug("Pull {0} => {1}".format(idx, obj))
                if pull_data or read_data:
                    def gcb(idx, obj, callback):
                        def cb(err, data):
                            if err:
                                logger.error("Pull {0} => {1}: Fail".format(idx, obj))
                                raise NotImplementedError("Need to propagate pull error")
                            else:
                                logger.debug("Pull {0} => {1}: Check = {2}".format(idx, obj, hashlib.sha1(data).hexdigest()))
                                with self.cache_lock:
                                    self.cache.seek(self.calc_offset(obj), os.SEEK_SET)
                                    self.cache.write(data)
                                if callback and callback(None, obj, data):
                                    self.last_modify[obj] = time.time()
                                    self.dirty_que.put(obj)
                                else:
                                    self.clean_que.put(obj)
                                self.check_delay_pull(idx)
                                logger.debug("Pull {0} => {1}: End".format(idx, obj))
                        return cb
                    self.gbd.read_block(idx, gcb(idx, obj, callback))
                    continue
                else:
                    modify = True

            assert data is None or len(data) == self.block_size

            if callback and callback(None, obj, data):
                modify = True
            if dobj is not None or modify:
                if modify:
                    self.last_modify[obj] = time.time()
                self.dirty_que.put(obj)
            else:
                self.clean_que.put(obj)
            self.check_delay_pull(idx)

    def do_writeback(self):

        delay = 0.5

        while True:

            self.wb_sem.acquire()
            ent = self.dirty_que.get()

            to_sleep = self.last_modify[ent] + delay - time.time()
            if to_sleep > 0:
                self.wb_sem.release()
                logging.debug("Sleep wb {0}".format(to_sleep))
                self.dirty_que.unget(ent)
                time.sleep(to_sleep)
                continue

            with self.map_lock:
                idx = self.rmap[ent]
                assert self.map[idx] == ent

            logger.debug("Collected {0}".format(ent))
            with self.cache_lock:
                self.cache.seek(self.calc_offset(ent), os.SEEK_SET)
                data = self.cache.read(self.block_size)

            logger.debug("Push {0} <= {1}: Check = {2}".format(idx, ent, hashlib.sha1(data).hexdigest()))

            def gcb(idx, ent):
                def cb(err, _):
                    if err:
                        logger.warning("Push {0} <= {1}: Fail".format(idx, ent))
                        self.dirty_que.put(ent)
                    else:
                        logger.debug("Push {0} <= {1}: Success".format(idx, ent))
                        self.clean_que.put(ent)
                    self.check_delay_pull(idx)
                    self.wb_sem.release()
                return cb
            self.gbd.write_block(idx, data, gcb(idx, ent), TimedPriorityQueue.PRI_LOW)

if __name__ == "__main__":

    if not os.path.isfile('./.cache'):
        with open('./.cache', 'wb') as fout:
            fout.write("\0" * (1 << 24))

    gbd = CachedGBD('./.cache', create=True, gbd_data_folder='gbd-meow')

    def read(x, l):
        print "=> '" + gbd.read(x, l) + "'"

    def write(x, d):
        gbd.write(x, d)

    read(0, 5)

    while True:
        cmd = raw_input().strip().split()
        if cmd[0] == 'w':
            write(int(cmd[1]), cmd[2])
        else:
            read(int(cmd[1]), int(cmd[2]))
