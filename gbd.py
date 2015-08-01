#!/usr/bin/python2

import json
import hashlib
import logging
import time

from threading import Thread, Lock, Semaphore
from config import Config, Metadata
from util import TimedPriorityQueue
from auth import AuthManager
from apiclient.discovery import build as build_service
from apiclient.http import MediaInMemoryUpload

logger = logging.getLogger('gbd')

class GBDWorker(Thread):

    def __init__(self, gbd, drive):
        Thread.__init__(self)
        self.gbd = gbd
        self.drive = drive

    def run(self):
        while True:
            idx, data, cb = self.gbd.que.get()
            try:
                if data is None:
                    ret = self.read_block(idx)
                else:
                    ret = self.write_block(idx, data)
                err = None
            except Exception as e:
                err = e
                ret = None
                logger.error("I/O failed: {0}".format(e))
            finally:
                try:
                    if cb:
                        cb(err, ret)
                except Exception as e:
                    logger.error("Callback failed: {0}".format(e))
                finally:
                    self.gbd.que.task_done()

    def read_block(self, idx):
        blkid = self.gbd.block_id(idx)
        if blkid is None:
            return "\0" * self.gbd.block_size
        else:
            results = self.drive.files().get_media(fileId=blkid).execute()
            assert len(results) == self.gbd.block_size
            return results

    def write_block(self, idx, data):
        assert len(data) == self.gbd.block_size
        blkid = self.gbd.block_id(idx)
        if blkid is None:
            return self.gbd.new_block(idx, data)
        else:
            media_body = MediaInMemoryUpload(data, mimetype=self.gbd.BLOCK_MIMETYPE, resumable=False)
            return self.drive.files().update(fileId=blkid, media_body=media_body).execute()

class GBD:

    FOLDER_MIMETYPE = 'application/vnd.google-apps.folder'
    BLOCK_MIMETYPE = 'application/octet-stream'

    def __init__(self, **config):

        self.config = Config.copy()
        self.config.update(config)

        self.auth_mgr = AuthManager(
            self.config['appname'],
            self.config['oauth_client_id'],
            self.config['oauth_client_secret'],
            self.config['oauth_scope'],
            self.config['oauth_redirect_uri'])
        self.drive = self.build_service()

        self.data_dir = self.get_data_dir()
        self.uuid = hashlib.sha1(self.data_dir).hexdigest()
        self.load_data_dir()

        self.block_size = self.bd_attr['block_size']
        self.block_count = self.bd_attr['block_count']
        self.total_size = self.block_size * self.block_count
        self.mapping = [None] * self.block_count
        self.que = TimedPriorityQueue()
        self.lock = Lock()

        self.running = True
        self.workers = []
        for i in xrange(self.config.get('workers', 8)):
            worker = GBDWorker(self, self.build_service())
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

    ## init

    def build_service(self):
        return build_service('drive', 'v2', http=self.auth_mgr.get_auth_http())

    def get_data_dir(self):

        folder = self.config['gbd_data_folder']
        query_str = "title='{0}'".format(folder)

        results = self.drive.files().list(q=query_str).execute()
        items = filter(lambda x: not x['labels']['trashed'], results['items'])
        if len(items) == 0:
            if not self.config.get('create', False):
                raise RuntimeError("Can't locate `{0}'".format(folder))
            else:
                return self.create_data_dir()
        if len(items) > 1:
            raise AssertionError("{0} results found for `{1}', don't know which to use".format(len(items), folder))

        item = items[0]
        if item['mimeType'] != self.FOLDER_MIMETYPE:
            raise AssertionError("`{0}' is not a folder!! (mimeType={1})".format(folder, item['mimeType']))
        if not item['editable']:
            raise RuntimeError("folder `{0}' is readonly!".format(folder))

        return item['id']

    def create_data_dir(self):

        folder = self.config['gbd_data_folder']
        body = {
            'title': folder,
            'parents': ['root'],
            'mimeType': self.FOLDER_MIMETYPE,
        }
        result = self.drive.files().insert(body=body).execute()

        if not result:
            raise RuntimeError("Can't create folder `{0}'".format(folder))
        return result['id']

    def load_data_dir(self):

        query_str = "title='config'"
        results = self.drive.children().list(folderId=self.data_dir, q=query_str).execute()
        if len(results['items']) == 0:
            self.init_data_dir()
            return
        if len(results['items']) > 1:
            raise AssertionError("config file should be unique")

        fileId = results['items'][0]['id']
        results = self.drive.files().get_media(fileId=fileId).execute()
        assert results

        self.bd_attr = json.loads(results)
        if self.bd_attr['version'] != Metadata['version']:
            raise AssertionError("Version mismatch: {0} vs {1}", Metadata['version'], self.bd_attr['version'])

    def init_data_dir(self):

        logger.info("Initializing data dir")

        if 'default_block_size' in self.config:
            block_size = int(self.config['default_block_size'])
        else:
            block_size = int(raw_input("Desired block size: "))
        if 'default_total_size' in self.config:
            total_size = int(self.config['default_total_size'])
        else:
            total_size = int(raw_input("Total size: "))
        if total_size < block_size:
            raise ValueError("block_size should not be bigger than total_size.")

        used_size = total_size // block_size * block_size
        if used_size != total_size:
            logger.info("Only using {0} bytes instead of {1}".format(used_size, total_size))

        self.bd_attr = {
            'version': Metadata['version'],
            'block_size': block_size,
            'block_count': used_size // block_size,
        }
        body = {
            'title': 'config',
            'description': 'config file for gbd',
            'mimeType': 'application/json',
            'parents': [{'id': self.data_dir}],
        }
        media_body = MediaInMemoryUpload(json.dumps(self.bd_attr), mimetype='application/json', resumable=False)

        self.drive.files().insert(body=body, media_body=media_body).execute()
    
    ## function

    def read_block(self, idx, cb=None, pri=TimedPriorityQueue.PRI_NORMAL):
        assert 0 <= idx < self.block_count
        if cb:
            self.que.put((idx, None, cb), pri)
        else:
            return self.sync_io(idx, None, pri)

    def write_block(self, idx, data, cb=None, pri=TimedPriorityQueue.PRI_NORMAL):
        assert 0 <= idx < self.block_count
        assert data and len(data) == self.block_size
        if cb:
            self.que.put((idx, data, cb), pri)
        else:
            return self.sync_io(idx, data, pri)

    def sync(self):
        logger.info("Syncing...")
        self.que.join()

    def end(self, force):
        if not force:
            self.sync()
        logger.info("End GBD")

    ## helper

    @classmethod
    def idx_to_name(cls, idx):
        return "gbd_b" + str(idx)

    def block_id(self, idx):
        with self.lock:
            if idx >= self.block_count or idx < 0:
                raise IndexError("Can't map idx {0}".format(idx))
            if self.mapping[idx] is None:
                query_str = "title='{0}'".format(self.idx_to_name(idx))
                results = self.drive.children().list(folderId=self.data_dir, q=query_str).execute()
                if len(results['items']) == 1:
                    self.mapping[idx] = results['items'][0]['id']
                else:
                    assert len(results['items']) == 0
            return self.mapping[idx]

    def new_block(self, idx, data=None):

        with self.lock:

            if idx >= self.block_count or idx < 0:
                raise ValueError("Index out of bound")
            if self.mapping[idx] is not None:
                raise ValueError("None empty mapping @ {0}".format(idx))
            if data is not None:
                assert len(data) == self.block_size
            else:
                data = "\0" * self.block_size

            body = {
                'title': self.idx_to_name(idx),
                'mimeType': self.BLOCK_MIMETYPE,
                'parents': [{'id': self.data_dir}],
            }
            media_body = MediaInMemoryUpload(data, mimetype=self.BLOCK_MIMETYPE, resumable=False)

            result = self.drive.files().insert(body=body, media_body=media_body).execute()
            self.mapping[idx] = result['id']
            return result

    def sync_io(self, idx, data, pri):

        ret = []
        sem = Semaphore(0)
        def mycb(*param):
            ret.append(param)
            sem.release()

        self.que.put((idx, data, mycb), pri)
        sem.acquire()

        err, data = ret.pop()
        if err:
            raise err
        else:
            return data
