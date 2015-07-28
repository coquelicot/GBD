#!/usr/bin/python2

import os
import sys
import struct
import socket
import traceback
import logging
from cached_gbd import CachedGBD

logger = logging.getLogger('gbd')

class NBDService:

    NBD_REQ_MAGIC = struct.pack("!I", 0x25609513)
    NBD_RPY_MAGIC = struct.pack("!I", 0x67446698)

    NBD_REQ_MASK = 0xffff
    NBD_REQ_FLAG_MASK = 0xffff0000
    NBD_CMD_READ = 0
    NBD_CMD_WRITE = 1
    NBD_CMD_DISC = 2
    NBD_CMD_FLUSH = 3
    #NBD_CMD_TRIM = 4

    NBD_ERR_PERM = 1
    NBD_ERR_IO = 5
    NBD_ERR_NOMEM = 12
    NBD_ERR_INVAL = 22
    NBD_ERR_NOSPC = 28

    def __init__(self, conn, gbd):
        self.conn = conn
        self.gbd = gbd

    def serve(self):

        while True:

            cmd, handle, offset, length, data = self.get_request()
            seq = "%016x" % struct.unpack("!Q", handle)[0]

            if cmd == self.NBD_CMD_DISC:
                logger.info("Disconnect")
                self.gbd.end()
                sys.exit(0)

            if cmd == self.NBD_CMD_READ:
                logger.debug("{0}: Read {1} {2}".format(seq, offset, length))
                def gcb(handle):
                    def cb(err, data):
                        if err:
                            logging.error("Read failed: {0}".format(e))
                            self.send_reply(self.NBD_ERR_IO, handle)
                        else:
                            self.send_reply(0, handle, data)
                    return cb
                self.gbd.read(offset, length, callback=gcb(handle))

            elif cmd == self.NBD_CMD_WRITE:
                logger.debug("{0}: Write {1} {2}".format(seq, offset, length))
                assert len(data) == length
                def gcb(handle):
                    def cb(err):
                        if err:
                            logging.error("Write failed: {0}".format(e))
                            self.send_reply(self.NBD_ERR_IO, handle)
                        else:
                            self.send_reply(0, handle)
                    return cb
                self.gbd.write(offset, data, callback=gcb(handle))

            elif cmd == self.NBD_CMD_FLUSH:
                logger.debug("{0}: Flush".format(seq))
                self.send_reply(0, handle)

            else:
                logger.error("{0}: Unknown command {1}".format(seq, cmd))
                self.send_reply(self.NBD_ERR_INVAL, handle)

    def get_request(self):

        magic = self.conn.recv(4, socket.MSG_WAITALL)
        type = struct.unpack("!I", self.conn.recv(4, socket.MSG_WAITALL))[0]
        handle = self.conn.recv(8, socket.MSG_WAITALL)
        offset = struct.unpack("!Q", self.conn.recv(8, socket.MSG_WAITALL))[0]
        length = struct.unpack("!I", self.conn.recv(4, socket.MSG_WAITALL))[0]
        data = None

        assert magic == self.NBD_REQ_MAGIC
        assert (type & self.NBD_REQ_FLAG_MASK) == 0

        cmd = type & self.NBD_REQ_MASK
        if cmd == self.NBD_CMD_FLUSH:
            assert offset == 0 and length == 0
        elif cmd == self.NBD_CMD_WRITE:
            data = self.conn.recv(length, socket.MSG_WAITALL)

        return (cmd, handle, offset, length, data)

    def send_reply(self, error, handle, data=None):
        self.conn.send(self.NBD_RPY_MAGIC)
        self.conn.send(struct.pack("!I", error))
        self.conn.send(handle)
        if data:
            self.conn.send(data)

class NBDServer:

    MAGIC = struct.pack("!Q", 0x49484156454F5054)

    NBD_OPT_EXPORT_NAME = 1
    NBD_OPT_ABORT = 2

    NBD_FLAG_HAS_FLAGS = 1 << 0
    NBD_FLAG_READ_ONLY = 1 << 1
    NBD_FLAG_SEND_FLUSH = 1 << 2
    NBD_FLAG_SEND_FUA = 1 << 3
    NBD_FLAG_ROTATIONAL = 1 << 4
    NBD_FLAG_SEND_TRIM = 1 << 5

    NBD_FLAG_FIXED_NEWSTYLE = 1 << 0
    NBD_FLAG_NO_ZEROES = 1 << 1

    NBD_FLAG_C_FIXED_NEWSTYLE = 1 << 0
    NBD_FLAG_C_NO_ZEROS = 1 << 1

    GBD_NAME_FMT = "gbd-{0}"

    def __init__(self, create=False, host='0.0.0.0', port=10809):

        self.create = create
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.sock.listen(4)

    def run(self):

        while True:

            conn, addr = self.sock.accept()
            if os.fork() != 0:
                conn.close()
                continue

            #self.sock.close()
            logger.info("Accept client from {0}".format(addr))
            conn, gbd = self.handshake(conn)
            NBDService(conn, gbd).serve()
            sys.exit(0)

    def handshake(self, conn):

        conn.send("NBDMAGIC")
        conn.send(self.MAGIC)
        conn.send(struct.pack("!H", self.NBD_FLAG_FIXED_NEWSTYLE | self.NBD_FLAG_NO_ZEROES))

        cliopt = struct.unpack("!I", conn.recv(4, socket.MSG_WAITALL))[0]
        assert conn.recv(8, socket.MSG_WAITALL) == self.MAGIC
        c_no_zero = cliopt & self.NBD_FLAG_C_NO_ZEROS

        option = struct.unpack("!I", conn.recv(4, socket.MSG_WAITALL))[0]
        assert option == self.NBD_OPT_EXPORT_NAME
        
        length = struct.unpack("!I", conn.recv(4, socket.MSG_WAITALL))[0]
        name = conn.recv(length, socket.MSG_WAITALL)

        gbd = self.get_gbd(name)

        conn.send(struct.pack("!Q", gbd.total_size))
        conn.send(struct.pack("!H", 0))
        if not c_no_zero:
            conn.send("\0" * 124)

        return conn, gbd

    def get_gbd(self, name):

        name = self.GBD_NAME_FMT.format(name)

        if not os.path.isfile(name):
            cache_size = int(raw_input("Desired cache size (MB): "))
            assert cache_size > 0
            with open(name, 'wb') as fout:
                fout.write("\0" * (cache_size << 20))

        return CachedGBD(
            cache_file=name,
            create=self.create,
            gbd_data_folder=name)
    
if __name__ == "__main__":

    logging.basicConfig()
    logger.setLevel(logging.DEBUG)
    NBDServer(create=True).run()
