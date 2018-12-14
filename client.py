import rpyc
import hashlib
import os
import sys
from metastore import ErrorResponse
from datetime import datetime
import logging
import json


logging.basicConfig(level=logging.INFO)

def current_time():
    return datetime.now().strftime("%m/%d %H:%M:%S")
"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""

class SurfStoreClient():

    """
    Initialize the client and set up connections to the block stores and
    metadata store using the config file
    """
    metastore = None
    blockstores = dict()
    def __init__(self, config):
        self.config = config
        self.connect_meta_store()
        self.connect_block_store()

    def connect_block_store(self):
        for i in range(self.config['number_of_blockstores']):
            addr, port = config['blockstores'][2 * i : 2 * i + 2]
            self.blockstores[(addr,port)] = rpyc.connect(addr,port)
            logging.info("[%s] connect to block store @ %s:%d" % (current_time(),addr,port))

    def connect_meta_store(self):
        addr, port = self.config['metastores'][:2]
        mstore = rpyc.connect(addr,port)
        leader = mstore.root.who_is_leader()
        mstore.close()
        self.metastore = rpyc.connect(*leader)
        logging.info("[%s] connect to meta store" % (current_time()))
        return 

    # def __del__(self):
    #     try: 
    #         self.metastore.close()
    #         logging.info("[%s] disconnect from meta store @ %s:%d" % (current_time(),addr,port))
    #     except: pass
    #     for b in self.blockstores:
    #         try:
    #             self.blockstores[b].close()
    #             logging.info("[%s] disconnect from block store @ %s:%d" % (current_time(),addr,port))
    #         except: pass

    """
    upload(filepath) : Reads the local file, creates a set of 
    hashed blocks and uploads them onto the MetadataStore 
    (and potentially the BlockStore if they were not already present there).
    """
    def upload(self, filepath):
        ### not finished
        upload_hashlist = []
        upload_blocks = []
        f = open(filepath,'rb')
        while True:
            block = f.read(2048*2048)
            if not block:
                break
            block_hash = hashlib.sha256(block).hexdigest()
            upload_hashlist.append(block_hash)
            upload_blocks.append(block)
        f.close()

        filename = os.path.split(filepath)[-1]
        try:
            version, hashlist = self.metastore.root.read_file(filename)
        except Exception as error:
            if error.error_type == 3:
                version = 0
                hashlist = []
        upload_version = version + 1
        success = False
        while success == False:
            try:
                # print("upload_hashlist,", upload_hashlist)
                self.metaserver.root.modify_file(filename, upload_version, upload_hashlist)
                success = True
                
            except Exception as error:
            # except self.metaserver.root.ErrorResponse as error:
                if error.error_type == 1:
                    # missing blocks
                    missing_blocks = error.missing_blocks
                    missing_blocks = eval(missing_blocks)
                    # print("%d missing_blocks" % len(missing_blocks),missing_blocks)
                    for block_hash in missing_blocks:
                        # print("missing",block_hash)
                        index = upload_hashlist.index(block_hash)
                        block = upload_blocks[index]
                        index = int(block_hash,16) % len(self.blockservers)
                        blockserver = self.blockservers[index]
                        blockserver.root.store_block(block_hash, block)
                elif error.error_type == 2:
                    # wrong version number
                    upload_version = error.current_version + 1
        print("OK")


    """
    delete(filename) : Signals the MetadataStore to delete a file.
    """
    def delete(self, filename):
        pass


    """
        download(filename, dst) : Downloads a file (f) from SurfStore and saves
        it to (dst) folder. Ensures not to download unnecessary blocks.
    """
    def download(self, filename, location):
        pass
        

    """
     Use eprint to print debug messages to stderr
     E.g - 
     self.eprint("This is a debug message")
    """
    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)



if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("usage: python %s <config.json>" % sys.argv[0])
    else:
        config = sys.argv[1]
        with open(config) as f:
            config = json.load(f)

        client = SurfStoreClient(config)
        del client

        