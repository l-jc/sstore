import rpyc
import sys
import json

"""
The BlockStore service is an in-memory data store that stores blocks of data,
indexed by the hash value.  Thus it is a key-value store. It supports basic
get() and put() operations. It does not need to support deleting blocks of
data–we just let unused blocks remain in the store. The BlockStore service only
knows about blocks–it doesn’t know anything about how blocks relate to files.
"""
class BlockStore(rpyc.Service):


    """
    Initialize any datastructures you may need.
    """
    def __init__(self):
        # a in-memory data structure e.g. dict
        self.storage = dict()
        

    """
        store_block(h, b) : Stores block b in the key-value store, indexed by
        hash value h
    
        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    """
    def exposed_store_block(self, h, block):
        """ return true or false """
        self.storage[h] = block


    """
    b = get_block(h) : Retrieves a block indexed by hash value h
    
        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    """
    def exposed_get_block(self, h):
        """ returns bytes """
        return self.storage[h]

    """
        rue/False = has_block(h) : Signals whether block indexed by h exists
        in the BlockStore service

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    """
    def exposed_has_block(self, h):
        """ return true or false """
        if h in self.storage:
            return True
        else:
            return False
        
if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer
    if len(sys.argv) != 3:
        print("usage: python %s <config.json> <id>" % sys.argv[0])
    else:
        config = sys.argv[1]
        sid = int(sys.argv[2])
        with open(config) as f:
            config = json.load(f)
        server_port = config['blockstores'][sid * 2 + 1] 
        server = ThreadPoolServer(BlockStore(), port=server_port)
        server.start()
