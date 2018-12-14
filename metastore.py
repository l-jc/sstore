import rpyc
import sys
import json
import logging
import time 
from datetime import datetime
import threading
import resetable
import random

logging.basicConfig(level=logging.INFO)


'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues - 
1. The file being modified has missing blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''
class ErrorResponse(Exception):
    def __init__(self, message):
        super(ErrorResponse, self).__init__(message)
        # super().__init__(message)
        self.error = message

    def missing_blocks(self, hashlist):
        self.error_type = 1
        self.missing_blocks = hashlist

    def wrong_version_error(self, version):
        self.error_type = 2
        self.current_version = version

    def file_not_found(self):
        self.error_type = 3

def current_time():
    return datetime.now().strftime("%m/%d %H:%M:%S")

def randt():
    return random.randint(30,50) / 100

'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''
class MetadataStore(rpyc.Service):

    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """
    def __init__(self, config, sid):
        self.config = config
        self.metapeers = dict()
        self.blockstores = dict()
        for i in range(self.config['number_of_blockstores']):
            addr = self.config['blockstores'][i * 2]
            port = self.config['blockstores'][i * 2 + 1]
            self.blockstores[(addr,port)] = rpyc.connect(addr,port)
        self.metadata = dict()
        self.tombstone = dict()
        self.id = sid
        addr = self.config['metastores'][self.id * 2]
        port = self.config['metastores'][self.id * 2 + 1]
        self.node = (addr,port)
        for i in range(self.config['number_of_metastores']):
            if i == self.id: continue 
            addr = self.config['metastores'][i * 2]
            port = self.config['metastores'][i * 2 + 1]
            self.metapeers[(addr,port)] = None

        # attributes of raftnode
        self.term = 0
        self.hasVoted = False
        self.leader = None
        self.state = 'follower'
        self.election_alert = threading.Event()
        self.election_timer = resetable.ResetableTimer(randt(), self.election_alert)
        t = threading.Thread(target=self.run)
        t.start()

    def run(self):
        def try_connect(addr,port):
            try:
                conn = rpyc.connect(addr,port)
            except ConnectionRefusedError:
                conn = None
            return conn

        logging.info("[%s] meta store start @ %s" % (current_time(),self.node))
        self.election_timer.start()

        while True:
            if self.state == 'leader':
                for peer in self.metapeers:
                    if self.metapeers[peer] == None:
                        self.metapeers[peer] = try_connect(*peer)
                    if self.metapeers[peer] != None:
                        try:
                            self.metapeers[peer].root.append_entry(self.term, self.node)
                        except EOFError:
                            self.metapeers[peer] = None
                time.sleep(0.1)
            elif self.state == 'follower' and self.leader == None and self.election_alert.is_set():
                self.state = 'candidate'
            elif self.state == 'candidate' and self.election_alert.is_set():
                logging.info("[%s] term %d timeout" % (current_time(), self.term))
                self.election_alert.clear()
                self.election_timer.resetTimer(randt())
                self.term += 1
                votes = 1
                self.hasVoted = True

                while (not self.election_alert.is_set()) and self.leader == None:
                    for peer in self.metapeers:
                        if self.metapeers[peer] == None:
                            self.metapeers[peer] = try_connect(*peer)
                        if self.metapeers[peer] != None:
                            try:
                                v = self.metapeers[peer].root.request_vote(self.term, self.node)
                            except EOFError:
                                v = False
                                self.metapeers[peer] = None
                            if v: votes += 1

                    if votes > len(self.metapeers) / 2:
                        self.state = 'leader'
                        self.leader = self.node
                        logging.info("[%s] elected as leader of term %d" % (current_time(),self.term))
                        break
            elif self.state == 'follower' and self.leader != None and self.election_alert.is_set():
                logging.info("[%s] not hearing from leader" % current_time())
                self.election_alert.clear()
                self.leader = None
                self.election_timer.resetTimer(randt())



        # connect to block stores
        # blockstores = config['blockstores']
        # number_of_blockstores = config['number_of_blockstores']
        # for i in range(number_of_blockstores):
        #     addr = blockstores[i * 2]
        #     port = blockstores[i * 2 + 1]
        #     self.blockstores[(addr,port)] = rpyc.connect(*(addr,port))
        #     logging.info("[%s] connect to block store @ %s:%d"%(current_time(),addr,port))



    '''
        ModifyFile(f,v,hl): Modifies file f so that it now contains the
        contents refered to by the hashlist hl.  The version provided, v, must
        be exactly one larger than the current version that the MetadataStore
        maintains.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    def exposed_modify_file(self, filename, version, hashlist):
        try:
            current_version, _ = self.metadata[filename]
        except KeyError:
            try:
                current_version = self.tombstone[filename]
            except KeyError:
                current_version = 0
        if current_version + 1 != version:
            error = ErrorResponse("modification failed with wrong version, version %d expected, version %d received" % (current_version+1,version))
            error.wrong_version_error(current_version)
            raise error

        missing_blocks = []
        for blockhash, server in hashlist:
            blockstore = self.blockstores[server]
            if not blockstore.root.has_block(blockhash):
                missing_blocks.append(blockhash)

        if len(missing_blocks) == 0:
            self.metadata[filename] = (int(version), list(hashlist))
            if filename in self.tombstone: del self.tombstone[filename]
        else:
            error = ErrorResponse("modification failed with missing blocks")
            error.missing_blocks(missing_blocks)
            raise error


    '''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    def exposed_delete_file(self, filename, version):
        try:
            current_version, _ = self.metadata[filename]
        except KeyError:
            error = ErrorResponse("file not found")
            error.file_not_found()
            raise error 
        if version != current_version + 1:
            error = ErrorResponse("deletion failed with wrong version number")
            error.wrong_version_error(current_version)
        else:
            del self.metadata[filename]
            self.tombstone[filename] = version


    '''
        (v,hl) = ReadFile(f): Reads the file with filename f, returning the
        most up-to-date version number v, and the corresponding hashlist hl. If
        the file does not exist, v will be 0.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    def exposed_read_file(self, filename):
        """ return a hashlist and a version number """
        try:
            version, hashlist = self.metadata[filename]
        except KeyError:
            try:
                version = self.tombstone[filename]
                hashlist = []
            except KeyError:
                version = 0
                hashlist = []
        return (version, hashlist)


    def exposed_append_entry(self, term, leader):
        self.leader = leader
        self.state = 'follower'
        self.term = term
        self.election_alert.clear()
        self.election_timer.resetTimer()
        logging.debug("[%s] receive hearbeat from %s" % (current_time(), self.leader))

    def exposed_request_vote(self, term, node):
        logging.info("[%s] a vote is requested by %s" % (current_time(),node))
        if self.term < term:
            if self.state == 'leader':
                self.state = 'follower'
                self.election_alert.clear()
                self.election_timer.resetTimer()
            self.term = term
            self.hasVoted = True
            return True
        elif self.term == term and not self.hasVoted:
            self.hasVoted = True
            return True
        else:
            return False

    def exposed_who_is_leader(self):
        return self.leader

    def exposed_is_leader(self):
        return self.state == 'leader'



if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer
    if len(sys.argv) != 3:
        print("usage: python %s <config.json> <id>" % sys.argv[0])
    else:
        config = sys.argv[1]
        sid = int(sys.argv[2])
        with open(config) as f:
            config = json.load(f)
        server_port = config['metastores'][sid * 2 + 1] 

        server = ThreadPoolServer(MetadataStore(config, sid), port = server_port)
        server.start()
