from enum import Enum
import multiprocessing
import queue

# Creates a queue for notifying on Kubernetes state changes
class NHDWatchTypes(Enum):
    NHD_WATCH_TYPE_TRIADSET_CREATE = 1,
    NHD_WATCH_TYPE_TRIADSET_DELETE = 2,
    NHD_WATCH_TYPE_TRIAD_POD_CREATE = 3,
    NHD_WATCH_TYPE_TRIAD_POD_DELETE = 4,
    NHD_WATCH_TYPE_NODE_UNCORDON = 5,
    NHD_WATCH_TYPE_NODE_CORDON = 6,
    NHD_WATCH_TYPE_GROUP_UPDATE = 7,
    NHD_WATCH_TYPE_NODE_MAINT_START = 8,
    NHD_WATCH_TYPE_NODE_MAINT_END = 9,
    NHD_WATCH_TYPE_TRIAD_NODE_DELETE = 10,


class NHDWatchQueue(object):
    def __init__(self):
        self.intq = None

    @property
    def nqueue(self):
        if self.intq is None:
            self.intq = queue.Queue() if (multiprocessing.cpu_count() == 1) else multiprocessing.Queue() 

        return self.intq

    def get(self, *args, **kw):
        return self.nqueue.get(*args, **kw)

    def put(self, *args, **kw):
        return self.nqueue.put(*args, **kw)

    def size(self):
        return self.nqueue.qsize()



qinst = NHDWatchQueue()
