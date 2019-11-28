from colorlog import ColoredFormatter
from nhd.NHDCommon import NHDCommon
from concurrent import futures
import grpc
import time
import sys
import threading
sys.path.insert(0, 'obj/')
from nhd import nhd_stats_pb2
from nhd import nhd_stats_pb2_grpc
from queue import Queue
from queue import Empty
from nhd.NHDCommon import RpcMsgType


SERVER_LISTEN = '[::]:45655'

"""
gRPC server that starts the server for handling incoming requests.
"""
class NHDRpcServer(threading.Thread):
    def __init__(self, q: Queue):
        self.logger = NHDCommon.GetLogger(__name__)
        self.mainq = q
        threading.Thread.__init__(self)

    def run(self):
        self.logger.info('Initializing RPC server')        

        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        nhd_stats_pb2_grpc.add_NHDControlServicer_to_server(NHDRpcHandler(self.mainq), self.server)
        self.server.add_insecure_port(SERVER_LISTEN)

        self.logger.info(f'Starting server on {SERVER_LISTEN}')
        self.server.start()

        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            self.server.stop(0)

"""
Handles all the gRPC queries. 
"""
class NHDRpcHandler(nhd_stats_pb2_grpc.NHDControlServicer):
    def __init__(self, q: Queue):
        self.mainq = q
        self.logger = NHDCommon.GetLogger(__name__)

    def GetBasicNodeStats(self, request, context):
        self.logger.info('Getting node stats')
        rsp = nhd_stats_pb2.NodeStats(status = nhd_stats_pb2.NHD_STATUS_ERR)

        tmpq = Queue()
        self.mainq.put((RpcMsgType.TYPE_NODE_INFO, tmpq))
        try:
            item = tmpq.get(True, 5)
            rsp.status = nhd_stats_pb2.NHD_STATUS_OK
            for n in item:
                tmpnode = rsp.info.add()
                tmpnode.name        = n['name']
                tmpnode.free_cpus   = n['freecpu']
                tmpnode.used_cpus   = n['totalcpu'] - n['freecpu']
                tmpnode.free_gpus   = n['freegpu']
                tmpnode.used_gpus   = n['totalgpu'] - n['freegpu']
                tmpnode.free_hugepages = n['freehuge_gb']
                tmpnode.used_hugepages = n['totalhuge_gb'] - n['freehuge_gb']     
                tmpnode.total_pods  = n['totalpods']    
                for nic in n['nicstats']:
                    nicinfo  = tmpnode.nic_info.add()                    
                    nicinfo.used_rx = nic[0]
                    nicinfo.used_tx = nic[1]

        except Empty as e:
            self.logger.error(f'Failed to get a response from NHD scheduler for Node stats query: {e}')
        
        return rsp


