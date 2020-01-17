import sys

sys.path.insert(0, '../')
import grpc
from nhd.proto import nhd_stats_pb2
from nhd.proto import nhd_stats_pb2_grpc

SERVER_INFO = 'localhost:31044'

if __name__ == "__main__":
    print('Starting RPC tests')

    with grpc.insecure_channel(SERVER_INFO) as channel:
        stub = nhd_stats_pb2_grpc.NHDControlStub(channel)

        nodes = stub.GetBasicNodeStats(nhd_stats_pb2.Empty())
        print(nodes)

