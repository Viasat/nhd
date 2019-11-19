import sys

sys.path.insert(0, '../obj/')
import grpc
import nhd_stats_pb2
import nhd_stats_pb2_grpc

SERVER_INFO = 'localhost:45655'

if __name__ == "__main__":
    print('Starting RPC tests')

    with grpc.insecure_channel(SERVER_INFO) as channel:
        stub = nhd_stats_pb2_grpc.NHDControlStub(channel)

        nodes = stub.GetBasicNodeStats(nhd_stats_pb2.Empty())
        print(nodes)

