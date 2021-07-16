from concurrent import futures

import grpc

from mipt_distencode.manager.manager_pb2_grpc import add_ManagerServicer_to_server
from mipt_distencode.manager.manager import ManagerServicer


class ManagerServer:
    def __init__(self, endpoint='127.0.0.1:50051', secure=False):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        add_ManagerServicer_to_server(
            ManagerServicer(), self.server)
        if secure:
            raise ValueError('ALTS not supported yet')
        self.server.add_insecure_port(endpoint)

    def start(self):
        self.server.start()

    def wait_for_termination(self):
        self.server.wait_for_termination()
