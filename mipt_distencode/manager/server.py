from concurrent import futures

import grpc

from mipt_distencode.manager.manager_pb2_grpc import add_ManagerServicer_to_server
from mipt_distencode.manager.manager import ManagerServicer
from mipt_distencode.pb_common import add_endpoint_to_server


class ManagerServer:
    def __init__(self, endpoint, secure=False):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        add_ManagerServicer_to_server(
            ManagerServicer(), self.server)
        add_endpoint_to_server(self.server, endpoint, secure)

    def start(self):
        self.server.start()

    def wait_for_termination(self):
        self.server.wait_for_termination()
