import logging
from concurrent import futures

import grpc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from mipt_distencode.worker.worker_pb2_grpc import add_WorkerServicer_to_server
from mipt_distencode.worker.worker import WorkerServicer
from mipt_distencode.pb_common import add_endpoint_to_server
from mipt_distencode.config import Config


class WorkerServer:
    def __init__(self, endpoint, secure=False):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        self.servicer = WorkerServicer()
        add_WorkerServicer_to_server(self.servicer, self.server)
        add_endpoint_to_server(self.server, endpoint, secure)

    def start(self):
        self.server.start()

    def post_start(self):
        self.servicer.post_start()

    def wait_for_termination(self):
        self.server.wait_for_termination()

    def pre_stop(self):
        self.servicer.pre_stop()

    def stop(self):
        self.server.stop(grace=30)
        self.servicer.join()

def server_main():
    worker_server = WorkerServer(f'{Config.identity}:50053', secure=True)
    worker_server.start()
    worker_server.post_start()
    try:
        worker_server.wait_for_termination()
    except KeyboardInterrupt:
        logging.info('Stopped by KeyboardInterrupt')
    worker_server.pre_stop()
    worker_server.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    server_main()
