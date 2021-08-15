import logging
from concurrent import futures

import grpc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from mipt_distencode.manager.db_models import Base, Session
from mipt_distencode.manager.manager_pb2_grpc import add_ManagerServicer_to_server
from mipt_distencode.manager.manager import ManagerServicer
from mipt_distencode.pb_common import add_endpoint_to_server
from mipt_distencode.config import Config


class ManagerServer:
    def __init__(self, endpoint, secure=False):
        self.db = create_engine(Config.db)
        Base.metadata.create_all(self.db)
        Session.configure(bind=self.db)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        self.servicer = ManagerServicer()
        add_ManagerServicer_to_server(
            self.servicer, self.server)
        add_endpoint_to_server(self.server, endpoint, secure)

    def start(self):
        self.server.start()

    def post_start(self):
        self.servicer.post_start()

    def pre_stop(self):
        self.servicer.pre_stop()

    def wait_for_termination(self):
        self.server.wait_for_termination()


def server_main():
    manager_server = ManagerServer(f'{Config.identity}:50052', secure=True)
    manager_server.start()
    manager_server.post_start()
    try:
        manager_server.wait_for_termination()
    except KeyboardInterrupt:
        logging.info('Stopped by KeyboardInterrupt')
    manager_server.pre_stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    server_main()
