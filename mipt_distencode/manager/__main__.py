import logging
import sys

from google.protobuf.text_format import MessageToString

from mipt_distencode.manager.server import ManagerServer
from mipt_distencode.manager.client import make_client
from mipt_distencode.mgmt_messages_pb2 import WorkerState, WorkerSelfAnnouncement


def server_main():
    manager_server = ManagerServer()
    manager_server.start()
    try:
        manager_server.wait_for_termination()
    except KeyboardInterrupt:
        logging.info('Stopped by KeyboardInterrupt')


def client_main():
    client = make_client()
    while True:
        try:
            newState, hostname = input().split()
            newState = WorkerState.Value(newState)
            announcement = WorkerSelfAnnouncement(
                newState=newState, hostname=hostname)
            response = client.WorkerAnnounce(announcement)
            logging.info(
                'Response: %s',
                MessageToString(response, as_one_line=True))
        except (KeyboardInterrupt, EOFError):
            break


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} (server|client)")
        sys.exit(1)
    if sys.argv[1] == 'server':
        logging.info('Running server')
        server_main()
    elif sys.argv[1] == 'client':
        logging.info('Running client')
        client_main()
    else:
        raise ValueError(f'Unknown operation mode: {sys.argv[1]}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
