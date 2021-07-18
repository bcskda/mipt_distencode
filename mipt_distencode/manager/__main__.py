import logging
import sys

from google.protobuf.text_format import MessageToString

from mipt_distencode.config import Config
from mipt_distencode.manager.client import make_client
from mipt_distencode.manager.server import ManagerServer
from mipt_distencode.jobs_pb2 import MLTJob
from mipt_distencode.mgmt_messages_pb2 import WorkerState, WorkerSelfAnnouncement
from mipt_distencode.pb_common import make_channel


def server_main(argv):
    manager_server = ManagerServer(f'{Config.identity}:50052', secure=True)
    manager_server.start()
    try:
        manager_server.wait_for_termination()
    except KeyboardInterrupt:
        logging.info('Stopped by KeyboardInterrupt')


def client_main(argv):
    peer = argv[0]
    channel = make_channel(f'{peer}:50052', secure=True)
    client = make_client(channel)
    while True:
        try:
            command, args = input().split(maxsplit=1)
            if command == 'WorkerSelfAnnouncement':
                newState, hostname = args.split()
                newState = WorkerState.Value(newState)
                announcement = WorkerSelfAnnouncement(
                    newState=newState, hostname=hostname)
                response = client.WorkerAnnounce(announcement)
            elif command == 'PostMLTJob':
                projectPath, encodingPresetName = args.split()
                job = MLTJob(
                    projectPath=projectPath,
                    encodingPresetName=encodingPresetName)
                response = client.PostMLTJob(job)
            else:
                logging.warning('Unknown command: %s', command)
                continue
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
        server_main(sys.argv[2:])
    elif sys.argv[1] == 'client':
        logging.info('Running client')
        client_main(sys.argv[2:])
    else:
        raise ValueError(f'Unknown operation mode: {sys.argv[1]}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
