import logging
import sys

import grpc
from google.protobuf.text_format import MessageToString

from mipt_distencode.worker.worker_pb2_grpc import WorkerStub
from mipt_distencode import jobs_pb2, mgmt_messages_pb2
from mipt_distencode.pb_common import make_channel


def make_client(channel=None, **kwargs) -> WorkerStub:
    if channel and kwargs:
        raise ValueError('Specify either channel or construction kwargs')
    if channel is None:
        channel = make_channel(**kwargs)
    return WorkerStub(channel)


def client_main(argv):
    peer, command, args = argv[0], argv[1], argv[2:]
    channel = make_channel(f'{peer}:50053', secure=True)
    client = make_client(channel)
    if command == 'PostMeltJob':
        jobId, projectPath, encodingPresetName, resultPath = args
        jobId = int(jobId)
        job = jobs_pb2.MeltJob(
            id=jobs_pb2.JobId(id=jobId),
            projectPath=projectPath,
            encodingPresetName=encodingPresetName,
            resultPath=resultPath)
        response = client.PostMeltJob(job)
    else:
        raise ValueError('Unknown command:', command)
    print('Response:', MessageToString(response, as_one_line=True))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    client_main(sys.argv[1:])
