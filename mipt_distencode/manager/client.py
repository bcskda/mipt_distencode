import logging
import sys

import grpc
from google.protobuf.text_format import MessageToString

from mipt_distencode.manager.manager_pb2_grpc import ManagerStub
from mipt_distencode import jobs_pb2, mgmt_messages_pb2
from mipt_distencode.pb_common import make_channel


def make_client(channel=None, **kwargs) -> ManagerStub:
    if channel and kwargs:
        raise ValueError('Specify either channel or construction kwargs')
    if channel is None:
        channel = make_channel(**kwargs)
    return ManagerStub(channel)


def client_main(argv):
    peer, command, args = argv[0], argv[1], argv[2:]
    channel = make_channel(f'{peer}:50052', secure=True)
    client = make_client(channel)
    if command == 'WorkerSelfAnnouncement':
        newState, hostname = args
        newState = mgmt_messages_pb2.WorkerState.Value(newState)
        announcement = mgmt_messages_pb2.WorkerSelfAnnouncement(
            newState=newState, hostname=hostname)
        response = client.WorkerAnnounce(announcement)
        assert response.newState == newState
        assert response.hostname == hostname
    elif command == 'PostMeltJob':
        projectPath, encodingPresetName = args
        job = jobs_pb2.MeltJob(
            projectPath=projectPath,
            encodingPresetName=encodingPresetName)
        response = client.PostMeltJob(job)
    elif command == 'PostMeltJobResult':
        jobId, success, error, log, result_path = args
        jobId = int(jobId)
        success = bool(success)
        error = error.encode('utf-8')
        log = log.encode('utf-8')
        jobResult = jobs_pb2.MeltJobResult(
            id=jobs_pb2.JobId(id=jobId),
            success=success,
            error=error,
            log=log,
            resultPath=result_path)
        response = client.PostMeltJobResult(jobResult)
        assert response == jobResult
    else:
        raise ValueError('Unknown command:', command)
    print('Response:', MessageToString(response, as_one_line=True))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    client_main(sys.argv[1:])
