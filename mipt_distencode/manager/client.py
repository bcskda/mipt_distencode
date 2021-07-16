import grpc

from mipt_distencode.manager.manager_pb2_grpc import ManagerStub


DEFAULT_ENDPOINT = '127.0.0.1:50051'


def make_client(endpoint=None, channel=None, secure=False) -> ManagerStub:
    if endpoint and channel:
        raise ValueError('Specify either endpoint or channel')
    if not endpoint:
        endpoint = DEFAULT_ENDPOINT
    if not channel:
        if secure:
            raise ValueError('ALTS not supported yet')
        channel = grpc.insecure_channel(endpoint)
    return ManagerStub(channel)
