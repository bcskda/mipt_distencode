import grpc

from mipt_distencode.manager.manager_pb2_grpc import ManagerStub
from mipt_distencode.pb_common import make_channel


def make_client(channel, **kwargs) -> ManagerStub:
    if channel and kwargs:
        raise ValueError('Specify either channel or construction kwargs')
    if channel is None:
        channel = make_channel(**kwargs)
    return ManagerStub(channel)
