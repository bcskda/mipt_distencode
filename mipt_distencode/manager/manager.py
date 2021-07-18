import logging

import grpc

from mipt_distencode.mgmt_messages_pb2 import WorkerSelfAnnouncement, WorkerState
from mipt_distencode.manager import manager_pb2_grpc
from mipt_distencode.pb_common import PeerIdentityMixin


class ManagerServicer(manager_pb2_grpc.ManagerServicer, PeerIdentityMixin):
    def __init__(self):
        super().__init__()
        self.workers = set()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def WorkerAnnounce(self, announcement, context):
        peer_id = self.identify_peer(context)
        if peer_id != announcement.hostname:
            message = 'Peer {0} identified as {1} is not {2}'.format(
                context.peer(), peer_id, announcement.hostname)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, message)

        if announcement.newState == WorkerState.ACTIVE:
            return self.add_worker(announcement, context)
        elif announcement.newState == WorkerState.STOPPING:
            return self.remove_worker(announcement, context)
        else:
            message = f'Unknown WorkerState: {announcement.newState}'
            self.logger.error(message)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, message)

    def add_worker(self, announcement, context):
        worker = announcement.hostname
        if worker in self.workers:
            message = f'Duplicate worker: {worker}'
            self.logger.info(message)
            context.abort(grpc.StatusCode.ALREADY_EXISTS, message)
        else:
            self.workers.add(worker)
            self.logger.info('Registered worker: %s', worker)
        return announcement
    
    def remove_worker(self, announcement, context):
        worker = announcement.hostname
        if worker in self.workers:
            self.workers.remove(worker)
            self.logger.info('Unregistered worker: %s', worker)
            return announcement
        else:
            message = f'Worker not found: {worker}'
            self.logger.warning(message)
            context.abort(grpc.StatusCode.NOT_FOUND, message)

