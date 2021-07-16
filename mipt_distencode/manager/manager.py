import logging

import grpc

from mipt_distencode.mgmt_messages_pb2 import WorkerSelfAnnouncement, WorkerState
from mipt_distencode.manager import manager_pb2_grpc


class ManagerServicer(manager_pb2_grpc.ManagerServicer):
    def __init__(self):
        super().__init__()
        self.workers = set()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def WorkerAnnounce(self, announcement, context):
        if announcement.newState == WorkerState.ACTIVE:
            return self.add_worker(announcement, context)
        elif announcement.newState == WorkerState.STOPPING:
            return self.remove_worker(announcement, context)
        else:
            message = f'Unknown WorkerState: {announcement.newState}'
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(message)
            raise ValueError(message)

    def add_worker(self, announcement, context):
        worker = announcement.hostname
        if worker in self.workers:
            message = f'Duplicate worker: {worker}'
            self.logger.info(message)
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(message)
            raise ValueError(message)
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
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            raise ValueError(message)
