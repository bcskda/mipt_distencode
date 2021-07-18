import collections
import logging

import grpc

from mipt_distencode.jobs_pb2 import JobId, MLTJob
from mipt_distencode.mgmt_messages_pb2 import WorkerSelfAnnouncement, WorkerState
from mipt_distencode.manager import manager_pb2_grpc
from mipt_distencode.pb_common import PeerIdentityMixin


class ManagerServicer(manager_pb2_grpc.ManagerServicer, PeerIdentityMixin):
    """Not thread-safe, use only with single-threaded RPC server"""
    def __init__(self):
        super().__init__()
        self.next_job_id = 1
        self.workers = collections.deque()
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

    def PostMLTJob(self, job, context):
        peer_id = self.identify_peer(context)
        job_id = self._register_job(job, context)
        worker = self._rotate_workers()
        job_str = str(job).replace('\n', ' ')
        self.logger.info(f'Accepted job: id={job_id}, params={job_str}, assigned worker {worker}')
        return JobId(id=job_id)

    def add_worker(self, announcement, context):
        worker = announcement.hostname
        if worker in self.workers:
            message = f'Duplicate worker: {worker}, peer={context.peer()}'
            self.logger.warning(message)
        else:
            self.workers.append(worker)
            self.logger.info('Registered worker: %s', worker)
        return announcement
    
    def remove_worker(self, announcement, context):
        worker = announcement.hostname
        if worker in self.workers:
            self.workers.remove(worker)
            self.logger.info('Unregistered worker: %s', worker)
            return announcement
        else:
            message = f'Worker not found: {worker}, peer={context.peer()}'
            self.logger.error(message)
            context.abort(grpc.StatusCode.NOT_FOUND, message)

    def _register_job(self, job, context):
        if job.HasField('id'):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f'Id must not be provided')
        this_id = self.next_job_id
        self.next_job_id += 1
        return this_id

    def _rotate_workers(self):
        chosen = self.workers.popleft()
        self.workers.append(chosen)
        return chosen
