import collections
import logging

import grpc

from mipt_distencode import jobs_pb2, mgmt_messages_pb2
from mipt_distencode.manager import manager_pb2_grpc
from mipt_distencode.manager.db_models import (
    MeltJobHandle, MeltJobState, Session, WorkerRecord, WorkerState
)
from mipt_distencode.pb_common import PeerIdentityMixin
from mipt_distencode.worker.client import make_client as make_worker_client


class ManagerServicer(manager_pb2_grpc.ManagerServicer, PeerIdentityMixin):
    """Not thread-safe, use only with single-threaded RPC server"""
    def __init__(self):
        super().__init__()
        self.workers = collections.deque()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def WorkerAnnounce(self, announcement, context):
        peer_id = self.identify_peer(context)
        state = WorkerState.from_proto(announcement.newState)
        if peer_id != announcement.hostname:
            message = 'Peer {0} identified as {1} is not {2}'.format(
                context.peer(), peer_id, announcement.hostname)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, message)

        if state == WorkerState.ACTIVE:
            return self.add_worker(announcement, context)
        elif state == WorkerState.STOPPING:
            return self.remove_worker(announcement, context)
        else:
            message = f'Unknown WorkerState: {announcement.newState}'
            self.logger.error(message)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, message)

    def PostMeltJob(self, proto, context):
        peer_id = self.identify_peer(context)
        if proto.HasField('id'):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f'Id must not be provided')
        with Session() as session:
            job_handle = MeltJobHandle.new_from_proto(proto, session)
            worker = self._choose_worker()
            self.logger.info(f'Accepted job: {job_handle}, chosen worker: {worker}')
            worker_client = make_worker_client(
                endpoint=f'{worker}:50053', secure=True)
            accepted_id = worker_client.PostMeltJob(job_handle.proto_job())
            assert accepted_id.id == job_handle.id
            job_handle.state = MeltJobState.IN_PROGRESS
            session.commit()
            return accepted_id

    def PostMeltJobResult(self, proto, context):
        peer_id = self.identify_peer(context)
        with Session() as session:
            job_handle = MeltJobHandle.lookup_from_proto(proto, session)
            if job_handle is None:
                message = f'Job id={proto.id.id} not found'
                self.logger.error(message)
                context.abort(grpc.StatusCode.NOT_FOUND, message)
            if job_handle.state != MeltJobState.IN_PROGRESS:
                message = f'Job id={job_handle.id} is in invalid state {job_handle.state}'
                self.logger.error(message)
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, message)
            elif not proto.success:
                job_handle.state = MeltJobState.FAILED
                session.commit()
                message = 'Job id={} failed, error: {}, log: {}'.format(
                    job_handle.id, proto.error, proto.log)
                self.logger.warning(message)
            elif not proto.HasField('resultPath'):
                message = f'Job id={job_handle.id}: missing field: resultPath'
                self.logger.error(message)
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, message)
            else:
                job_handle.state = MeltJobState.VERIFICATION
                session.commit()
                self.logger.info(
                    'Job id=%s successfully finished: error=%s, log=%s, result=%s',
                    job_handle.id, proto.error, proto.log, proto.resultPath)
                return proto

    def add_worker(self, proto, context):
        with Session() as session:
            worker_record = WorkerRecord.lookup_from_proto(proto, session)
            if worker_record is None:
                worker_record = WorkerRecord.new_from_proto(proto, session)
            else:
                message = f'Duplicate worker: {worker_record.hostname}, peer={context.peer()}'
                self.logger.warning(message)
            self.workers.append(worker_record.hostname)
            self.logger.info('Successfully registered worker: %s', worker_record.hostname)
        return proto

    def remove_worker(self, proto, context):
        with Session() as session:
            worker_record = WorkerRecord.lookup_from_proto(proto, session)
            if worker_record is None:
                message = f'Worker not found: {worker_record.hostname}, peer={context.peer()}'
                self.logger.error(message)
                context.abort(grpc.StatusCode.NOT_FOUND, message)
            host = worker_record.hostname
            session.delete(worker_record)
            session.commit()
            self.workers.remove(host)
            self.logger.info('Successfully unregistered worker: %s', host)
        return proto

    def _choose_worker(self):
        chosen = self.workers.popleft()
        self.workers.append(chosen)
        return chosen
