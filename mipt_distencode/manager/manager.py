import collections
import logging
import time
from threading import Lock, Thread

import grpc

from mipt_distencode import jobs_pb2, mgmt_messages_pb2
from mipt_distencode.manager import manager_pb2_grpc
from mipt_distencode.manager.db_models import (
    MeltJobHandle, MeltJobState, Session, WorkerRecord, WorkerState
)
from mipt_distencode.pb_common import PeerIdentityMixin
from mipt_distencode.worker.client import make_client as make_worker_client


class LockHolder:
    def __init__(self, lock):
        self.lock = lock

    def __enter__(self):
        self.lock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.lock.release()
        return False


class Pinger:
    PING_TIMEOUT = 10

    def __init__(self, workers, lock):
        self._workers = workers
        self._lock = lock
        self._logger = logging.getLogger('pinger')
        self._logger.setLevel(logging.INFO)
        self._stopping = False
        self._thread = Thread(target=self._loop)

    def start(self):
        self._thread.start()

    def stop(self):
        with LockHolder(self._lock):
            self._stopping = True
        self._thread.join()

    def _loop(self):
        while True:
            with LockHolder(self._lock):
                if self._stopping:
                    break
                removal = list(filter(self._is_dead, self._workers))
                self._remove(removal)
            time.sleep(self.PING_TIMEOUT)

    def _is_dead(self, hostname):
        try:
            worker_client = make_worker_client(
                endpoint=f'{hostname}:50053', secure=True)
            worker_client.Ping(mgmt_messages_pb2.PingMesg())
            return False
        except Exception:
            return True

    def _remove(self, removal):
        with Session() as session:
            for hostname in removal:
                try:
                    jobs = MeltJobHandle.mark_retry_by_worker(hostname, session)
                    self._workers.remove(hostname)
                    session.delete(WorkerRecord.by_hostname(hostname, session))
                    session.commit()
                    self._logger.info('Removed worker %s, pending jobs: %s', hostname, jobs)
                except Exception as e:
                    self._logger.warning('Failed to remove worker %s: %s', hostname, e)


class ManagerServicer(manager_pb2_grpc.ManagerServicer, PeerIdentityMixin):
    """Not thread-safe, use only with single-threaded RPC server"""
    def __init__(self):
        super().__init__()
        self.workers = collections.deque()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self._lock = Lock()
        self._reset_workers()  # TODO request state from workers
        self._pinger = Pinger(self.workers, self._lock)

    def post_start(self):
        self._pinger.start()

    def pre_stop(self):
        self._pinger.stop()

    def WorkerAnnounce(self, announcement, context):
        with self._synced():
            peer_id = self.identify_peer(context)
            state = WorkerState.from_proto(announcement.newState)
            if peer_id != announcement.hostname:
                message = 'Peer {0} identified as {1} is not {2}'.format(
                    context.peer(), peer_id, announcement.hostname)
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, message)

            if state == WorkerState.ACTIVE:
                return self._add_worker(announcement, context)
            elif state == WorkerState.STOPPING:
                return self._remove_worker(announcement, context)
            else:
                message = f'Unknown WorkerState: {announcement.newState}'
                self.logger.error(message)
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, message)

    def PostMeltJob(self, proto, context):
        with self._synced():
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
                job_handle.mark_in_progress(worker, session)
                session.commit()
                return accepted_id

    def PostMeltJobResult(self, proto, context):
        with self._synced():
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
                    return proto
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

    def _add_worker(self, proto, context):
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

    def _remove_worker(self, proto, context):
        with Session() as session:
            worker_record = WorkerRecord.lookup_from_proto(proto, session)
            if worker_record is None:
                message = f'Worker not found: {proto.hostname}, peer={context.peer()}'
                self.logger.error(message)
                context.abort(grpc.StatusCode.NOT_FOUND, message)
            host = worker_record.hostname
            session.delete(worker_record)
            session.commit()
            self.workers.remove(host)
            self.logger.info('Successfully unregistered worker: %s', host)
        return proto

    def _reset_workers(self):
        self.logger.info('Resetting worker info at startup...')
        with Session() as session:
            for worker in session.query(WorkerRecord).all():
                self.logger.warning('Found: %s', worker)
                session.delete(worker)
            session.commit()
        self.logger.info('Done')

    def _choose_worker(self):
        chosen = self.workers.popleft()
        self.workers.append(chosen)
        return chosen

    def _synced(self):
        return LockHolder(self._lock)
