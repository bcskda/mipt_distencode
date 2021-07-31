import logging
import multiprocessing

import grpc

from mipt_distencode.jobs_pb2 import JobId, MeltJob, JobResult
from mipt_distencode.mgmt_messages_pb2 import WorkerSelfAnnouncement, WorkerState
from mipt_distencode.pb_common import PeerIdentityMixin
from mipt_distencode.worker import worker_pb2_grpc
import mipt_distencode.manager.make_client as make_manager_client


class WorkerServicer(worker_pb2_grpc.WorkerServicer, PeerIdentityMixin):
    class ChildStopPill:
        pass

    def __init__(self, config):
        super().__init__()
        self.config = config
        self.mlt_presets = self._load_mlt_presets(self.config)
        self.process_count = 1
        self.process_pool = multiprocessing.Pool(process_count)
        self.result_queue = multiprocessing.JoinableQueue()
        self.state = WorkerState.ACTIVE
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def PostMeltJob(self, job, context):
        peer = self.identify_peer(context)
        if self.state == WorkerState.STOPPING:
            context.abort(grpc.StatusCode.UNAVAILABLE, 'Worker is stopping')
        preset = self.mlt_presets[job.encodingPresetName]
        self.process_pool.apply_async(self.)
        return job.id

    def post_start(self):
        self._report_state(WorkerState.ACTIVE)
        pass

    def pre_stop(self):
        self._report_state(WorkerState.STOPPING)
        self.state = WorkerState.STOPPING
        pass

    def join():
        """Graceful shutdown"""
        for _ in range(self.process_count):
            self.queue.put(ChildStopPill())
        self.queue.join()
        self.process_pool.close()
        self.process_pool.join()

    def _report_state(self, state):
        client = make_manager_client(
            endpoint=self.config.manager_address, secure=True)
        message = WorkerSelfAnnounce(
            hostname=self.config.identity, newState=state)
        client.WorkerSelfAnnounce(message)

    @staticmethod
    def _load_presets(config) -> 'Dict[str, Path]':
        return {
            'default': 'default_preset_description'
        }

    def _call_melt(project_path, preset):
        logger = multiprocessing.log_to_stderr() \
            .getChild(__name__).getChild(f'Process-{os.getpid()}')
        logger.setLevel(logging.DEBUG)
        logger.info(
            '_call_melt: project_path=%s, preset=%s', project_path, preset)
