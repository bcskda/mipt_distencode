import logging
import multiprocessing

import grpc
from google.protobuf.text_format import MessageToString

from mipt_distencode.config import Config
from mipt_distencode.jobs_pb2 import JobId, MeltJob, MeltJobResult
from mipt_distencode.mgmt_messages_pb2 import WorkerSelfAnnouncement, WorkerState
from mipt_distencode.pb_common import PeerIdentityMixin, make_channel
from mipt_distencode.worker import worker_pb2_grpc
from mipt_distencode.worker.melt import MeltHelper
from mipt_distencode.manager.client import make_client as make_manager_client


class WorkerServicer(worker_pb2_grpc.WorkerServicer, PeerIdentityMixin):
    PROCESS_COUNT = 1

    def __init__(self):
        super().__init__()
        self.mlt_presets = self._load_melt_presets()
        self.process_count = 1
        self.process_pool = multiprocessing.Pool(self.PROCESS_COUNT)
        self.state = WorkerState.ACTIVE
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def PostMeltJob(self, job, context):
        peer = self.identify_peer(context)
        if self.state == WorkerState.STOPPING:
            context.abort(grpc.StatusCode.UNAVAILABLE, 'Worker is stopping')
        for field in ['projectPath', 'encodingPresetName', 'resultPath']:
            if not job.HasField(field):
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, f'Missing field: {field}')
        preset = self.mlt_presets.get(job.encodingPresetName)
        if preset is None:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f'Unknown preset: {job.encodingPresetName}')
        self.logger.info(
            'Accepted job [%s] from [%s]',
            MessageToString(job, as_one_line=true), peer)
        self.process_pool.apply_async(self._call_melt, args=[job, preset])
        return job.id

    def post_start(self):
        self._report_state(WorkerState.ACTIVE)
        pass

    def pre_stop(self):
        self._report_state(WorkerState.STOPPING)
        self.state = WorkerState.STOPPING
        pass

    def join(self):
        self.process_pool.close()
        self.process_pool.join()

    def _report_state(self, state):
        client = make_manager_client(
            make_channel(f'{Config.manager_address}:50052', secure=True))
        message = WorkerSelfAnnouncement(
            hostname=Config.identity, newState=state)
        client.WorkerAnnounce(message)
        self.logger.info('Reported state: %s', MessageToString(message, as_one_line=True))

    @staticmethod
    def _load_melt_presets() -> 'Dict[str, Path]':
        return {
            'default': 'default_preset_description'
        }

    def _call_melt(job, preset):
        logger = multiprocessing.log_to_stderr() \
            .getChild(__name__).getChild(f'Process-{os.getpid()}')
        logger.setLevel(logging.DEBUG)
        cmdline = MeltHelper.build_cmdline(job.projectPath, preset, job.resultPath)
        logger.info('job=%s cmdline: %s', job.id, cmdline)
        client = make_manager_client(
            make_channel(f'{Config.manager_address}:50052', secure=True))
        message = MeltJobResult(
            id=job.id,
            success=False,
            error='Dry run'.encode('utf-8'),
            log=cmdline.encode('utf-8'))
        resp = client.PostMeltJobResult(message)
        logger.info(
            'job=%s report response: %s',
            job.id, MessageToString(resp, as_one_line=True))
