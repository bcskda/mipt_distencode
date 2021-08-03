import io
import json
import logging
import multiprocessing
import os
import sys
import traceback

import grpc
from google.protobuf.text_format import MessageToString

from mipt_distencode.config import Config
from mipt_distencode.jobs_pb2 import JobId, MeltJob, MeltJobResult
from mipt_distencode.mgmt_messages_pb2 import WorkerSelfAnnouncement, WorkerState
from mipt_distencode.pb_common import PeerIdentityMixin, make_channel
from mipt_distencode.worker import worker_pb2_grpc
from mipt_distencode.worker.melt import MeltHelper
from mipt_distencode.manager.client import make_client as make_manager_client


class JobExecutionError(Exception):
    def __init__(self, jobId, message, stacktrace):
        self.jobId = jobId
        self.message = message
        self.stacktrace = stacktrace


class WorkerServicer(worker_pb2_grpc.WorkerServicer, PeerIdentityMixin):
    PROCESS_COUNT = 1

    def __init__(self):
        super().__init__()
        self.melt_presets = self._load_melt_presets()
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
        preset = self.melt_presets.get(job.encodingPresetName)
        if preset is None:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f'Unknown preset: {job.encodingPresetName}')
        self.logger.info(
            'Accepted job [%s] from [%s]',
            MessageToString(job, as_one_line=True), peer)
        self.process_pool.apply_async(
            self._call_melt, args=[job, preset],
            callback=self._report_job_success, error_callback=self._report_job_error)
        return job.id

    def post_start(self):
        self._report_state(WorkerState.ACTIVE)

    def pre_stop(self):
        self._report_state(WorkerState.STOPPING)
        self.state = WorkerState.STOPPING

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

    def _report_job_success(self, result):
        job, cmdline = result
        message = MeltJobResult(
            id=job.id,
            success=True,
            resultPath=job.resultPath,
            error='Dry run'.encode('utf-8'),
            log=str(cmdline).encode('utf-8'))
        self._report_job_result(message)

    def _report_job_error(self, e: JobExecutionError):
        emesg = f'Unhandled exception in worker thread: {e.message}'
        self.logger.exception(emesg)

        message = MeltJobResult(
            id=JobId(id=e.jobId),
            success=False,
            error=emesg.encode('utf-8'),
            log=e.stacktrace.encode('utf-8'))
        self._report_job_result(message)

    def _report_job_result(self, message):
        client = make_manager_client(
            make_channel(f'{Config.manager_address}:50052', secure=True))
        resp = client.PostMeltJobResult(message)
        self.logger.info(
            'Job id=%s report response: %s',
            message.id.id, MessageToString(resp, as_one_line=True))

    @staticmethod
    def _load_melt_presets() -> 'Dict[str, Dict]':
        SUFFIX = '.json'
        presets = dict()
        for entry in os.listdir(Config.melt_preset_dir):
            if not entry.endswith(SUFFIX):
                continue
            name = entry.rstrip(SUFFIX)
            path = f'{Config.melt_preset_dir}/{entry}'
            with open(path) as pfile:
                presets[name] = json.load(pfile)
        return presets

    @staticmethod
    def _call_melt(job, preset):
        try:
            logger = multiprocessing.log_to_stderr() \
                .getChild(__name__).getChild(f'Process-{os.getpid()}')
            logger.setLevel(logging.DEBUG)
            cmdline = MeltHelper.build_cmdline(job.projectPath, preset, job.resultPath)
            logger.info('job=%s cmdline: %s', job.id.id, cmdline)
            return job, cmdline
        except Exception as e:
            with io.StringIO() as string_io:
                traceback.print_exc(file=string_io)
                trace = string_io.getvalue()
                raise JobExecutionError(job.id.id, str(e), trace)
