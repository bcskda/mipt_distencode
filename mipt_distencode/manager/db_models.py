import enum

from sqlalchemy import Column, ForeignKey, MetaData, Sequence
from sqlalchemy import DateTime, Integer, String, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from mipt_distencode import jobs_pb2, mgmt_messages_pb2


metadata = MetaData()
Base = declarative_base(metadata=metadata)
Session = sessionmaker()


class WorkerState(enum.Enum):
    ACTIVE = 0
    STOPPING = 1

    @classmethod
    def from_proto(cls, proto: mgmt_messages_pb2.WorkerState):
        return cls(int(proto))

    def proto(self) -> mgmt_messages_pb2.WorkerState:
        return getattr(mgmt_messages_pb2.WorkerState, self.name)


class WorkerRecord(Base):
    __tablename__ = 'WorkerRecord'
    hostname = Column(String, primary_key=True)
    state = Column(Enum(WorkerState), nullable=False)

    def __repr__(self):
        attrs = {
            'hostname': self.hostname,
            'state': self.state
        }
        return f'WorkerRecord{repr(attrs)}'

    @classmethod
    def new_from_proto(cls, proto: mgmt_messages_pb2.WorkerSelfAnnouncement,
                           session: Session) -> 'WorkerRecord':
        attr_mappers = {
            'hostname': lambda p: p.hostname,
            'state': lambda p: WorkerState.from_proto(p.newState)
        }
        attrs = {
            attr: mapper(proto) for attr, mapper in attr_mappers.items()
        }
        orm = cls(**attrs)
        session.add(orm)
        session.commit()
        return orm

    @classmethod
    def by_hostname(cls, hostname: str, session: Session) -> 'WorkerRecord':
        return session.query(cls) \
            .filter(cls.hostname == hostname) \
            .one_or_none()

    @classmethod
    def lookup_from_proto(cls, proto: mgmt_messages_pb2.WorkerSelfAnnouncement,
                           session: Session) -> 'WorkerRecord':
        return cls.by_hostname(proto.hostname, session)


class MeltJobState(enum.Enum):
    ACCEPTED = 1
    IN_PROGRESS = 2
    WAITING_RETRY = 3
    FAILED = 4
    VERIFICATION = 5
    FINISHED = 6


class MeltJobHandle(Base):
    __tablename__ = 'MeltJobHandle'
    id = Column(Integer, Sequence('MeltJob_id_seq'),
                        primary_key=True)
    projectPath = Column(String, nullable=False)
    encodingPresetName = Column(String, nullable=False)
    resultPath = Column(String, nullable=False)
    state = Column(Enum(MeltJobState), nullable=False)
    worker_hostname = Column(String, nullable=True)

    def __repr__(self):
        attrs = {
            'id': self.id,
            'projectPath': self.projectPath,
            'encodingPresetName': self.encodingPresetName,
            'resultPath': self.resultPath,
            'state': self.state,
            'worker_hostname': self.worker_hostname
        }
        return f'MeltJobHandle{repr(attrs)}'

    @classmethod
    def new_from_proto(cls, proto: jobs_pb2.MeltJob,
                          session: Session) -> 'MeltJobHandle':
        attr_mappers = {
            'projectPath': lambda p: p.projectPath,
            'encodingPresetName': lambda p: p.encodingPresetName,
            'resultPath': lambda p: p.resultPath
        }
        new_attrs = {
            attr: mapper(proto) for attr, mapper in attr_mappers.items()
        }
        new_attrs['state'] = MeltJobState.ACCEPTED
        orm = cls(**new_attrs)
        session.add(orm)
        session.commit()
        return orm

    @classmethod
    def lookup_from_proto(cls, proto: jobs_pb2.MeltJob,
                          session: Session) -> 'MeltJobHandle':
        return session.query(cls) \
            .filter(cls.id == proto.id.id) \
            .one_or_none()

    @classmethod
    def mark_retry_by_worker(cls, hostname: str, session: Session):
        jobs = session.query(cls) \
            .filter(
                cls.worker_hostname == hostname,
                cls.state.in_({MeltJobState.ACCEPTED, MeltJobState.IN_PROGRESS}))
        jobs = list(jobs)
        for job in jobs:
            job.state = MeltJobState.WAITING_RETRY
        return jobs

    def mark_in_progress(self, worker_hostname: str, session: Session):
        self.state = MeltJobState.IN_PROGRESS
        self.worker_hostname = worker_hostname

    def proto_job(self) -> jobs_pb2.MeltJob:
        attrs = {
            'id': jobs_pb2.JobId(id=self.id),
            'projectPath': self.projectPath,
            'encodingPresetName': self.encodingPresetName,
            'resultPath': self.resultPath
        }
        return jobs_pb2.MeltJob(**attrs)
