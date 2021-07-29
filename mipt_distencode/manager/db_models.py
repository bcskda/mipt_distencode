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
    def lookup_from_proto(cls, proto: mgmt_messages_pb2.WorkerSelfAnnouncement,
                           session: Session) -> 'WorkerRecord':
        return session.query(cls) \
            .filter(cls.hostname == proto.hostname) \
            .one_or_none()


class MLTJobState(enum.Enum):
    ACCEPTED = 1
    IN_PROGRESS = 2
    WAITING_RETRY = 3
    FAILED = 4
    VERIFICATION = 5
    FINISHED = 6


class MLTJobHandle(Base):
    __tablename__ = 'MLTJobHandle'
    id = Column(Integer, Sequence('MLTJob_id_seq'),
                        primary_key=True)
    projectPath = Column(String, nullable=False)
    encodingPresetName = Column(String, nullable=False)
    state = Column(Enum(MLTJobState), nullable=False)

    _PROTO_ATTRS = ['projectPath', 'encodingPresetName']

    def __repr__(self):
        attrs = {
            'id': self.id,
            'projectPath': self.projectPath,
            'encodingPresetName': self.encodingPresetName,
            'state': self.state
        }
        return f'MLTJobHandle {repr(attrs)}'

    @classmethod
    def new_from_proto(cls, proto: jobs_pb2.MLTJob,
                          session: Session) -> 'MLTJobHandle':
        attr_mappers = {
            'projectPath': lambda p: p.projectPath,
            'encodingPresetName': lambda p: p.encodingPresetName
        }
        new_attrs = {
            attr: mapper(proto) for attr, mapper in attr_mappers.items()
        }
        new_attrs['state'] = MLTJobState.ACCEPTED
        orm = cls(**new_attrs)
        session.add(orm)
        session.commit()
        return orm

    @classmethod
    def lookup_from_proto(cls, proto: jobs_pb2.MLTJob,
                          session: Session) -> 'MLTJobHandle':
        return session.query(cls) \
            .filter(cls.id == proto.id.id) \
            .one_or_none()

    def proto_job(self) -> jobs_pb2.MLTJob:
        fields = { attr: getattr(self, attr) for attr in self._PROTO_ATTRS }
        return jobs_pb2.MLTJob(**fields)
