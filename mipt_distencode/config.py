import os


class Config:
    identity = os.environ['DISTENC_IDENTITY']
    db = os.environ['DISTENC_DB']
