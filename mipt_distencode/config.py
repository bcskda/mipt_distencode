import os


class Config:
    identity = os.environ['DISTENC_IDENTITY']
    db = os.environ.get('DISTENC_DB')
    melt_path = os.environ.get('DISTENC_MELT')
    manager_address = os.environ.get('DISTENC_MANAGER_ADDR')
