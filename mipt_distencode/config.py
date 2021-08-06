import os


class Config:
    identity = os.environ['DISTENC_IDENTITY']
    db = os.environ.get('DISTENC_DB')
    melt_path = os.environ.get('DISTENC_MELT')
    melt_preset_dir = os.environ.get('DISTENC_MELT_PRESET_DIR')
    manager_address = os.environ.get('DISTENC_MANAGER_ADDR')
