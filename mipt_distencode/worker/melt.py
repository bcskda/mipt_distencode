import json

from mipt_distencode.config import Config


class MeltHelper:
    @classmethod
    def _build_consumer(cls, preset, target_path):
        cmdline = list()
        for section, options in preset.items():
            for key, value in options.items():
                cmdline.extend([f'-{key}', f'{value}'])
        cmdline.extend(['-target', f'{target_path}'])
        return cmdline

    @classmethod
    def build_cmdline(cls, project_path, preset, result_path):
        cmdline = list()
        cmdline.append(Config.melt_path)
        cmdline.append(project_path)
        cmdline.extend(cls._build_consumer(preset, result_path))
        return cmdline
