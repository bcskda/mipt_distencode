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
    def build_cmdline(cls, project_path, preset_path, result_path):
        preset = dict()
        with open(preset_path, 'r') as preset_file:
            preset = json.load(preset_file)
        cmdline = list()
        cmdline.append(Config.melt_path)
        cmdline.append(project_path)
        cmdline.extend(cls._build_consumer(preset, result_path))
        return cmdline
