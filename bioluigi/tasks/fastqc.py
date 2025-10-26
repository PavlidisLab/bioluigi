from os import makedirs
from os.path import join, basename, splitext
from typing import Optional

import luigi

from ..config import BioluigiConfig
from ..local_target import LocalTarget
from ..scheduled_external_program import ScheduledExternalProgramTask

cfg = BioluigiConfig()

class GenerateReport(ScheduledExternalProgramTask):
    task_namespace = 'fastqc'

    input_file: str = luigi.Parameter()
    output_dir: str = luigi.Parameter()

    temp_dir: Optional[str] = luigi.OptionalParameter(positional=False, significant=False,
                                                      description='Temporary directory to use for FastQC intermediary files.')

    _tmp_output_dir: Optional[str] = None

    @property
    def resources(self):
        r = super().resources
        r.update({'fastqc_jobs': 1, 'io_jobs': 1})
        return r

    @staticmethod
    def gen_report_basename(fastq_path):
        sample_name, ext = splitext(basename(fastq_path))
        if ext == '.gz':
            sample_name, ext = splitext(sample_name)
        return '{}_fastqc.html'.format(sample_name)

    def program_args(self):
        args = [cfg.fastqc_bin,
                '--outdir', self._tmp_output_dir if self._tmp_output_dir else self.output_dir]
        if self.temp_dir:
            args.extend(['--dir', self.temp_dir])
        args.append(self.input_file)
        return args

    def run(self):
        with LocalTarget(self.output_dir).temporary_path() as self._tmp_output_dir:
            makedirs(self._tmp_output_dir)
            return super().run()

    def output(self):
        return LocalTarget(join(self.output_dir, self.gen_report_basename(self.input_file)))
