from os.path import join, basename, splitext

import luigi

from ..config import bioluigi
from ..scheduled_external_program import ScheduledExternalProgramTask

cfg = bioluigi()

class GenerateReport(ScheduledExternalProgramTask):
    task_namespace = 'fastqc'

    input_file = luigi.Parameter()
    output_dir = luigi.Parameter()

    temp_dir = luigi.OptionalParameter(positional=False, significant=False,
                                       description='Temporary directory to use for FastQC intermediary files.')

    @staticmethod
    def gen_report_basename(fastq_path):
        sample_name, ext = splitext(basename(fastq_path))
        if ext == '.gz':
            sample_name, ext = splitext(sample_name)
        return '{}_fastqc.html'.format(sample_name)

    def program_args(self):
        args = [cfg.fastqc_bin,
                '--outdir', self.output_dir]
        if self.temp_dir:
            args.extend(['--dir', self.temp_dir])
        args.append(self.input_file)
        return args

    def output(self):
        return luigi.LocalTarget(join(self.output_dir, self.gen_report_basename(self.input_file)))
