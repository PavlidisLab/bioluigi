import datetime
from os.path import join, basename, splitext
import luigi
from ..scheduled_external_program import ScheduledExternalProgramTask
from ..config import bioluigi

cfg = bioluigi()

class GenerateReport(ScheduledExternalProgramTask):
    task_namespace = 'fastqc'

    input_file = luigi.Parameter()
    output_dir = luigi.Parameter()

    @staticmethod
    def gen_report_basename(fastq_path):
        sample_name, ext = splitext(basename(fastq_path))
        if ext == '.gz':
            sample_name, ext = splitext(sample_name)
        return '{}_fastqc.html'.format(sample_name)

    def program_args(self):
        return [cfg.fastqc_bin,
                '--outdir', self.output_dir,
                self.input_file]

    def output(self):
        return luigi.LocalTarget(join(self.output_dir, self.gen_report_basename(self.input_file)))
