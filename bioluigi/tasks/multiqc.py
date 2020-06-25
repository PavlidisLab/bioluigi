import datetime
from os.path import join

import luigi

from ..config import bioluigi
from ..scheduled_external_program import ScheduledExternalProgramTask

cfg = bioluigi()

class GenerateReport(ScheduledExternalProgramTask):
    task_namespace = 'multiqc'

    input_dirs = luigi.ListParameter()
    output_dir = luigi.Parameter()

    title = luigi.OptionalParameter(default=None, positional=False)
    comment = luigi.OptionalParameter(default=None, positional=False)

    def program_args(self):
        args = [cfg.multiqc_bin, '--outdir', self.output_dir]
        if self.title is not None:
            args.extend(['--title', self.title])
        if self.comment is not None:
            args.extend(['--comment', self.comment])
        args.extend(self.input_dirs)
        return args

    def output(self):
        return luigi.LocalTarget(join(self.output_dir, 'multiqc_report.html'))
