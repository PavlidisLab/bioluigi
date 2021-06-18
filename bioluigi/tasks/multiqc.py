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

    force = luigi.BoolParameter(default=False, positional=False)

    def program_args(self):
        args = [cfg.multiqc_bin, '--outdir', self.output_dir]
        if self.title is not None:
            args.extend(['--title', self.title])
        if self.comment is not None:
            args.extend(['--comment', self.comment])
        if self.force:
            args.append('--force')
        args.extend(self.input_dirs)
        return args

    def run(self):
        try:
            return super(GenerateReport, self).run()
        finally:
            self._did_run = True

    def output(self):
        return luigi.LocalTarget(join(self.output_dir, 'multiqc_report.html'))

    def complete(self):
        # since we're forcing the task, we first ignore any existing completion state
        if self.force and not hasattr(self, '_did_run'):
            return False
        return super(GenerateReport, self).complete()
