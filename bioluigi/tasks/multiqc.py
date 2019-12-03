import datetime
from os.path import join

import luigi

from ..scheduled_external_program import ScheduledExternalProgramTask

class GenerateReport(ScheduledExternalProgramTask):
    task_namespace = 'multiqc'

    input_dirs = luigi.ListParameter()
    output_dir = luigi.Parameter()

    title = luigi.OptionalParameter(default='', positional=False)
    comment = luigi.OptionalParameter(default='', positional=False)

    walltime = datetime.timedelta(days=1)
    cpus = 1

    def program_args(self):
        args = [cfg.multiqc_bin, '--outdir', self.output_dir]
        if self.title:
            args.extend(['--title', self.title])
        if self.comment:
            args.extend(['--comment', self.comment])
        args.extend(input_dirs)
        return args

    def output(self):
        return luigi.LocalTarget(join(self.output_dir, 'multiqc_report.html'))
