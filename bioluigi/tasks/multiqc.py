from os.path import join
from typing import Optional

import luigi

from ..config import BioluigiConfig
from ..local_target import LocalTarget
from ..scheduled_external_program import ScheduledExternalProgramTask

cfg = BioluigiConfig()

class GenerateReport(ScheduledExternalProgramTask):
    task_namespace = 'multiqc'

    input_dirs: list[str] = luigi.ListParameter()
    output_dir: str = luigi.Parameter()

    sample_names: Optional[str] = luigi.OptionalParameter(default=None, positional=False)
    replace_names: Optional[str] = luigi.OptionalParameter(default=None, positional=False)

    title: Optional[str] = luigi.OptionalParameter(default=None, positional=False)
    comment: Optional[str] = luigi.OptionalParameter(default=None, positional=False)

    force: bool = luigi.BoolParameter(default=False, positional=False)

    _tmp_output_dir: Optional[str] = None
    _did_run: bool = False

    def program_args(self):
        args = [cfg.multiqc_bin, '--outdir', self._tmp_output_dir if self._tmp_output_dir else self.output_dir]
        if self.sample_names:
            args.extend(['--sample-names', self.sample_names])
        if self.replace_names:
            args.extend(['--replace-names', self.replace_names])
        if self.title is not None:
            args.extend(['--title', self.title])
        if self.comment is not None:
            args.extend(['--comment', self.comment])
        if self.force:
            args.append('--force')
        args.extend(self.input_dirs)
        return args

    def run(self):
        with LocalTarget(self.output_dir).temporary_path() as self._tmp_output_dir:
            try:
                return super().run()
            finally:
                self._did_run = True

    def output(self):
        return LocalTarget(join(self.output_dir, 'multiqc_report.html'))

    def complete(self):
        # since we're forcing the task, we first ignore any existing completion state
        if self.force and not self._did_run:
            return False
        return super().complete()
