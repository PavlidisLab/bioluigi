from typing import Optional

import luigi

from ..config import BioluigiConfig
from ..local_target import LocalTarget
from ..scheduled_external_program import ScheduledExternalProgramTask

cfg = BioluigiConfig()

class CutadaptTask(ScheduledExternalProgramTask):
    """
    Base class for all cutadapt-derived tasks.
    """
    task_namespace = 'cutadapt'

    adapter_3prime: Optional[str] = luigi.OptionalParameter(default=None, positional=False)
    adapter_5prime: Optional[str] = luigi.OptionalParameter(default=None, positional=False)

    cut: int = luigi.IntParameter(default=0, positional=False)
    trim_n: bool = luigi.BoolParameter(default=False, positional=False)
    minimum_length: int = luigi.IntParameter(default=0, positional=False)

    report_file: Optional[str] = luigi.OptionalParameter(default=None, positional=False,
                                                         description='Destination for the JSON report')

    @property
    def resources(self):
        r = super().resources
        r.update({'cutadapt_jobs': 1, 'io_jobs': 1})
        return r

    def program_args(self):
        args = [cfg.cutadapt_bin]

        args.extend(['-j', str(self.cpus)])

        if self.adapter_3prime:
            args.extend(['-a', self.adapter_3prime])

        if self.adapter_5prime:
            args.extend(['-g', self.adapter_5prime])

        if self.cut:
            args.extend(['-u', self.cut])

        if self.trim_n:
            args.append('--trim-n')

        if self.minimum_length:
            args.extend(['--minimum-length', self.minimum_length])

        if self.report_file:
            args.extend(['--json', self.report_file])

        return args

class TrimReads(CutadaptTask):
    """
    For consistency with TrimPairedReads, this task output a list with a single
    target corresponding to trimmed FASTQ.
    """
    input_file: str = luigi.Parameter()
    output_file: str = luigi.Parameter()

    # temporary location for cutadapt output
    _tmp_output_file: Optional[str] = None

    def program_args(self):
        args = super().program_args()
        args.extend(['-o', self._tmp_output_file if self._tmp_output_file else self.output_file, self.input_file])
        return args

    def run(self):
        with self.output()[0].temporary_path() as self._tmp_output_file:
            super().run()

    def output(self):
        return [LocalTarget(self.output_file)]

class TrimPairedReads(CutadaptTask):
    input_file: str = luigi.Parameter()
    input2_file: str = luigi.Parameter()
    output_file: str = luigi.Parameter()
    output2_file: str = luigi.Parameter()

    reverse_adapter_3prime: Optional[str] = luigi.OptionalParameter(default=None, positional=False)
    reverse_adapter_5prime: Optional[str] = luigi.OptionalParameter(default=None, positional=False)

    # temporary location for cutadapt output
    _tmp_output_file: Optional[str] = None
    _tmp_output2_file: Optional[str] = None

    def program_args(self):
        args = super().program_args()
        if self.reverse_adapter_3prime:
            args.extend(['-A', self.reverse_adapter_3prime])
        if self.reverse_adapter_5prime:
            args.extend(['-G', self.reverse_adapter_5prime])
        args.extend([
            '-o', self._tmp_output_file if self._tmp_output_file else self.output_file,
            '-p', self._tmp_output2_file if self._tmp_output2_file else self.output2_file,
            self.input_file, self.input2_file])
        return args

    def run(self):
        with self.output()[0].temporary_path() as self._tmp_output_file, \
            self.output()[1].temporary_path() as self._tmp_output2_file:
            super().run()

    def output(self):
        return [LocalTarget(self.output_file), LocalTarget(self.output2_file)]
