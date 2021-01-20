import datetime
import logging

import luigi
from luigi.task import flatten_output

from ..scheduled_external_program import ScheduledExternalProgramTask
from ..config import bioluigi
from .utils import RemoveTaskOutputOnFailureMixin

logger = logging.getLogger('luigi-interface')

cfg = bioluigi()

class CutadaptTask(RemoveTaskOutputOnFailureMixin, ScheduledExternalProgramTask):
    """
    Base class for all cutadapt-derived tasks.
    """
    task_namespace = 'cutadapt'

    adapter_3prime = luigi.OptionalParameter(default='', positional=False)
    adapter_5prime = luigi.OptionalParameter(default='', positional=False)

    cut = luigi.IntParameter(default=0, positional=False)
    trim_n = luigi.BoolParameter(default=False, positional=False)
    minimum_length = luigi.IntParameter(default=0, positional=False)

    def program_args(self):
        args = [cfg.cutadapt_bin]

        args.extend(['-j', self.cpus])

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

        return args

class TrimReads(CutadaptTask):
    """
    For consistency with TrimPairedReads, this task output a list with a single
    target corresponding to trimmed FASTQ.
    """
    input_file =  luigi.Parameter()
    output_file = luigi.Parameter()

    def program_args(self):
        args = super(TrimReads, self).program_args()
        args.extend(['-o', self.output_file, self.input_file])
        return args

    def output(self):
        return [luigi.LocalTarget(self.output_file)]

class TrimPairedReads(CutadaptTask):
    input_file = luigi.Parameter()
    input2_file = luigi.Parameter()
    output_file = luigi.Parameter()
    output2_file = luigi.Parameter()

    reverse_adapter_3prime = luigi.OptionalParameter(default='', positional=False)
    reverse_adapter_5prime = luigi.OptionalParameter(default='', positional=False)

    def program_args(self):
        args = super(TrimPairedReads, self).program_args()
        if self.reverse_adapter_3prime:
            args.extend(['-A', self.reverse_adapter_3prime])
        if self.reverse_adapter_5prime:
            args.extend(['-G', self.reverse_adapter_5prime])
        args.extend([
            '-o', self.output_file,
            '-p', self.output2_file,
            self.input_file, self.input2_file])
        return args

    def output(self):
        return [luigi.LocalTarget(self.output_file), luigi.LocalTarget(self.output2_file)]
