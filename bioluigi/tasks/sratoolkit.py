import datetime
import os
from os.path import join, split, basename
from tempfile import mkdtemp
import shutil

import luigi
from luigi.task import flatten

from ..scheduled_external_program import ScheduledExternalProgramTask
from ..config import bioluigi

cfg = bioluigi()

class Prefetch(ScheduledExternalProgramTask):
    """
    Prefetch a SRA run archive.

    :attr max_size: Maximum download size in gigabytes
    :attr extra_args: Extra argumets to pass to prefetch which can be used to setup
    Aspera.
    """
    task_namespace = 'sratoolkit'

    srr_accession = luigi.Parameter()
    output_file = luigi.Parameter()
    max_size = luigi.IntParameter(default=20, positional=False, significant=False)
    extra_args = luigi.ListParameter(default=[], positional=False)

    def program_args(self):
        args = [cfg.prefetch_bin,
                '--max-size',    '{}G'.format(self.max_size),
                '--output-file', self.output().path]

        args.extend(self.extra_args)
        args.append(self.srr_accession)

        return args

    def output(self):
        return luigi.LocalTarget(self.output_file)

class FastqDump(ScheduledExternalProgramTask):
    """
    Extract one or multiple FASTQs from a SRA archive

    :attr input_file: A file path or a SRA archive, or a SRA run accession
    :attr output_dir: Destination directory for the extracted FASTQs
    :attr paired_reads: Indicate if the original data has paired mates, in
    which case the output will consist of two files instead of one
    """
    task_namespace = 'sratoolkit'

    input_file = luigi.Parameter()
    output_dir = luigi.Parameter()

    minimum_read_length = luigi.IntParameter(default=0, positional=False)

    paired_reads = luigi.BoolParameter(default=False, positional=False)

    walltime = datetime.timedelta(hours=6)
    cpus = 1
    memory = 1

    def __init__(self, *kwargs, **kwds):
        super(FastqDump, self).__init__(*kwargs, **kwds)
        base, tail = split(self.output_dir)
        self.temp_output_dir = mkdtemp(prefix=tail + '-tmp', dir=base)

    def program_args(self):
        args = [cfg.fastqdump_bin,
                '--gzip',
                '--clip',
                '--skip-technical',
                '--readids',
                '--dumpbase',
                '--split-files',
                '--keep-empty-files']

        if self.minimum_read_length > 0:
            args.extend(['-M', self.minimum_read_length])

        args.extend(['--outdir', self.temp_output_dir])

        args.append(self.input_file)

        return args

    def run(self):
        try:
            super(FastqDump, self).run()
            # move every output to the final directory
            for out in self.output():
                out.makedirs()
                os.replace(join(self.temp_output_dir, basename(out.path)), out.path)
        finally:
            shutil.rmtree(self.temp_output_dir)

    def output(self):
        sra_accession, _ = os.path.splitext(os.path.basename(self.input_file))
        if self.paired_reads:
            return [luigi.LocalTarget(join(self.output_dir, sra_accession + '_1.fastq.gz')),
                    luigi.LocalTarget(join(self.output_dir, sra_accession + '_2.fastq.gz'))]
        else:
            return [luigi.LocalTarget(join(self.output_dir, sra_accession + '_1.fastq.gz'))]
