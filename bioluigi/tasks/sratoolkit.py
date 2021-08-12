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

    The number of concurrent prefetch jobs can be adjusted by setting the
    'prefetch_jobs' resource.
    """
    task_namespace = 'sratoolkit'

    srr_accession = luigi.Parameter()
    output_file = luigi.Parameter()
    max_size = luigi.IntParameter(default=20, positional=False, significant=False, description='Maximum download size in gigabytes')
    extra_args = luigi.ListParameter(default=[], positional=False, description='Extra arguments to pass to prefetch which can be used to setup Aspera')

    @property
    def resources(self):
        r = super(Prefetch, self).resources
        r.update({'prefetch_jobs': 1})
        return r

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
    Extract FASTQs from a SRA archive

    The task output three targets: single-end reads (1 file) and paired-end
    reads (2 files). Note that it is sufficient for either the single-end file
    or both paired-end files to exist for the task to be considered complete.

    The number of concurrent fastq-dump jobs can be adjusted by setting the
    'fastq_dump_jobs' resource.
    """
    task_namespace = 'sratoolkit'

    input_file = luigi.Parameter(description='A file path or a SRA archive, or a SRA run accession')
    output_dir = luigi.Parameter(description='Destination directory for the extracted FASTQs')

    minimum_read_length = luigi.IntParameter(default=0, positional=False, description='Minimum read length to be extracted from the archive')

    @property
    def resources(self):
        r = super(FastqDump, self).resources
        r.update({'fastq_dump_jobs': 1})
        return r

    def __init__(self, *kwargs, **kwds):
        super(FastqDump, self).__init__(*kwargs, **kwds)
        base, tail = split(self.output_dir)
        self.temp_output_dir = mkdtemp(prefix=tail + '-tmp', dir=base)

    def program_args(self):
        args = [cfg.fastqdump_bin,
                '--gzip',
                '--clip',
                '--skip-technical',
                '--dumpbase',
                '--split-e',
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
                tmp_out_path = join(self.temp_output_dir, basename(out.path))
                if os.path.exists(tmp_out_path):
                    out.makedirs()
                    os.replace(tmp_out_path, out.path)
        finally:
            shutil.rmtree(self.temp_output_dir)

    def output(self):
        sra_accession, _ = os.path.splitext(os.path.basename(self.input_file))
        return [luigi.LocalTarget(join(self.output_dir, sra_accession + '.fastq.gz')),
                luigi.LocalTarget(join(self.output_dir, sra_accession + '_1.fastq.gz')),
                luigi.LocalTarget(join(self.output_dir, sra_accession + '_2.fastq.gz'))]

    def complete(self):
        se, r1, r2 = self.output()
        return se.exists() or (r1.exists() and r2.exists())
