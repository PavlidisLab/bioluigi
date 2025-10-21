from os.path import join, basename, splitext
from typing import Optional, SupportsIndex

import luigi

from .utils import TaskWithMetadataMixin
from ..config import bioluigi
from ..scheduled_external_program import ScheduledExternalProgramTask

cfg = bioluigi()

class Prefetch(TaskWithMetadataMixin, ScheduledExternalProgramTask):
    """
    Prefetch a SRA run archive.

    The number of concurrent prefetch jobs can be adjusted by setting the
    'prefetch_jobs' resource.
    """
    task_namespace = 'sratoolkit'

    retry_count = 3

    srr_accession: str = luigi.Parameter()
    output_file: str = luigi.Parameter()
    max_size: int = luigi.IntParameter(default=20, positional=False, significant=False,
                                       description='Maximum download size in gigabytes')
    extra_args: list[str] = luigi.ListParameter(default=[], positional=False,
                                                description='Extra arguments to pass to prefetch which can be used to setup Aspera')

    @property
    def resources(self):
        r = super().resources
        r.update({'prefetch_jobs': 1, 'io_jobs': 1})
        return r

    def program_args(self):
        args = [cfg.prefetch_bin,
                '--max-size', '{}G'.format(self.max_size),
                # prefetch already has a built-in mechanism to coordinate multiple reader/writer with a lockfile
                '--output-file', self.output().path]

        args.extend(self.extra_args)
        args.append(self.srr_accession)

        return args

    def output(self):
        return luigi.LocalTarget(self.output_file)

class FastqDump(TaskWithMetadataMixin, ScheduledExternalProgramTask):
    """
    Extract FASTQs from a SRA archive

    The task output three targets: single-end reads (1 file) and paired-end
    reads (2 files). Note that it is sufficient for either the single-end file
    or both paired-end files to exist for the task to be considered complete.

    The number of concurrent fastq-dump jobs can be adjusted by setting the
    'fastq_dump_jobs' resource.
    """
    task_namespace = 'sratoolkit'

    input_file: str = luigi.Parameter(description='A file path or a SRA archive, or a SRA run accession')
    output_dir: str = luigi.Parameter(description='Destination directory for the extracted FASTQs')

    split: str = luigi.ChoiceParameter(default='three', choices=['three', 'files', 'spot'], positional=False,
                                       description='Way to split the output files.')

    clip: bool = luigi.BoolParameter(default=False, positional=False,
                                     description='Clip the reads to remove adapter sequences')

    minimum_read_length: Optional[int] = luigi.OptionalIntParameter(default=None, positional=False,
                                                            description='Minimum read length to be extracted from the archive')

    number_of_reads_per_spot: Optional[int] = luigi.OptionalIntParameter(default=None, positional=False,
                                                                         description='Number of reads per spot. This must be set when using split=files.')

    skip_technical: bool = luigi.BoolParameter(default=False, positional=False,
                                               description='Skip technical reads. Only applicable if using split=spot.')

    _tmp_output_dir: str = None

    @property
    def resources(self):
        r = super().resources
        r.update({'fastq_dump_jobs': 1, 'io_jobs': 1})
        return r

    def __init__(self, *kwargs, **kwds):
        super().__init__(*kwargs, **kwds)
        if self.split == 'files' and self.number_of_reads_per_spot is None:
            raise ValueError(
                'If split_files is True, number_of_reads_per_spot must be set.')

    def program_args(self):
        args = [cfg.fastqdump_bin,
                '--gzip',
                # this is necessary to have consistent output files
                '--keep-empty-files']

        if self.clip:
            args.append('--clip')

        if self.split == 'three':
            args.append('--split-3')
        elif self.split == 'files':
            args.append('--split-files')
        elif self.split == 'spot':
            args.append('--split-spot')
        else:
            raise ValueError('Invalid split option: ' + self.split)

        if self.skip_technical:
            args.append('--skip-technical')

        if self.minimum_read_length is not None:
            args.extend(['-M', self.minimum_read_length])

        # temp_output_dir is only set within a run() execution, so this is a
        # graceful fallback
        args.extend(['--outdir', self._tmp_output_dir if self._tmp_output_dir else self.output_dir])

        args.append(self.input_file)

        return args

    def run(self):
        with luigi.LocalTarget(self.output_dir).temporary_path() as self._tmp_output_dir:
            super().run()

    def output(self):
        sra_accession, _ = splitext(basename(self.input_file))
        if self.split == 'three':
            return [luigi.LocalTarget(join(self.output_dir, sra_accession + '.fastq.gz')),
                    luigi.LocalTarget(join(self.output_dir, sra_accession + '_1.fastq.gz')),
                    luigi.LocalTarget(join(self.output_dir, sra_accession + '_2.fastq.gz'))]
        elif self.split == 'files':
            return [luigi.LocalTarget(join(self.output_dir, sra_accession + '_' + str(i + 1) + '.fastq.gz'))
                    for i in range(self.number_of_reads_per_spot)]
        elif self.split == 'spot':
            return [luigi.LocalTarget(join(self.output_dir, sra_accession + '.fastq.gz'))]
        else:
            raise ValueError('Invalid split option: ' + self.split)

    def complete(self):
        if self.split == 'three':
            se, r1, r2 = self.output()
            return se.exists() or (r1.exists() and r2.exists())
        else:
            return super().complete()
