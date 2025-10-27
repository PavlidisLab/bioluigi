from typing import Optional

import luigi

from .utils import TaskWithMetadataMixin
from ..config import BioluigiConfig
from ..local_target import LocalTarget
from ..scheduled_external_program import ScheduledExternalProgramTask

cfg = BioluigiConfig()

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
        return LocalTarget(self.output_file)

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

    split: Optional[str] = luigi.ChoiceParameter(default='three', choices=['three', 'files', 'spot'], positional=False,
                                                 description='Way to split the output files.')

    clip: bool = luigi.BoolParameter(default=False, positional=False,
                                     description='Clip the reads to remove adapter sequences')

    minimum_read_length: Optional[int] = luigi.OptionalIntParameter(default=None, positional=False,
                                                                    description='Minimum read length to be extracted from the archive')

    skip_technical: bool = luigi.BoolParameter(default=False, positional=False,
                                               description='Skip technical reads. Only applicable if using split=spot.')

    min_spot_id: Optional[int] = luigi.OptionalIntParameter(default=None,
                                                            description='Minimum spot ID to retrieve (inclusive)')
    max_spot_id: Optional[int] = luigi.OptionalIntParameter(default=None,
                                                            description='Maximum spot ID to retrieve (inclusive)')

    _tmp_output_dir: Optional[str] = None

    @property
    def resources(self):
        r = super().resources
        r.update({'fastq_dump_jobs': 1, 'io_jobs': 1})
        return r

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
        elif self.split:
            raise ValueError('Invalid split option: ' + self.split)

        if self.skip_technical:
            args.append('--skip-technical')

        if self.minimum_read_length is not None:
            args.extend(['-M', str(self.minimum_read_length)])

        if self.min_spot_id is not None:
            args.extend(['-N', str(self.min_spot_id)])

        if self.max_spot_id is not None:
            args.extend(['-X', str(self.max_spot_id)])

        # temp_output_dir is only set within a run() execution, so this is a
        # graceful fallback
        args.extend(['--outdir', self._tmp_output_dir if self._tmp_output_dir else self.output_dir])

        args.append(self.input_file)

        return args

    def run(self):
        with self.output().temporary_path() as self._tmp_output_dir:
            super().run()

    def output(self):
        return LocalTarget(self.output_dir)
