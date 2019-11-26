import datetime
from os.path import join
import luigi
from ..scheduled_external_program import ScheduledExternalProgramTask
from ..config import bioluigi

cfg = bioluigi()

class Prefetch(ScheduledExternalProgramTask):
    """
    Prefetch a SRA run archive.

    This task is not schedulable because it does not make much sense to
    allocate cores for a download job. You can however limit the number of
    concurrent prefetch tasks by setting the 'sra_connections' resource.

    :max_size: Maximum download size in gigabytes
    :extra_args: Extra argumets to pass to prefetch which can be used to setup
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

    :param input_file: A file path or a SRA archive
    :param output_dir: Destination directory for the extracted FASTQs
    :param paired_reads: Indicate if the original data has paired mates, in
    which case the output will consist of two files instead of one

    Note that this task does not produce its outputs atomically.
    """
    task_namespace = 'sratoolkit'

    input_file = luigi.Parameter()
    output_dir = luigi.Parameter()

    paired_reads = luigi.BoolParameter(default=False, positional=False)

    walltime = datetime.timedelta(hours=6)
    cpus = 1
    memory = 1

    def program_args(self):
        return [cfg.fastqdump_bin,
                '--gzip',
                '--clip',
                '--skip-technical',
                '--readids',
                '--dumpbase',
                '--split-files',
                '--outdir', self.output_dir,
                self.input_file]

    def output(self):
        if self.paired_reads:
            return [luigi.LocalTarget(join(self.output_dir, self.srr + '_1.fastq.gz')),
                    luigi.LocalTarget(join(self.output_dir, self.srr + '_2.fastq.gz'))]
        else:
            return [luigi.LocalTarget(join(self.output_dir, self.srr + '_1.fastq.gz'))]
