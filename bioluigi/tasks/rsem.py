from os.path import exists, join

import luigi
from luigi.util import requires

from ..config import bioluigi
from ..scheduled_external_program import ScheduledExternalProgramTask

cfg = bioluigi()

class RsemReference(luigi.Target):
    """
    Represents the target of rsem-prepare-reference script.
    """
    def __init__(self, reference_name):
        self.reference_name = reference_name

    def exists(self):
        exts = ['grp', 'ti', 'seq', 'chrlist']
        return all(exists('{}.{}'.format(self.reference_name, ext))
                for ext in exts)

class PrepareReference(ScheduledExternalProgramTask):
    task_namespace = 'rsem'

    annotation_file = luigi.Parameter()
    reference_fasta_files = luigi.ListParameter()
    reference_name = luigi.Parameter()

    aligner = luigi.ChoiceParameter(choices=['star'], positional=False)

    star_path = luigi.OptionalParameter(default=None, positional=False)

    def program_args(self):
        args = [join(cfg.rsem_dir, 'rsem-prepare-reference')]

        args.extend(['--gtf', self.annotation_file])

        if self.aligner == 'star':
            args.append('--star')

            if self.star_path is not None:
                args.extend(['--star-path', self.star_path])

        args.extend(['-p', self.cpus])

        args.extend(self.reference_fasta_files)

        args.append(self.reference_name)

        return args

    def output(self):
        return RsemReference(self.reference_name)

@requires(PrepareReference)
class CalculateExpression(ScheduledExternalProgramTask):
    task_namespace = 'rsem'

    upstream_read_files = luigi.ListParameter()
    sample_name = luigi.Parameter()

    strandedness = luigi.ChoiceParameter(default='none', choices=['none', 'forward', 'reverse'], positional=False)

    extra_args = luigi.ListParameter(default=[], positional=False)

    def program_args(self):
        args = [join(cfg.rsem_dir, 'rsem-calculate-expression')]

        args.extend(['-p', self.cpus])

        if len(self.upstream_read_files) == 1:
            pass
        elif len(self.upstream_read_files) == 2:
            args.append('--paired-end')
        else:
            raise ValueError('Unexpected number of mates: {}.'.format(len(self.upstream_read_files)))

        args.extend(['--strandedness', self.strandedness])

        if self.aligner == 'star':
            args.append('--star')

            if self.star_path is not None:
                args.extend(['--star-path', self.star_path])

            if self.upstream_read_files[0].endswith('.gz'):
                args.append('--star-gzipped-read-file')

        args.extend(self.extra_args)

        args.extend(self.upstream_read_files)
        args.extend([self.reference_name, self.sample_name])

        return args

    def output(self):
        return [luigi.LocalTarget('{}.isoforms.results'.format(self.sample_name)),
                luigi.LocalTarget('{}.genes.results'.format(self.sample_name))]

