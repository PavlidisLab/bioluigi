import luigi
from os.path import join

from ..scheduled_external_program import ScheduledExternalProgramTask
from ..config import bioluigi

cfg = bioluigi()

class BcftoolsTask(ScheduledExternalProgramTask):
    task_namespace = 'bcftools'

    input_file = luigi.Parameter()

    include = luigi.OptionalParameter(positional=False, default='')
    exclude = luigi.OptionalParameter(positional=False, default='')
    regions_file = luigi.OptionalParameter(positional=False, default='')
    apply_filters = luigi.OptionalParameter(positional=False, default='')

    # FIXME: the '--threads' flag does not seem to work
    cpus = 1

    def subcommand_args(self):
        raise NotImplementedError

    def subcommand_post_input_args(self):
        return []

    def program_args(self):
        args = [cfg.bcftools_bin]

        if self.include:
            args.extend(['-i', self.include])

        if self.exclude:
            args.extend(['-e', self.exclude])

        if self.regions_file:
            args.extend(['-R', self.regions_file])

        if self.apply_filters is not None:
            args.extend(['-f', self.apply_filters])

        args.extend(self.subcommand_args())

        args.append(self.input_file)

        args.extend(self.subcommand_post_input_args())

        return args

class View(BcftoolsTask):
    output_file = luigi.Parameter()
    output_format = luigi.Parameter(positional=False, default='z')

    def subcommand_args(self):
        return ['view',
            '--output-type', self.output_format,
            '--output', self.output_file]

    def output(self):
        return luigi.LocalTarget(self.output_file)

class Annotate(BcftoolsTask):
    output_file = luigi.Parameter()
    output_format = luigi.Parameter(positional=False, default='z')

    # options given an annotation file
    annotations_file = luigi.OptionalParameter(positional=False, default='')

    columns = luigi.ListParameter(positional=False, default=[])

    rename_chrs = luigi.OptionalParameter(positional=False, default='')

    def subcommand_args(self):
        args = ['annotate']

        if self.rename_chrs is not None:
            args.extend(['--rename-chrs', self.rename_chrs])

        if self.annotations_file:
            args.extend(['-a', self.annotations_file])
            args.extend(['-c', ','.join(self.columns)])

        args.extend([
            '--output-type', self.output_format,
            '--output', self.output_file])

        return args

    def output(self):
        return luigi.LocalTarget(self.output_file)

class Sort(BcftoolsTask):
    output_file = luigi.Parameter()
    output_format = luigi.Parameter(positional=False, default='z')

    tmp_dir = luigi.Parameter(default='/tmp', significant=False)

    def subcommand_args(self):
        return ['sort',
                '--temp-dir', self.tmp_dir,
                '--output-type', self.output_format,
                '--output', self.output_file]

    def output(self):
        return luigi.LocalTarget(self.output_file)

class Index(BcftoolsTask):
    """
    Use tabix to create a tabular index for a VCF.
    """
    def subcommand_args(self):
        return ['index', '--tbi']

    def output(self):
        return luigi.LocalTarget(self.input_file + '.tbi')

class Intersect(BcftoolsTask):
    input_file2 = luigi.Parameter()
    output_dir = luigi.Parameter()

    def subcommand_args(self):
        return ['isec', '-p', self.output_dir]

    def subcommand_post_input_args(self):
        return [self.input_file2]

    def output(self):
        return [luigi.LocalTarget(join(self.output_dir, '000{}.vcf.gz'.format(i))) for i in range(4)]
